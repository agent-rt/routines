use std::collections::HashMap;
use std::fmt::Write;
use std::path::PathBuf;

use rmcp::handler::server::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::*;
use rmcp::{ServerHandler, tool, tool_handler, tool_router};
use schemars::JsonSchema;
use serde::Deserialize;

use crate::audit::AuditDb;
use crate::executor::{self, RunStatus, StepStatus};
use crate::mcp_config::{McpConfig, McpServerConfig};
use crate::parser::{Routine, StepAction};
use crate::registry::{self, Registries, RegistryConfig};
use crate::resolve::resolve_routine_path;
use crate::secrets;

fn routines_dir() -> PathBuf {
    std::env::var("ROUTINES_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| dirs_home().join(".routines"))
}

fn dirs_home() -> PathBuf {
    std::env::var("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("."))
}

#[derive(Clone, Default)]
pub struct RoutinesMcpServer {
    tool_router: ToolRouter<Self>,
}

// --- Parameter structs ---

#[derive(Debug, Deserialize, JsonSchema)]
pub struct RunRoutineParams {
    /// Routine name (matches filename in hub without .yml extension)
    pub name: String,
    /// Key-value input parameters for the routine
    #[serde(default)]
    pub inputs: HashMap<String, String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CreateRoutineParams {
    /// Routine name (will be saved as <name>.yml in hub)
    pub name: String,
    /// Human-readable description of what this routine does
    pub description: String,
    /// Complete YAML content of the routine definition
    pub yaml_content: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetRunLogParams {
    /// The UUID run_id to retrieve logs for
    pub run_id: String,
}

/// Parameter struct for get_routine
#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetRoutineParams {
    /// Routine name (matches filename in hub without .yml extension)
    pub name: String,
}

/// Parameter struct for validate_routine
#[derive(Debug, Deserialize, JsonSchema)]
pub struct ValidateRoutineParams {
    /// Complete YAML content to validate
    pub yaml_content: String,
}

/// Parameter struct for manage_mcp_servers
#[derive(Debug, Deserialize, JsonSchema)]
pub struct ManageMcpServersParams {
    /// Action to perform: "list", "add", "remove", "get"
    pub action: String,
    /// Server name (required for add, remove, get)
    #[serde(default)]
    pub name: Option<String>,
    /// Command to start the MCP server (required for add)
    #[serde(default)]
    pub command: Option<String>,
    /// Arguments for the command (optional, for add)
    #[serde(default)]
    pub args: Option<Vec<String>>,
    /// Environment variables (optional, for add). Values support {{ secrets.X }} templates.
    #[serde(default)]
    pub env: Option<HashMap<String, String>>,
}

/// Parameter struct for manage_registries
#[derive(Debug, Deserialize, JsonSchema)]
pub struct ManageRegistriesParams {
    /// Action to perform: "list", "add", "remove", "sync"
    pub action: String,
    /// Registry name (required for add, remove, sync with specific name)
    #[serde(default)]
    pub name: Option<String>,
    /// Git repository URL (required for add)
    #[serde(default)]
    pub url: Option<String>,
    /// Git branch or tag (optional for add, default: main)
    #[serde(default)]
    pub git_ref: Option<String>,
}

/// Parameter struct for dry_run_routine
#[derive(Debug, Deserialize, JsonSchema)]
pub struct DryRunRoutineParams {
    /// Complete YAML content of the routine
    pub yaml_content: String,
    /// Sample input parameters for template resolution
    #[serde(default)]
    pub inputs: HashMap<String, String>,
}

const DSL_SCHEMA: &str = "\
Routine YAML DSL Reference
===========================
Top-level fields:
  name: String (required)
  description: String (required)
  strict_mode: bool (default: false) — blocks dangerous commands (rm -rf, mkfs, etc.)
  secrets_env: none|auto|list (default: none) — inject secrets as CLI subprocess env vars
    none: no injection (default). auto: all secrets as same-name env vars. list: only named secrets.
    Step-level env: overrides secrets_env for same key.
  inputs: list of InputDef (default: [])
  steps: list of Step (required)
  finally: list of Step (default: []) — cleanup steps, always run after main steps regardless of success/failure
  output: String (optional) — template expression resolved after all steps, returned as routine result
  output_format: plain|table (default: plain) — table expects JSON array, renders as table in CLI

InputDef:
  name: String (required)
  required: bool (default: false)
  default: String (optional)
  description: String (optional)

Step (common fields):
  id: String (required) — unique identifier, used in {{ step_id.stdout }}
  type: cli|http|routine (required)
  timeout: integer (optional) — seconds before step is killed
  when: String (optional) — condition; step skipped if false. Supports: A == B, A != B, truthy
  on_fail: stop|continue (default: stop) — error strategy; continue allows subsequent steps to run
  needs: list of String (default: []) — step IDs that must complete first. Enables parallel execution.
  retry: object (optional) — retry on failure before triggering on_fail
    count: integer (required) — max retries (total attempts = count + 1)
    delay: integer (default: 1) — initial delay in seconds
    backoff: fixed|exponential (default: fixed) — exponential doubles delay each retry
  for_each: list or template (optional) — iterate step over items, injecting {{ item }} and {{ item_index }}

Step (type: cli):
  command: String (required) — executable name or path
  args: list of String (default: [])
  env: map of String→String (default: {})
  stdin: String (optional) — content piped to subprocess stdin
  working_dir: String (optional) — working directory for subprocess

Step (type: http):
  url: String (required) — HTTP URL, supports templates
  method: String (default: GET) — HTTP method
  headers: map of String→String (default: {}) — request headers, supports templates
  body: String (optional) — request body, supports templates

Step (type: routine):
  name: String (required) — routine reference: 'name', 'namespace/name', or '@registry/name'
  inputs: map of String→String (default: {}) — input parameters, supports templates

Step (type: mcp):
  server: String (required) — MCP server name (from ~/.routines/mcp.json)
  tool: String (required) — tool name to call on the server
  arguments: map of String→JSON (default: {}) — tool arguments, string values support templates

Step (type: transform):
  input: String (required) — template resolving to JSON string
  select: String (optional) — JSON path to extract (e.g. '.data.items'). Array → mapping per element
  mapping: map of String→String (optional) — output_key → path + filter pipeline

  Path syntax: .field, [0], [-1], [*] (wildcard expands array, applies remaining path per element)

  Filter pipeline (use | to chain):
    Type: to_int, to_float, to_string
    String: slice(start, end), split(sep), join(sep), replace(old, new), trim
    Math: math(expr) — use _ for current value (e.g. math(_ / 60)), round, floor, ceil
    Format: duration_fmt — minutes→'Xh Ym', default(value), fmt(template) — {} placeholder

  Example:
    - id: format
      type: transform
      input: '{{ search.stdout }}'
      select: '.data.itemList'
      mapping:
        price: '.ticketPrice'
        duration: '.totalDuration | to_int | duration_fmt'
        flights: '.journeys[0].segments[*].marketingTransportNo | join(\"/\")'
        dep: '.journeys[0].segments[0].depDateTime | slice(11, 16)'

Template syntax:
  {{ inputs.NAME }}       — input parameter value
  {{ secrets.KEY }}       — secret from ~/.routines/.env
  {{ step_id.stdout }}       — stdout of a previous step (trimmed)
  {{ step_id.stderr }}       — stderr of a previous step (trimmed)
  {{ step_id.exit_code }}    — exit code of a previous step (integer string)
  {{ step_id.stdout_lines }} — stdout split by newline as JSON array (for use with for_each)
  {{ item }}                 — current iteration value (inside for_each)
  {{ item_index }}           — current iteration index, 0-based (inside for_each)
  {{ _run.status }}          — run status: SUCCESS or FAILED (available in finally block)

Example:
  name: greet
  description: Say hello
  inputs:
    - name: who
      required: true
  steps:
    - id: say_hi
      type: cli
      command: echo
      args: [\"Hello {{ inputs.who }}\"]
";

/// Render output for MCP responses. Table format converts JSON array to compact text.
fn render_output_for_mcp(output: &str, format: &crate::parser::OutputFormat) -> String {
    use crate::parser::OutputFormat;

    if *format != OutputFormat::Table {
        return output.to_string();
    }

    // Try to parse as JSON array of objects
    let Ok(rows) = serde_json::from_str::<Vec<serde_json::Map<String, serde_json::Value>>>(output)
    else {
        return output.to_string();
    };

    if rows.is_empty() {
        return "(empty)".to_string();
    }

    let columns: Vec<&String> = rows[0].keys().collect();
    let mut lines = Vec::with_capacity(rows.len() + 1);

    // Header
    lines.push(columns.iter().map(|c| c.as_str()).collect::<Vec<_>>().join(" "));

    // Rows
    for row in &rows {
        let cells: Vec<String> = columns
            .iter()
            .map(|col| match row.get(*col) {
                Some(serde_json::Value::String(s)) => s.clone(),
                Some(v) => v.to_string(),
                None => String::new(),
            })
            .collect();
        lines.push(cells.join(" "));
    }

    lines.join("\n")
}

fn err_internal(msg: String) -> ErrorData {
    ErrorData::internal_error(msg, None)
}

fn err_params(msg: String) -> ErrorData {
    ErrorData::invalid_params(msg, None)
}

fn text_ok(text: String) -> Result<CallToolResult, ErrorData> {
    Ok(CallToolResult::success(vec![Content::text(text)]))
}

/// Recursively collect routine YAML files under a directory.
/// Returns (relative_name, Routine) pairs sorted by name.
fn collect_routines_recursive(
    dir: &std::path::Path,
    prefix: &str,
) -> Vec<(String, Routine)> {
    let mut results = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return results;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            let dir_name = entry.file_name().to_string_lossy().to_string();
            // Skip hidden directories (.git, etc.)
            if dir_name.starts_with('.') {
                continue;
            }
            let sub_prefix = if prefix.is_empty() {
                dir_name
            } else {
                format!("{prefix}/{dir_name}")
            };
            results.extend(collect_routines_recursive(&path, &sub_prefix));
        } else if path
            .extension()
            .is_some_and(|ext| ext == "yml" || ext == "yaml")
            && let Ok(routine) = Routine::from_file(&path)
        {
            let stem = path.file_stem().unwrap_or_default().to_string_lossy();
            let ref_name = if prefix.is_empty() {
                stem.to_string()
            } else {
                format!("{prefix}/{stem}")
            };
            results.push((ref_name, routine));
        }
    }
    results.sort_by(|a, b| a.0.cmp(&b.0));
    results
}

/// Format input descriptions for list output.
fn format_inputs(inputs: &[crate::parser::InputDef], has_required: &mut bool) -> String {
    if inputs.is_empty() {
        return String::new();
    }
    let parts: Vec<String> = inputs
        .iter()
        .map(|i| {
            if i.required {
                *has_required = true;
                format!("{}*", i.name)
            } else {
                i.name.clone()
            }
        })
        .collect();
    format!(" (inputs: {})", parts.join(", "))
}

#[tool_router]
impl RoutinesMcpServer {
    pub fn new() -> Self {
        Self {
            tool_router: Self::tool_router(),
        }
    }

    #[tool(description = "List all available routines with their input schemas")]
    async fn list_routines(&self) -> Result<CallToolResult, ErrorData> {
        let rdir = routines_dir();
        let mut out = String::new();
        let mut has_required = false;

        // Scan hub/ (local routines with namespace support)
        let hub_dir = rdir.join("hub");
        if hub_dir.exists() {
            let entries = collect_routines_recursive(&hub_dir, "");
            for (ref_name, routine) in &entries {
                let inputs_desc = format_inputs(&routine.inputs, &mut has_required);
                let _ = writeln!(out, "{ref_name} — {}{inputs_desc}", routine.description);
            }
        }

        // Scan registries/
        let reg_dir = rdir.join("registries");
        if reg_dir.exists()
            && let Ok(dirs) = std::fs::read_dir(&reg_dir)
        {
            for dir_entry in dirs.flatten() {
                if dir_entry.path().is_dir() {
                    let reg_name = dir_entry.file_name().to_string_lossy().to_string();
                    let entries = collect_routines_recursive(&dir_entry.path(), "");
                    if !entries.is_empty() && !out.is_empty() {
                        let _ = writeln!(out);
                    }
                    for (name, routine) in &entries {
                        let inputs_desc =
                            format_inputs(&routine.inputs, &mut has_required);
                        let _ = writeln!(
                            out,
                            "@{reg_name}/{name} — {}{inputs_desc}",
                            routine.description
                        );
                    }
                }
            }
        }

        if has_required {
            let _ = write!(out, "\n(* = required)");
        }

        if out.is_empty() {
            out = "No routines found".to_string();
        }

        text_ok(out.trim().to_string())
    }

    #[tool(description = "Return the YAML DSL reference: fields, types, template syntax, example")]
    async fn get_dsl_schema(&self) -> Result<CallToolResult, ErrorData> {
        text_ok(DSL_SCHEMA.trim().to_string())
    }

    #[tool(description = "Read an existing routine's YAML source by name")]
    async fn get_routine(
        &self,
        Parameters(params): Parameters<GetRoutineParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let yaml_path = resolve_routine_path(&params.name, &routines_dir());
        if !yaml_path.exists() {
            return Err(err_params(format!("Routine '{}' not found", params.name)));
        }
        let content = std::fs::read_to_string(&yaml_path)
            .map_err(|e| err_internal(format!("Read error: {e}")))?;
        text_ok(content)
    }

    #[tool(description = "Validate routine YAML without saving. Returns summary or parse errors.")]
    async fn validate_routine(
        &self,
        Parameters(params): Parameters<ValidateRoutineParams>,
    ) -> Result<CallToolResult, ErrorData> {
        match Routine::from_yaml(&params.yaml_content) {
            Ok(routine) => {
                let strict = if routine.strict_mode { "on" } else { "off" };
                text_ok(format!(
                    "OK: name={}, {} steps, {} inputs, strict_mode={}",
                    routine.name,
                    routine.steps.len(),
                    routine.inputs.len(),
                    strict,
                ))
            }
            Err(e) => Err(err_params(format!("Invalid YAML: {e}"))),
        }
    }

    #[tool(
        description = "Dry-run: resolve all templates with sample inputs and show resolved commands without executing"
    )]
    async fn dry_run_routine(
        &self,
        Parameters(params): Parameters<DryRunRoutineParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let routine = Routine::from_yaml(&params.yaml_content)
            .map_err(|e| err_params(format!("Invalid YAML: {e}")))?;

        // Validate required inputs
        for input_def in &routine.inputs {
            if input_def.required && !params.inputs.contains_key(&input_def.name) {
                return Err(err_params(format!(
                    "Missing required input: {}",
                    input_def.name
                )));
            }
        }

        // Build resolved inputs with defaults
        let mut resolved_inputs = HashMap::new();
        for input_def in &routine.inputs {
            if let Some(value) = params.inputs.get(&input_def.name) {
                resolved_inputs.insert(input_def.name.clone(), value.clone());
            } else if let Some(default) = &input_def.default {
                resolved_inputs.insert(input_def.name.clone(), default.clone());
            }
        }

        // Use placeholder secrets for dry run
        let secrets = secrets::load_secrets(&routines_dir().join(".env"));
        let redacted_secrets: HashMap<String, String> = secrets
            .keys()
            .map(|k| (k.clone(), "[REDACTED]".to_string()))
            .collect();

        let ctx = crate::context::Context::new(resolved_inputs, redacted_secrets);
        let mut out = String::new();

        for (i, step) in routine.steps.iter().enumerate() {
            match &step.action {
                StepAction::Cli {
                    command,
                    args,
                    env,
                    stdin,
                    working_dir,
                } => {
                    let cmd = ctx
                        .resolve(command, &step.id)
                        .unwrap_or_else(|e| format!("<error: {e}>"));
                    let resolved_args: Vec<String> = args
                        .iter()
                        .map(|a| {
                            ctx.resolve(a, &step.id)
                                .unwrap_or_else(|e| format!("<error: {e}>"))
                        })
                        .collect();
                    let _ = writeln!(
                        out,
                        "[{}] {}: {} {}",
                        i + 1,
                        step.id,
                        cmd,
                        resolved_args.join(" ")
                    );
                    if !env.is_empty() {
                        let env_parts: Vec<String> = env
                            .iter()
                            .map(|(k, v)| {
                                let resolved = ctx
                                    .resolve(v, &step.id)
                                    .unwrap_or_else(|e| format!("<error: {e}>"));
                                format!("{k}={resolved}")
                            })
                            .collect();
                        let _ = writeln!(out, "    env: {}", env_parts.join(" "));
                    }
                    if let Some(s) = stdin {
                        let resolved = ctx
                            .resolve(s, &step.id)
                            .unwrap_or_else(|e| format!("<error: {e}>"));
                        let preview = if resolved.len() > 80 {
                            format!("{}...", &resolved[..80])
                        } else {
                            resolved
                        };
                        let _ = writeln!(out, "    stdin: {preview}");
                    }
                    if let Some(dir) = working_dir {
                        let resolved = ctx
                            .resolve(dir, &step.id)
                            .unwrap_or_else(|e| format!("<error: {e}>"));
                        let _ = writeln!(out, "    working_dir: {resolved}");
                    }
                }
                StepAction::Http {
                    url,
                    method,
                    headers,
                    body,
                } => {
                    let resolved_url = ctx
                        .resolve(url, &step.id)
                        .unwrap_or_else(|e| format!("<error: {e}>"));
                    let resolved_method = ctx
                        .resolve(method, &step.id)
                        .unwrap_or_else(|e| format!("<error: {e}>"));
                    let _ = writeln!(
                        out,
                        "[{}] {}: {} {}",
                        i + 1,
                        step.id,
                        resolved_method,
                        resolved_url
                    );
                    for (k, v) in headers {
                        let resolved = ctx
                            .resolve(v, &step.id)
                            .unwrap_or_else(|e| format!("<error: {e}>"));
                        let _ = writeln!(out, "    header: {k}: {resolved}");
                    }
                    if let Some(b) = body {
                        let resolved = ctx
                            .resolve(b, &step.id)
                            .unwrap_or_else(|e| format!("<error: {e}>"));
                        let preview = if resolved.len() > 120 {
                            format!("{}...", &resolved[..120])
                        } else {
                            resolved
                        };
                        let _ = writeln!(out, "    body: {preview}");
                    }
                }
                StepAction::Routine { name, inputs } => {
                    let _ = writeln!(out, "[{}] {}: routine {}", i + 1, step.id, name);
                    if !inputs.is_empty() {
                        let input_parts: Vec<String> = inputs
                            .iter()
                            .map(|(k, v)| {
                                let resolved = ctx
                                    .resolve(v, &step.id)
                                    .unwrap_or_else(|e| format!("<error: {e}>"));
                                format!("{k}={resolved}")
                            })
                            .collect();
                        let _ = writeln!(out, "    inputs: {}", input_parts.join(" "));
                    }
                }
                StepAction::Mcp {
                    server,
                    tool,
                    arguments,
                } => {
                    let _ = writeln!(
                        out,
                        "[{}] {}: mcp {}:{}",
                        i + 1,
                        step.id,
                        server,
                        tool
                    );
                    if !arguments.is_empty() {
                        let arg_parts: Vec<String> = arguments
                            .iter()
                            .map(|(k, v)| {
                                let display = match v {
                                    serde_json::Value::String(s) => ctx
                                        .resolve(s, &step.id)
                                        .unwrap_or_else(|e| format!("<error: {e}>")),
                                    other => other.to_string(),
                                };
                                format!("{k}={display}")
                            })
                            .collect();
                        let _ = writeln!(out, "    arguments: {}", arg_parts.join(" "));
                    }
                }
                StepAction::Transform {
                    input,
                    select,
                    mapping,
                } => {
                    let resolved_input = ctx
                        .resolve(input, &step.id)
                        .unwrap_or_else(|e| format!("<error: {e}>"));
                    let preview = if resolved_input.len() > 80 {
                        format!("{}...", &resolved_input[..80])
                    } else {
                        resolved_input
                    };
                    let _ = writeln!(out, "[{}] {}: transform", i + 1, step.id);
                    let _ = writeln!(out, "    input: {preview}");
                    if let Some(sel) = select {
                        let _ = writeln!(out, "    select: {sel}");
                    }
                    if let Some(m) = mapping {
                        for (k, v) in m {
                            let _ = writeln!(out, "    {k}: {v}");
                        }
                    }
                }
            }

            if let Some(when_expr) = &step.when {
                let resolved = ctx
                    .resolve(when_expr, &step.id)
                    .unwrap_or_else(|e| format!("<error: {e}>"));
                let _ = writeln!(out, "    when: {resolved}");
            }
            if step.on_fail == crate::parser::OnFail::Continue {
                let _ = writeln!(out, "    on_fail: continue");
            }
            if let Some(timeout) = step.timeout {
                let _ = writeln!(out, "    timeout: {timeout}s");
            }
            if !step.needs.is_empty() {
                let _ = writeln!(out, "    needs: [{}]", step.needs.join(", "));
            }
            if let Some(retry) = &step.retry {
                let _ = writeln!(
                    out,
                    "    retry: {}x, delay={}s, backoff={:?}",
                    retry.count, retry.delay, retry.backoff
                );
            }
            if let Some(for_each) = &step.for_each {
                match for_each {
                    crate::parser::ForEach::List(items) => {
                        let _ = writeln!(out, "    for_each: [{}]", items.join(", "));
                    }
                    crate::parser::ForEach::Template(t) => {
                        let resolved = ctx
                            .resolve(t, &step.id)
                            .unwrap_or_else(|e| format!("<error: {e}>"));
                        let _ = writeln!(out, "    for_each: {resolved}");
                    }
                }
            }
        }

        // Show execution mode
        if routine.has_dag() {
            let _ = writeln!(out, "\nMode: parallel (DAG)");
        }

        // Show secrets_env config
        match &routine.secrets_env {
            crate::parser::SecretsEnv::None => {}
            crate::parser::SecretsEnv::Auto => {
                let _ = writeln!(out, "\nSecrets env: auto (all secrets injected as env vars)");
            }
            crate::parser::SecretsEnv::List(names) => {
                let _ = writeln!(out, "\nSecrets env: [{}]", names.join(", "));
            }
        }

        // Show finally block
        if !routine.finally.is_empty() {
            let _ = writeln!(out, "\nFinally ({} steps):", routine.finally.len());
            for (i, step) in routine.finally.iter().enumerate() {
                match &step.action {
                    StepAction::Cli { command, args, .. } => {
                        let cmd = ctx.resolve(command, &step.id).unwrap_or_else(|e| format!("<error: {e}>"));
                        let resolved_args: Vec<String> = args.iter().map(|a| ctx.resolve(a, &step.id).unwrap_or_else(|e| format!("<error: {e}>"))).collect();
                        let _ = writeln!(out, "  [F{}] {}: {} {}", i + 1, step.id, cmd, resolved_args.join(" "));
                    }
                    StepAction::Http { url, method, .. } => {
                        let _ = writeln!(out, "  [F{}] {}: {} {}", i + 1, step.id, method, url);
                    }
                    StepAction::Routine { name, .. } => {
                        let _ = writeln!(out, "  [F{}] {}: routine {}", i + 1, step.id, name);
                    }
                    StepAction::Mcp { server, tool, .. } => {
                        let _ = writeln!(out, "  [F{}] {}: mcp {}:{}", i + 1, step.id, server, tool);
                    }
                    StepAction::Transform { select, mapping, .. } => {
                        let _ = write!(out, "  [F{}] {}: transform", i + 1, step.id);
                        if let Some(sel) = select {
                            let _ = write!(out, " select={sel}");
                        }
                        if let Some(m) = mapping {
                            let _ = write!(out, " fields=[{}]", m.keys().cloned().collect::<Vec<_>>().join(", "));
                        }
                        let _ = writeln!(out);
                    }
                }
                if let Some(when_expr) = &step.when {
                    let _ = writeln!(out, "      when: {when_expr}");
                }
            }
        }

        // Show output declaration
        if let Some(output_template) = &routine.output {
            let _ = writeln!(out, "\nOutput: {output_template}");
            if routine.output_format != crate::parser::OutputFormat::Plain {
                let _ = writeln!(out, "Format: {:?}", routine.output_format);
            }
        }

        text_ok(out.trim().to_string())
    }

    #[tool(description = "Execute a routine by name with the given input parameters")]
    async fn run_routine(
        &self,
        Parameters(params): Parameters<RunRoutineParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let yaml_path = resolve_routine_path(&params.name, &routines_dir());
        if !yaml_path.exists() {
            return Err(err_params(format!("Routine '{}' not found", params.name)));
        }

        let routine = Routine::from_file(&yaml_path)
            .map_err(|e| err_internal(format!("Parse error: {e}")))?;

        let secret_map = secrets::load_secrets(&routines_dir().join(".env"));
        let db = AuditDb::open(&routines_dir().join("data.db"))
            .map_err(|e| err_internal(format!("DB error: {e}")))?;

        let run_id = uuid::Uuid::new_v4().to_string();
        let short_id = &run_id[..8];
        let started_at = chrono::Utc::now().to_rfc3339();

        let secret_values: Vec<&str> = secret_map.values().map(|s| s.as_str()).collect();
        let input_json = serde_json::to_string(&params.inputs).unwrap_or_default();
        let input_redacted = secrets::redact(&input_json, &secret_values);

        db.insert_run(&run_id, &routine.name, &input_redacted, &started_at)
            .map_err(|e| err_internal(format!("DB write error: {e}")))?;

        let result = executor::run_routine(&routine, params.inputs, secret_map.clone())
            .map_err(|e| err_internal(format!("Execution error: {e}")))?;

        for step in &result.step_results {
            let step_started = chrono::Utc::now().to_rfc3339();
            db.insert_step_log(&run_id, step, &secret_values, &step_started)
                .map_err(|e| err_internal(format!("DB write error: {e}")))?;
        }

        let ended_at = chrono::Utc::now().to_rfc3339();
        db.finalize_run(&run_id, &result, &ended_at)
            .map_err(|e| err_internal(format!("DB finalize error: {e}")))?;

        let total_ms: u64 = result
            .step_results
            .iter()
            .map(|s| s.execution_time_ms)
            .sum();
        let total_steps = result.step_results.len();

        let mut out = String::new();

        match result.status {
            RunStatus::Success => {
                // Compact: one-line summary + output
                let _ = write!(
                    out,
                    "run={short_id} SUCCESS {total_ms}ms {total_steps}/{total_steps} steps"
                );
                if let Some(output) = &result.output {
                    let trimmed = output.trim();
                    if !trimmed.is_empty() {
                        let rendered = render_output_for_mcp(trimmed, &result.output_format);
                        let _ = write!(out, "\n---\n");
                        if rendered.len() > 2000 {
                            let _ = write!(out, "{}... (truncated)", &rendered[..2000]);
                        } else {
                            let _ = write!(out, "{rendered}");
                        }
                    }
                }
            }
            RunStatus::Failed => {
                // Expand: show each step, with stderr on failed step
                let failed_at = result
                    .step_results
                    .iter()
                    .position(|s| s.status == StepStatus::Failed)
                    .map(|i| i + 1)
                    .unwrap_or(0);
                let _ = writeln!(
                    out,
                    "run={short_id} FAILED at step {failed_at}/{total_steps}"
                );
                for step in &result.step_results {
                    let icon = match step.status {
                        StepStatus::Success => "OK",
                        StepStatus::Failed => "FAIL",
                        StepStatus::Skipped => "SKIP",
                    };
                    let _ = writeln!(
                        out,
                        "[{icon}] {} exit={} {}ms",
                        step.step_id,
                        step.exit_code.unwrap_or(-1),
                        step.execution_time_ms,
                    );
                    if step.status == StepStatus::Failed {
                        let stderr = step.stderr.trim();
                        if !stderr.is_empty() {
                            for line in stderr.lines().take(10) {
                                let _ = writeln!(out, "  {line}");
                            }
                        }
                    }
                }
            }
        }

        text_ok(out.trim().to_string())
    }

    #[tool(description = "Create a new routine from YAML. Validates the YAML before saving.")]
    async fn create_routine(
        &self,
        Parameters(params): Parameters<CreateRoutineParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let _routine = Routine::from_yaml(&params.yaml_content)
            .map_err(|e| err_params(format!("Invalid YAML: {e}")))?;

        let file_path = resolve_routine_path(&params.name, &routines_dir());
        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| err_internal(format!("Failed to create dir: {e}")))?;
        }
        std::fs::write(&file_path, &params.yaml_content)
            .map_err(|e| err_internal(format!("Failed to write file: {e}")))?;

        text_ok(format!("created {} → {}", params.name, file_path.display()))
    }

    #[tool(
        description = "Get the complete audit log for a routine run, including all step details"
    )]
    async fn get_run_log(
        &self,
        Parameters(params): Parameters<GetRunLogParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let db = AuditDb::open(&routines_dir().join("data.db"))
            .map_err(|e| err_internal(format!("DB error: {e}")))?;

        let log = db
            .get_run_log(&params.run_id)
            .map_err(|e| err_internal(format!("Query error: {e}")))?;

        let Some(log) = log else {
            return Err(err_params(format!("Run '{}' not found", params.run_id)));
        };

        // Same format as CLI `routines log`
        let mut out = String::new();
        let _ = writeln!(out, "Routine: {}", log.routine_name);
        let _ = writeln!(out, "Run ID:  {}", log.run_id);
        let _ = writeln!(out, "Status:  {}", log.status);
        let _ = writeln!(out, "Started: {}", log.started_at);
        if let Some(ended) = &log.ended_at {
            let _ = writeln!(out, "Ended:   {ended}");
        }
        if let Some(inputs) = &log.input_vars
            && inputs != "{}"
        {
            let _ = writeln!(out, "Inputs:  {inputs}");
        }
        let _ = writeln!(out, "{}", "-".repeat(50));

        for (i, step) in log.steps.iter().enumerate() {
            let icon = if step.status == "SUCCESS" {
                "OK"
            } else {
                "FAIL"
            };
            let _ = writeln!(
                out,
                "[{icon}] Step {}: {} exit={} {}ms",
                i + 1,
                step.step_id,
                step.exit_code.unwrap_or(-1),
                step.execution_time_ms,
            );
            if let Some(stdout) = &step.stdout {
                let trimmed = stdout.trim();
                if !trimmed.is_empty() {
                    let _ = writeln!(out, "  stdout: {trimmed}");
                }
            }
            if let Some(stderr) = &step.stderr {
                let trimmed = stderr.trim();
                if !trimmed.is_empty() {
                    let _ = writeln!(out, "  stderr: {trimmed}");
                }
            }
        }

        if let Some(output) = &log.output {
            let trimmed = output.trim();
            if !trimmed.is_empty() {
                let _ = writeln!(out, "---\nOutput:\n{trimmed}");
            }
        }

        text_ok(out.trim().to_string())
    }

    #[tool(
        description = "Manage MCP server configurations. Actions: list (show all), add (register server), remove (delete server), get (show details + test connection)"
    )]
    async fn manage_mcp_servers(
        &self,
        Parameters(params): Parameters<ManageMcpServersParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let rdir = routines_dir();

        match params.action.as_str() {
            "list" => {
                let config = McpConfig::load(&rdir)
                    .map_err(|e| err_internal(format!("Config error: {e}")))?;
                if config.servers.is_empty() {
                    return text_ok("No MCP servers configured".to_string());
                }
                let mut out = String::new();
                for (name, srv) in &config.servers {
                    let args = if srv.args.is_empty() {
                        String::new()
                    } else {
                        format!(" {}", srv.args.join(" "))
                    };
                    let env_count = if srv.env.is_empty() {
                        String::new()
                    } else {
                        format!(" ({} env vars)", srv.env.len())
                    };
                    let _ = writeln!(out, "{name}: {}{args}{env_count}", srv.command);
                }
                text_ok(out.trim().to_string())
            }
            "add" => {
                let name = params
                    .name
                    .ok_or_else(|| err_params("'name' is required for add".into()))?;
                let command = params
                    .command
                    .ok_or_else(|| err_params("'command' is required for add".into()))?;
                let mut config = McpConfig::load(&rdir)
                    .map_err(|e| err_internal(format!("Config error: {e}")))?;
                config.add(
                    name.clone(),
                    McpServerConfig {
                        command,
                        args: params.args.unwrap_or_default(),
                        env: params.env.unwrap_or_default(),
                    },
                );
                config
                    .save(&rdir)
                    .map_err(|e| err_internal(format!("Save error: {e}")))?;
                text_ok(format!("Added MCP server '{name}'"))
            }
            "remove" => {
                let name = params
                    .name
                    .ok_or_else(|| err_params("'name' is required for remove".into()))?;
                let mut config = McpConfig::load(&rdir)
                    .map_err(|e| err_internal(format!("Config error: {e}")))?;
                if !config.remove(&name) {
                    return Err(err_params(format!("MCP server '{name}' not found")));
                }
                config
                    .save(&rdir)
                    .map_err(|e| err_internal(format!("Save error: {e}")))?;
                text_ok(format!("Removed MCP server '{name}'"))
            }
            "get" => {
                let name = params
                    .name
                    .ok_or_else(|| err_params("'name' is required for get".into()))?;
                let config = McpConfig::load(&rdir)
                    .map_err(|e| err_internal(format!("Config error: {e}")))?;
                let srv = config
                    .get(&name)
                    .ok_or_else(|| err_params(format!("MCP server '{name}' not found")))?;

                let mut out = String::new();
                let _ = writeln!(out, "Name: {name}");
                let _ = writeln!(out, "Command: {}", srv.command);
                if !srv.args.is_empty() {
                    let _ = writeln!(out, "Args: {}", srv.args.join(" "));
                }
                if !srv.env.is_empty() {
                    let env_keys: Vec<&String> = srv.env.keys().collect();
                    let _ = writeln!(out, "Env: {} (values hidden)", env_keys.iter().map(|k| k.as_str()).collect::<Vec<_>>().join(", "));
                }

                // Test connection
                let _ = writeln!(out, "---");
                let secrets = secrets::load_secrets(&rdir.join(".env"));
                let resolved_env = McpConfig::resolve_env(srv, &secrets);
                let command = srv.command.clone();
                let args = srv.args.clone();

                match test_mcp_connection(&command, &args, &resolved_env).await {
                    Ok(tools) => {
                        let _ = writeln!(out, "Connection: OK");
                        let _ = writeln!(out, "Tools ({}):", tools.len());
                        for t in &tools {
                            let desc = t.description.as_deref().unwrap_or("");
                            let _ = writeln!(out, "  {} — {desc}", t.name);
                        }
                    }
                    Err(e) => {
                        let _ = writeln!(out, "Connection: FAILED — {e}");
                    }
                }

                text_ok(out.trim().to_string())
            }
            other => Err(err_params(format!(
                "Unknown action '{other}'. Use: list, add, remove, get"
            ))),
        }
    }

    #[tool(
        description = "Manage routine registries (remote sources). Actions: list, add (name+url), remove (name), sync (name or all)"
    )]
    async fn manage_registries(
        &self,
        Parameters(params): Parameters<ManageRegistriesParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let rdir = routines_dir();

        match params.action.as_str() {
            "list" => {
                let config = Registries::load(&rdir)
                    .map_err(|e| err_internal(format!("Config error: {e}")))?;
                if config.registries.is_empty() {
                    return text_ok("No registries configured".to_string());
                }
                let mut out = String::new();
                for (name, reg) in &config.registries {
                    let synced = if rdir.join("registries").join(name).join(".git").exists() {
                        "synced"
                    } else {
                        "not synced"
                    };
                    let _ = writeln!(out, "@{name}: {} ({}) [{synced}]", reg.url, reg.git_ref);
                }
                text_ok(out.trim().to_string())
            }
            "add" => {
                let name = params
                    .name
                    .ok_or_else(|| err_params("'name' is required for add".into()))?;
                let url = params
                    .url
                    .ok_or_else(|| err_params("'url' is required for add".into()))?;
                let git_ref = params.git_ref.unwrap_or_else(|| "main".to_string());
                let mut config = Registries::load(&rdir)
                    .map_err(|e| err_internal(format!("Config error: {e}")))?;
                config.add(name.clone(), RegistryConfig { url, git_ref });
                config
                    .save(&rdir)
                    .map_err(|e| err_internal(format!("Save error: {e}")))?;
                text_ok(format!("Added registry '@{name}'. Use sync to fetch."))
            }
            "remove" => {
                let name = params
                    .name
                    .ok_or_else(|| err_params("'name' is required for remove".into()))?;
                let mut config = Registries::load(&rdir)
                    .map_err(|e| err_internal(format!("Config error: {e}")))?;
                if !config.remove(&name) {
                    return Err(err_params(format!("Registry '{name}' not found")));
                }
                config
                    .save(&rdir)
                    .map_err(|e| err_internal(format!("Save error: {e}")))?;
                registry::remove_registry_files(&name, &rdir)
                    .map_err(|e| err_internal(format!("Cleanup error: {e}")))?;
                text_ok(format!("Removed registry '@{name}'"))
            }
            "sync" => {
                if let Some(name) = params.name {
                    let config = Registries::load(&rdir)
                        .map_err(|e| err_internal(format!("Config error: {e}")))?;
                    let reg = config
                        .get(&name)
                        .ok_or_else(|| err_params(format!("Registry '{name}' not found")))?;
                    let msg = registry::sync_registry(&name, reg, &rdir)
                        .map_err(|e| err_internal(format!("Sync error: {e}")))?;
                    text_ok(msg)
                } else {
                    let results = registry::sync_all(&rdir)
                        .map_err(|e| err_internal(format!("Sync error: {e}")))?;
                    if results.is_empty() {
                        text_ok("No registries to sync".to_string())
                    } else {
                        text_ok(results.join("\n"))
                    }
                }
            }
            other => Err(err_params(format!(
                "Unknown action '{other}'. Use: list, add, remove, sync"
            ))),
        }
    }
}

/// Test connection to an MCP server: spawn → initialize → list_tools → close.
async fn test_mcp_connection(
    command: &str,
    args: &[String],
    env: &HashMap<String, String>,
) -> std::result::Result<Vec<rmcp::model::Tool>, String> {
    use rmcp::ServiceExt;
    use rmcp::transport::TokioChildProcess;
    use tokio::process::Command;

    let mut cmd = Command::new(command);
    cmd.args(args);
    for (k, v) in env {
        cmd.env(k, v);
    }

    let transport = TokioChildProcess::new(cmd)
        .map_err(|e| format!("Failed to spawn: {e}"))?;

    let client = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        ().serve(transport),
    )
    .await
    .map_err(|_| "Initialization timed out (30s)".to_string())?
    .map_err(|e| format!("Initialization failed: {e}"))?;

    let tools = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        client.list_all_tools(),
    )
    .await
    .map_err(|_| "list_tools timed out (10s)".to_string())?
    .map_err(|e| format!("list_tools failed: {e}"))?;

    let _ = client.cancel().await;
    Ok(tools)
}

#[tool_handler]
impl ServerHandler for RoutinesMcpServer {
    fn get_info(&self) -> ServerInfo {
        let mut info = ServerInfo::default();
        info.capabilities = ServerCapabilities::builder().enable_tools().build();
        info.server_info = Implementation::new("routines", env!("CARGO_PKG_VERSION"));
        info.instructions = Some(
            "Routines: deterministic workflow engine for AI agents. \
             get_dsl_schema → DSL reference, list_routines → discover, \
             get_routine → read source, validate_routine → check YAML, \
             dry_run_routine → preview commands, run_routine → execute, \
             create_routine → save, get_run_log → inspect, \
             manage_mcp_servers → configure MCP servers (list/add/remove/get), \
             manage_registries → remote routine sources (list/add/remove/sync)."
                .into(),
        );
        info
    }
}
