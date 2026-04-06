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
use crate::parser::{Routine, StepAction};
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
    /// Routine name
    pub name: String,
    /// Input key-value pairs
    #[serde(default)]
    pub inputs: HashMap<String, String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct MetaParams {
    /// Action: schema, get, create, validate, dry_run, test, history, log
    pub action: String,
    /// Routine name (for get/create/history) or run_id (for log)
    #[serde(default)]
    pub name: Option<String>,
    /// Description (for create)
    #[serde(default)]
    pub description: Option<String>,
    /// YAML content (for create/validate/dry_run/test)
    #[serde(default)]
    pub yaml_content: Option<String>,
    /// Sample inputs (for dry_run)
    #[serde(default)]
    pub inputs: Option<HashMap<String, String>>,
}

const DSL_SCHEMA: &str = "\
Routine YAML DSL Reference
===========================
Top-level fields:
  name: String (required)
  description: String (required)
  timeout: integer (optional) — max execution time for entire routine in seconds. Steps are skipped after deadline.
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
  type: string|int|float|bool|date|enum (default: string) — validated before execution
  enum_values: list of String (required when type: enum) — allowed values

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
pub fn collect_routines_recursive(
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

    #[tool(description = "List routines with input schemas")]
    async fn list(&self) -> Result<CallToolResult, ErrorData> {
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

    #[tool(description = "Routine meta-operations: schema/get/create/validate/dry_run/test/history/log")]
    async fn meta(
        &self,
        Parameters(params): Parameters<MetaParams>,
    ) -> Result<CallToolResult, ErrorData> {
        match params.action.as_str() {
            "schema" => {
                return text_ok(DSL_SCHEMA.trim().to_string());
            }
            "get" => {
                let name = params
                    .name
                    .ok_or_else(|| err_params("'name' required for get".into()))?;
                let yaml_path = resolve_routine_path(&name, &routines_dir());
                if !yaml_path.exists() {
                    return Err(err_params(format!("Routine '{name}' not found")));
                }
                let content = std::fs::read_to_string(&yaml_path)
                    .map_err(|e| err_internal(format!("Read error: {e}")))?;
                return text_ok(content);
            }
            "create" => {
                let name = params
                    .name
                    .ok_or_else(|| err_params("'name' required for create".into()))?;
                let yaml_content = params
                    .yaml_content
                    .ok_or_else(|| err_params("'yaml_content' required for create".into()))?;
                let _routine = Routine::from_yaml(&yaml_content)
                    .map_err(|e| err_params(format!("Invalid YAML: {e}")))?;
                let file_path = resolve_routine_path(&name, &routines_dir());
                if let Some(parent) = file_path.parent() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| err_internal(format!("Failed to create dir: {e}")))?;
                }
                std::fs::write(&file_path, &yaml_content)
                    .map_err(|e| err_internal(format!("Failed to write file: {e}")))?;
                return text_ok(format!("created {name} → {}", file_path.display()));
            }
            "validate" => {
                let yaml_content = params
                    .yaml_content
                    .ok_or_else(|| err_params("'yaml_content' required for validate".into()))?;
                return match Routine::from_yaml(&yaml_content) {
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
                };
            }
            "dry_run" => {
                // fall through to dry_run logic below
            }
            "test" => {
                let yaml_content = params
                    .yaml_content
                    .ok_or_else(|| err_params("'yaml_content' required for test".into()))?;
                let suite = crate::testing::TestSuite::from_yaml(&yaml_content)
                    .map_err(|e| err_params(format!("Invalid test YAML: {e}")))?;
                let results = crate::testing::run_test_suite(&suite, &routines_dir());
                let mut out = String::new();
                let mut pass = 0;
                let mut fail = 0;
                for r in &results {
                    if r.passed {
                        let _ = writeln!(out, "PASS {}", r.name);
                        pass += 1;
                    } else {
                        let _ = writeln!(out, "FAIL {}", r.name);
                        for f in &r.failures {
                            let _ = writeln!(out, "  {f}");
                        }
                        fail += 1;
                    }
                }
                let _ = write!(out, "\n{pass} passed, {fail} failed");
                return text_ok(out.trim().to_string());
            }
            "history" => {
                let name = params
                    .name
                    .ok_or_else(|| err_params("'name' required for history".into()))?;
                let db = AuditDb::open(&routines_dir().join("data.db"))
                    .map_err(|e| err_internal(format!("DB error: {e}")))?;
                let runs = db
                    .get_history(&name, 10)
                    .map_err(|e| err_internal(format!("Query error: {e}")))?;
                if runs.is_empty() {
                    return text_ok(format!("No runs found for '{name}'"));
                }
                let mut out = String::new();
                for r in &runs {
                    let ended = r.ended_at.as_deref().unwrap_or("—");
                    let _ = writeln!(
                        out,
                        "{} {} started={} ended={}",
                        &r.run_id[..8],
                        r.status,
                        r.started_at,
                        ended,
                    );
                }
                return text_ok(out.trim().to_string());
            }
            "log" => {
                let run_id = params
                    .name
                    .ok_or_else(|| err_params("'name' (as run_id) required for log".into()))?;
                let db = AuditDb::open(&routines_dir().join("data.db"))
                    .map_err(|e| err_internal(format!("DB error: {e}")))?;
                let log = db
                    .get_run_log(&run_id)
                    .map_err(|e| err_internal(format!("Query error: {e}")))?;
                let Some(log) = log else {
                    return Err(err_params(format!("Run '{run_id}' not found")));
                };
                let mut out = String::new();
                let _ = writeln!(out, "{} {} {}", log.routine_name, log.status, log.run_id);
                for step in &log.steps {
                    let icon = if step.status == "SUCCESS" { "OK" } else { "FAIL" };
                    let _ = writeln!(
                        out,
                        "[{icon}] {} exit={} {}ms",
                        step.step_id,
                        step.exit_code.unwrap_or(-1),
                        step.execution_time_ms,
                    );
                    if step.status != "SUCCESS"
                        && let Some(stderr) = &step.stderr
                    {
                        let trimmed = stderr.trim();
                        if !trimmed.is_empty() {
                            for line in trimmed.lines().take(10) {
                                let _ = writeln!(out, "  {line}");
                            }
                        }
                    }
                }
                if let Some(output) = &log.output {
                    let trimmed = output.trim();
                    if !trimmed.is_empty() {
                        let _ = writeln!(out, "---\n{trimmed}");
                    }
                }
                return text_ok(out.trim().to_string());
            }
            other => {
                return Err(err_params(format!(
                    "Unknown action '{other}'. Use: schema, get, create, validate, dry_run, test, history, log"
                )));
            }
        }

        // --- dry_run logic ---
        let yaml_content = params
            .yaml_content
            .ok_or_else(|| err_params("'yaml_content' required for dry_run".into()))?;
        let inputs = params.inputs.unwrap_or_default();
        let routine = Routine::from_yaml(&yaml_content)
            .map_err(|e| err_params(format!("Invalid YAML: {e}")))?;

        // Validate required inputs
        for input_def in &routine.inputs {
            if input_def.required && !inputs.contains_key(&input_def.name) {
                return Err(err_params(format!(
                    "Missing required input: {}",
                    input_def.name
                )));
            }
        }

        // Build resolved inputs with defaults
        let mut resolved_inputs = HashMap::new();
        for input_def in &routine.inputs {
            if let Some(value) = inputs.get(&input_def.name) {
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

        // Show routine timeout
        if let Some(timeout) = routine.routine_timeout {
            let _ = writeln!(out, "\nRoutine timeout: {timeout}s");
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

    #[tool(description = "Run a routine by name")]
    async fn run(
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
                // Diagnostic hint
                let failed_step = result
                    .step_results
                    .iter()
                    .find(|s| s.status == StepStatus::Failed);
                if let Some(fs) = failed_step {
                    let hint = if fs.stderr.contains("timeout") || fs.stderr.contains("timed out") {
                        "increase step/routine timeout or optimize command"
                    } else if fs.stderr.contains("Missing required input") {
                        "check required inputs with meta(action='get')"
                    } else {
                        "use meta(action='log', name='<run_id>') for full stderr"
                    };
                    let _ = write!(out, "hint: {hint}");
                }
            }
        }

        text_ok(out.trim().to_string())
    }

}

#[tool_handler]
impl ServerHandler for RoutinesMcpServer {
    fn get_info(&self) -> ServerInfo {
        let mut info = ServerInfo::default();
        info.capabilities = ServerCapabilities::builder().enable_tools().build();
        info.server_info = Implementation::new("routines", env!("CARGO_PKG_VERSION"));
        info.instructions = Some(
            "Routines: workflow engine. list_routines → discover, \
             run → execute, meta → schema/get/create/validate/dry_run/test/history/log."
                .into(),
        );
        info
    }
}
