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
use crate::parser::Routine;
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

fn err_internal(msg: String) -> ErrorData {
    ErrorData::internal_error(msg, None)
}

fn err_params(msg: String) -> ErrorData {
    ErrorData::invalid_params(msg, None)
}

fn text_ok(text: String) -> Result<CallToolResult, ErrorData> {
    Ok(CallToolResult::success(vec![Content::text(text)]))
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
        let hub_dir = routines_dir().join("hub");
        let mut out = String::new();

        if hub_dir.exists() {
            let entries = std::fs::read_dir(&hub_dir)
                .map_err(|e| err_internal(format!("Failed to read hub: {e}")))?;

            let mut has_required = false;
            for entry in entries.flatten() {
                let path: PathBuf = entry.path();
                if path
                    .extension()
                    .is_some_and(|ext| ext == "yml" || ext == "yaml")
                    && let Ok(routine) = Routine::from_file(&path)
                {
                    let inputs_desc = if routine.inputs.is_empty() {
                        String::new()
                    } else {
                        let parts: Vec<String> = routine
                            .inputs
                            .iter()
                            .map(|i| {
                                if i.required {
                                    has_required = true;
                                    format!("{}*", i.name)
                                } else {
                                    i.name.clone()
                                }
                            })
                            .collect();
                        format!(" (inputs: {})", parts.join(", "))
                    };
                    let _ = writeln!(
                        out,
                        "{} — {}{}",
                        routine.name, routine.description, inputs_desc
                    );
                }
            }
            if has_required {
                let _ = write!(out, "(* = required)");
            }
        }

        if out.is_empty() {
            out = "No routines found".to_string();
        }

        text_ok(out.trim().to_string())
    }

    #[tool(description = "Execute a routine by name with the given input parameters")]
    async fn run_routine(
        &self,
        Parameters(params): Parameters<RunRoutineParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let yaml_path = routines_dir()
            .join("hub")
            .join(format!("{}.yml", params.name));
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
                // Compact: one-line summary
                let _ = write!(
                    out,
                    "run={short_id} SUCCESS {total_ms}ms {total_steps}/{total_steps} steps"
                );
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

        let hub_dir = routines_dir().join("hub");
        std::fs::create_dir_all(&hub_dir)
            .map_err(|e| err_internal(format!("Failed to create hub dir: {e}")))?;

        let file_path = hub_dir.join(format!("{}.yml", params.name));
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
            "Routines: deterministic workflow engine for AI agents. \
             list_routines → discover, run_routine → execute, \
             create_routine → define, get_run_log → inspect."
                .into(),
        );
        info
    }
}
