use std::collections::HashMap;
use std::path::PathBuf;

use rmcp::handler::server::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::*;
use rmcp::{ServerHandler, tool, tool_handler, tool_router};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

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

// --- Response structs ---

#[derive(Debug, Serialize)]
struct RoutineInfo {
    name: String,
    description: String,
    inputs_schema: Vec<InputInfo>,
}

#[derive(Debug, Serialize)]
struct InputInfo {
    name: String,
    required: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    default: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
}

#[derive(Debug, Serialize)]
struct RunResponse {
    run_id: String,
    status: String,
    steps_summary: Vec<StepSummary>,
}

#[derive(Debug, Serialize)]
struct StepSummary {
    step_id: String,
    status: String,
    exit_code: Option<i32>,
    execution_time_ms: u64,
}

#[derive(Debug, Serialize)]
struct CreateResponse {
    success: bool,
    path: String,
}

fn err_internal(msg: String) -> ErrorData {
    ErrorData::internal_error(msg, None)
}

fn err_params(msg: String) -> ErrorData {
    ErrorData::invalid_params(msg, None)
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
        let mut routines = Vec::new();

        if hub_dir.exists() {
            let entries = std::fs::read_dir(&hub_dir)
                .map_err(|e| err_internal(format!("Failed to read hub: {e}")))?;

            for entry in entries.flatten() {
                let path: PathBuf = entry.path();
                if path
                    .extension()
                    .is_some_and(|ext| ext == "yml" || ext == "yaml")
                    && let Ok(routine) = Routine::from_file(&path)
                {
                    routines.push(RoutineInfo {
                        name: routine.name,
                        description: routine.description,
                        inputs_schema: routine
                            .inputs
                            .into_iter()
                            .map(|i| InputInfo {
                                name: i.name,
                                required: i.required,
                                default: i.default,
                                description: i.description,
                            })
                            .collect(),
                    });
                }
            }
        }

        let json =
            serde_json::to_string_pretty(&routines).map_err(|e| err_internal(e.to_string()))?;
        Ok(CallToolResult::success(vec![Content::text(json)]))
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

        let response = RunResponse {
            run_id,
            status: match result.status {
                RunStatus::Success => "SUCCESS".to_string(),
                RunStatus::Failed => "FAILED".to_string(),
            },
            steps_summary: result
                .step_results
                .iter()
                .map(|s| StepSummary {
                    step_id: s.step_id.clone(),
                    status: match s.status {
                        StepStatus::Success => "SUCCESS".to_string(),
                        StepStatus::Failed => "FAILED".to_string(),
                    },
                    exit_code: s.exit_code,
                    execution_time_ms: s.execution_time_ms,
                })
                .collect(),
        };

        let json =
            serde_json::to_string_pretty(&response).map_err(|e| err_internal(e.to_string()))?;
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(description = "Create a new routine from YAML. Validates the YAML before saving.")]
    async fn create_routine(
        &self,
        Parameters(params): Parameters<CreateRoutineParams>,
    ) -> Result<CallToolResult, ErrorData> {
        // Validate YAML parses correctly
        let _routine = Routine::from_yaml(&params.yaml_content)
            .map_err(|e| err_params(format!("Invalid YAML: {e}")))?;

        let hub_dir = routines_dir().join("hub");
        std::fs::create_dir_all(&hub_dir)
            .map_err(|e| err_internal(format!("Failed to create hub dir: {e}")))?;

        let file_path = hub_dir.join(format!("{}.yml", params.name));
        std::fs::write(&file_path, &params.yaml_content)
            .map_err(|e| err_internal(format!("Failed to write file: {e}")))?;

        let response = CreateResponse {
            success: true,
            path: file_path.display().to_string(),
        };

        let json =
            serde_json::to_string_pretty(&response).map_err(|e| err_internal(e.to_string()))?;
        Ok(CallToolResult::success(vec![Content::text(json)]))
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

        match log {
            Some(run_log) => {
                let json = serde_json::to_string_pretty(&run_log)
                    .map_err(|e| err_internal(e.to_string()))?;
                Ok(CallToolResult::success(vec![Content::text(json)]))
            }
            None => Err(err_params(format!("Run '{}' not found", params.run_id))),
        }
    }
}

#[tool_handler]
impl ServerHandler for RoutinesMcpServer {
    fn get_info(&self) -> ServerInfo {
        let mut info = ServerInfo::default();
        info.capabilities = ServerCapabilities::builder().enable_tools().build();
        info.server_info = Implementation::new("routines", env!("CARGO_PKG_VERSION"));
        info.instructions = Some(
            "Routines: deterministic workflow orchestration engine for AI agents. \
             Use list_routines to discover available workflows, run_routine to execute, \
             create_routine to define new ones, and get_run_log to inspect execution history."
                .into(),
        );
        info
    }
}
