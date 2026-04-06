use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;

use crate::context::Context;
use crate::error::Result;
use crate::mcp_config::McpConfig;

use super::{StepResult, StepStatus};

/// Parameters for executing an MCP step.
pub(super) struct McpParams<'a> {
    pub step_id: &'a str,
    pub server: &'a str,
    pub tool: &'a str,
    pub arguments: &'a HashMap<String, serde_json::Value>,
    pub timeout: Option<u64>,
    pub routines_dir: &'a Path,
    pub secrets: &'a HashMap<String, String>,
}

/// Execute an MCP tool call step.
pub(super) fn execute(params: &McpParams, ctx: &Context) -> Result<StepResult> {
    let start = Instant::now();

    // Load MCP config
    let config = McpConfig::load(params.routines_dir)?;
    let server_config = match config.get(params.server) {
        Some(c) => c,
        None => {
            return Ok(StepResult {
                step_id: params.step_id.to_string(),
                status: StepStatus::Failed,
                exit_code: Some(2),
                stdout: String::new(),
                stderr: format!(
                    "MCP server '{}' not found in mcp.json",
                    params.server
                ),
                execution_time_ms: start.elapsed().as_millis() as u64,
            });
        }
    };

    // Resolve template variables in arguments
    let mut resolved_args = serde_json::Map::new();
    for (k, v) in params.arguments {
        let resolved_value = resolve_json_value(v, params.step_id, ctx)?;
        resolved_args.insert(k.clone(), resolved_value);
    }

    // Resolve env with secrets
    let resolved_env = McpConfig::resolve_env(server_config, params.secrets);

    // Build tokio runtime for async rmcp client
    let rt = tokio::runtime::Runtime::new().map_err(crate::error::RoutineError::Io)?;

    let timeout_secs = params.timeout.unwrap_or(30);
    let command = server_config.command.clone();
    let args = server_config.args.clone();
    let tool_name = params.tool.to_string();

    let result = rt.block_on(async {
        use rmcp::ServiceExt;
        use rmcp::model::CallToolRequestParams;
        use rmcp::transport::TokioChildProcess;
        use tokio::process::Command;

        let mut cmd = Command::new(&command);
        cmd.args(&args);
        for (k, v) in &resolved_env {
            cmd.env(k, v);
        }

        let transport = TokioChildProcess::new(cmd)
            .map_err(|e| format!("Failed to spawn MCP server '{}': {e}", command))?;

        let client = tokio::time::timeout(
            std::time::Duration::from_secs(timeout_secs),
            ().serve(transport),
        )
        .await
        .map_err(|_| format!("MCP server initialization timed out after {timeout_secs}s"))?
        .map_err(|e| format!("MCP initialization failed: {e}"))?;

        let call_params =
            CallToolRequestParams::new(tool_name).with_arguments(resolved_args);

        let tool_result = tokio::time::timeout(
            std::time::Duration::from_secs(timeout_secs),
            client.peer().call_tool(call_params),
        )
        .await
        .map_err(|_| format!("MCP tool call timed out after {timeout_secs}s"))?
        .map_err(|e| format!("MCP tool call failed: {e}"))?;

        // Graceful shutdown
        let _ = client.cancel().await;

        Ok::<_, String>(tool_result)
    });

    let elapsed = start.elapsed().as_millis() as u64;

    match result {
        Ok(tool_result) => {
            let is_error = tool_result.is_error.unwrap_or(false);
            let stdout: String = tool_result
                .content
                .iter()
                .filter_map(|c| c.as_text().map(|t| t.text.as_ref()))
                .collect::<Vec<_>>()
                .join("\n");

            let (status, exit_code, stderr) = if is_error {
                (StepStatus::Failed, Some(1), stdout.clone())
            } else {
                (StepStatus::Success, Some(0), String::new())
            };

            Ok(StepResult {
                step_id: params.step_id.to_string(),
                status,
                exit_code,
                stdout,
                stderr,
                execution_time_ms: elapsed,
            })
        }
        Err(err_msg) => Ok(StepResult {
            step_id: params.step_id.to_string(),
            status: StepStatus::Failed,
            exit_code: Some(2),
            stdout: String::new(),
            stderr: err_msg,
            execution_time_ms: elapsed,
        }),
    }
}

/// Recursively resolve `{{ ... }}` templates inside JSON values.
fn resolve_json_value(
    value: &serde_json::Value,
    step_id: &str,
    ctx: &Context,
) -> Result<serde_json::Value> {
    match value {
        serde_json::Value::String(s) => {
            let resolved = ctx.resolve(s, step_id)?;
            Ok(serde_json::Value::String(resolved))
        }
        serde_json::Value::Array(arr) => {
            let resolved: Result<Vec<_>> = arr
                .iter()
                .map(|v| resolve_json_value(v, step_id, ctx))
                .collect();
            Ok(serde_json::Value::Array(resolved?))
        }
        serde_json::Value::Object(obj) => {
            let mut resolved = serde_json::Map::new();
            for (k, v) in obj {
                resolved.insert(k.clone(), resolve_json_value(v, step_id, ctx)?);
            }
            Ok(serde_json::Value::Object(resolved))
        }
        // Numbers, bools, null pass through unchanged
        other => Ok(other.clone()),
    }
}
