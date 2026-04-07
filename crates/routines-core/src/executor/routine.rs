use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;

use crate::context::Context;
use crate::error::Result;
use crate::parser::Routine;
use crate::resolve::resolve_routine_path;

use super::{StepResult, StepStatus, run_routine_with_depth};

const MAX_DEPTH: u32 = 10;

/// Parameters for executing a sub-routine step.
pub(super) struct RoutineParams<'a> {
    pub step_id: &'a str,
    pub name: &'a str,
    pub input_templates: &'a HashMap<String, String>,
    pub timeout: Option<u64>,
    pub depth: u32,
    pub secrets: &'a HashMap<String, String>,
    pub routines_dir: &'a Path,
}

/// Execute a sub-routine step.
pub(super) fn execute(params: &RoutineParams, ctx: &Context) -> Result<StepResult> {
    if params.depth >= MAX_DEPTH {
        return Ok(StepResult {
            step_id: params.step_id.to_string(),
            status: StepStatus::Failed,
            exit_code: Some(1),
            stdout: String::new(),
            stderr: format!("Max routine nesting depth ({MAX_DEPTH}) exceeded"),
            execution_time_ms: 0,
            diagnostic: None,
            headers: HashMap::new(),
        });
    }

    // Load sub-routine
    let yaml_path = resolve_routine_path(params.name, params.routines_dir);
    if !yaml_path.exists() {
        return Ok(StepResult {
            step_id: params.step_id.to_string(),
            status: StepStatus::Failed,
            exit_code: Some(1),
            stdout: String::new(),
            stderr: format!(
                "Sub-routine '{}' not found: {}",
                params.name,
                yaml_path.display()
            ),
            execution_time_ms: 0,
            diagnostic: None,
            headers: HashMap::new(),
        });
    }

    let sub_routine = match Routine::from_file(&yaml_path) {
        Ok(r) => r,
        Err(e) => {
            return Ok(StepResult {
                step_id: params.step_id.to_string(),
                status: StepStatus::Failed,
                exit_code: Some(1),
                stdout: String::new(),
                stderr: format!("Failed to parse sub-routine '{}': {e}", params.name),
                execution_time_ms: 0,
                diagnostic: None,
                headers: HashMap::new(),
            });
        }
    };

    // Resolve input templates
    let mut resolved_inputs = HashMap::new();
    for (k, v) in params.input_templates {
        resolved_inputs.insert(k.clone(), ctx.resolve(v, params.step_id)?);
    }

    let start = Instant::now();

    // Execute sub-routine
    let result = run_routine_with_depth(
        &sub_routine,
        resolved_inputs,
        params.secrets.clone(),
        params.routines_dir.to_path_buf(),
        params.depth + 1,
        None,
    )?;

    let elapsed = start.elapsed().as_millis() as u64;
    if let Some(t) = params.timeout
        && elapsed > t * 1000
    {
        return Ok(StepResult {
            step_id: params.step_id.to_string(),
            status: StepStatus::Failed,
            exit_code: None,
            stdout: String::new(),
            stderr: format!("Sub-routine timed out after {t}s"),
            execution_time_ms: elapsed,
            diagnostic: None,
            headers: HashMap::new(),
        });
    }

    // Aggregate sub-routine outputs
    let stdout: String = result
        .step_results
        .iter()
        .filter(|s| !s.stdout.trim().is_empty())
        .map(|s| s.stdout.trim())
        .collect::<Vec<_>>()
        .join("\n");
    let stderr: String = result
        .step_results
        .iter()
        .filter(|s| !s.stderr.trim().is_empty())
        .map(|s| s.stderr.trim())
        .collect::<Vec<_>>()
        .join("\n");

    let (status, exit_code) = match result.status {
        super::RunStatus::Success => (StepStatus::Success, Some(0)),
        super::RunStatus::Failed => (StepStatus::Failed, Some(1)),
    };

    Ok(StepResult {
        step_id: params.step_id.to_string(),
        status,
        exit_code,
        stdout,
        stderr,
        execution_time_ms: elapsed,
        diagnostic: None,
        headers: HashMap::new(),
    })
}
