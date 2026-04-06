use std::collections::HashMap;
use std::io::Write;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use crate::context::Context;
use crate::error::{Result, RoutineError};

use super::{StepResult, StepStatus, check_dangerous};

/// Parameters for executing a CLI step.
pub(super) struct CliParams<'a> {
    pub step_id: &'a str,
    pub command: &'a str,
    pub args: &'a [String],
    pub env: &'a HashMap<String, String>,
    pub stdin_template: Option<&'a str>,
    pub working_dir_template: Option<&'a str>,
    pub timeout: Option<u64>,
    pub strict_mode: bool,
    /// Secrets to inject as environment variables (from routine-level secrets_env).
    pub secrets_env: &'a HashMap<String, String>,
}

/// Execute a CLI (subprocess) step.
pub(super) fn execute(params: &CliParams, ctx: &Context) -> Result<StepResult> {
    let resolved_command = ctx.resolve(params.command, params.step_id)?;
    let resolved_args: Vec<String> = params
        .args
        .iter()
        .map(|a| ctx.resolve(a, params.step_id))
        .collect::<Result<_>>()?;

    if params.strict_mode {
        check_dangerous(params.step_id, &resolved_command, &resolved_args)?;
    }

    let resolved_env: HashMap<String, String> = params
        .env
        .iter()
        .map(|(k, v)| Ok((k.clone(), ctx.resolve(v, params.step_id)?)))
        .collect::<Result<_>>()?;
    let stdin_data = match params.stdin_template {
        Some(tmpl) => Some(ctx.resolve(tmpl, params.step_id)?),
        None => None,
    };
    let working_dir = match params.working_dir_template {
        Some(tmpl) => Some(ctx.resolve(tmpl, params.step_id)?),
        None => None,
    };

    let start = Instant::now();

    let mut cmd = Command::new(&resolved_command);
    cmd.args(&resolved_args)
        .envs(params.secrets_env)  // secrets_env first (lower priority)
        .envs(&resolved_env)       // step-level env overrides
        .stdin(if stdin_data.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    if let Some(dir) = &working_dir {
        cmd.current_dir(dir);
    }

    let mut child = cmd.spawn().map_err(RoutineError::Io)?;

    if let (Some(data), Some(mut stdin)) = (&stdin_data, child.stdin.take()) {
        stdin.write_all(data.as_bytes())?;
    }

    let output = if let Some(timeout_secs) = params.timeout {
        let deadline = Instant::now() + Duration::from_secs(timeout_secs);
        loop {
            match child.try_wait() {
                Ok(Some(_status)) => break child.wait_with_output()?,
                Ok(None) => {
                    if Instant::now() >= deadline {
                        let _ = child.kill();
                        let _ = child.wait();
                        let elapsed = start.elapsed().as_millis() as u64;
                        return Ok(StepResult {
                            step_id: params.step_id.to_string(),
                            status: StepStatus::Failed,
                            exit_code: None,
                            stdout: String::new(),
                            stderr: format!("Timed out after {timeout_secs}s"),
                            execution_time_ms: elapsed,
                        });
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(e) => return Err(RoutineError::Io(e)),
            }
        }
    } else {
        child.wait_with_output()?
    };

    let elapsed = start.elapsed().as_millis() as u64;
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    let (status, exit_code) = if output.status.success() {
        (StepStatus::Success, output.status.code())
    } else {
        match output.status.code() {
            Some(code) => (StepStatus::Failed, Some(code)),
            None => {
                return Err(RoutineError::StepKilled {
                    step_id: params.step_id.to_string(),
                });
            }
        }
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
