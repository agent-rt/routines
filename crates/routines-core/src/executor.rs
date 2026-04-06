use std::collections::HashMap;
use std::io::Write;
use std::process::{Command, Stdio};
use std::time::Instant;

use crate::context::{Context, StepOutput};
use crate::error::{Result, RoutineError};
use crate::parser::{Routine, Step};

/// Result of executing a single step.
#[derive(Debug, Clone)]
pub struct StepResult {
    pub step_id: String,
    pub status: StepStatus,
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub execution_time_ms: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum StepStatus {
    Success,
    Failed,
}

/// Result of executing an entire routine.
#[derive(Debug)]
pub struct RunResult {
    pub status: RunStatus,
    pub step_results: Vec<StepResult>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RunStatus {
    Success,
    Failed,
}

/// Patterns that trigger strict_mode rejection.
const DANGEROUS_PATTERNS: &[&str] = &[
    "rm -rf",
    "rm -fr",
    "mkfs",
    "dd if=",
    "> /dev/sd",
    "chmod 777",
    ":(){ :|:& };:",
    "shutdown",
    "reboot",
    "init 0",
    "init 6",
];

/// Check if a resolved command line contains dangerous patterns.
fn check_dangerous(step_id: &str, command: &str, args: &[String]) -> Result<()> {
    let full_command = format!("{} {}", command, args.join(" "));
    let lower = full_command.to_lowercase();
    for pattern in DANGEROUS_PATTERNS {
        if lower.contains(pattern) {
            return Err(RoutineError::DangerousCommand {
                step_id: step_id.to_string(),
                command: full_command,
            });
        }
    }
    Ok(())
}

/// Execute a full routine with the given inputs and secrets.
pub fn run_routine(
    routine: &Routine,
    inputs: HashMap<String, String>,
    secrets: HashMap<String, String>,
) -> Result<RunResult> {
    // Validate required inputs
    for input_def in &routine.inputs {
        if input_def.required && !inputs.contains_key(&input_def.name) {
            return Err(RoutineError::MissingInput(input_def.name.clone()));
        }
    }

    // Build inputs with defaults applied
    let mut resolved_inputs = HashMap::new();
    for input_def in &routine.inputs {
        if let Some(value) = inputs.get(&input_def.name) {
            resolved_inputs.insert(input_def.name.clone(), value.clone());
        } else if let Some(default) = &input_def.default {
            resolved_inputs.insert(input_def.name.clone(), default.clone());
        }
    }

    let mut ctx = Context::new(resolved_inputs, secrets);
    let mut step_results = Vec::new();

    for step in &routine.steps {
        let result = execute_step(step, &ctx, routine.strict_mode)?;

        let status = result.status.clone();
        ctx.add_step_output(
            step.id.clone(),
            StepOutput {
                stdout: result.stdout.trim().to_string(),
                stderr: result.stderr.trim().to_string(),
            },
        );
        step_results.push(result);

        if status == StepStatus::Failed {
            return Ok(RunResult {
                status: RunStatus::Failed,
                step_results,
            });
        }
    }

    Ok(RunResult {
        status: RunStatus::Success,
        step_results,
    })
}

/// Execute a single CLI step.
fn execute_step(step: &Step, ctx: &Context, strict_mode: bool) -> Result<StepResult> {
    let command = ctx.resolve(&step.command, &step.id)?;
    let args: Vec<String> = step
        .args
        .iter()
        .map(|a| ctx.resolve(a, &step.id))
        .collect::<Result<_>>()?;

    if strict_mode {
        check_dangerous(&step.id, &command, &args)?;
    }

    let env: HashMap<String, String> = step
        .env
        .iter()
        .map(|(k, v)| Ok((k.clone(), ctx.resolve(v, &step.id)?)))
        .collect::<Result<_>>()?;
    let stdin_data = match &step.stdin {
        Some(tmpl) => Some(ctx.resolve(tmpl, &step.id)?),
        None => None,
    };

    let start = Instant::now();

    let mut child = Command::new(&command)
        .args(&args)
        .envs(&env)
        .stdin(if stdin_data.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(RoutineError::Io)?;

    if let (Some(data), Some(mut stdin)) = (&stdin_data, child.stdin.take()) {
        stdin.write_all(data.as_bytes())?;
    }

    let output = child.wait_with_output()?;
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
                    step_id: step.id.clone(),
                });
            }
        }
    };

    Ok(StepResult {
        step_id: step.id.clone(),
        status,
        exit_code,
        stdout,
        stderr,
        execution_time_ms: elapsed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_mode_blocks_rm_rf() {
        let routine = Routine::from_yaml(
            r#"
name: danger
description: test
strict_mode: true
inputs: []
steps:
  - id: nuke
    type: cli
    command: rm
    args: ["-rf", "/"]
"#,
        )
        .unwrap();

        let result = run_routine(&routine, HashMap::new(), HashMap::new());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Dangerous command blocked"));
        assert!(err.contains("rm -rf"));
    }

    #[test]
    fn strict_mode_off_allows_anything() {
        let routine = Routine::from_yaml(
            r#"
name: safe
description: test
strict_mode: false
inputs: []
steps:
  - id: greet
    type: cli
    command: echo
    args: ["hello"]
"#,
        )
        .unwrap();

        let result = run_routine(&routine, HashMap::new(), HashMap::new());
        assert!(result.is_ok());
        assert_eq!(result.unwrap().status, RunStatus::Success);
    }

    #[test]
    fn strict_mode_blocks_case_insensitive() {
        let result = check_dangerous("test", "RM", &["-RF".to_string(), "/".to_string()]);
        assert!(result.is_err());
    }
}
