use std::collections::HashMap;
use std::io::Write;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use crate::context::{Context, StepOutput};
use crate::error::{Result, RoutineError};
use crate::parser::{OnFail, Routine, Step, StepType};

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
    Skipped,
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

/// Evaluate a simple condition expression.
/// Supports: `A == B` (equal), `A != B` (not equal), or truthy (non-empty string).
fn evaluate_condition(expr: &str) -> bool {
    let trimmed = expr.trim();
    if let Some((left, right)) = trimmed.split_once("==") {
        // Check it's not actually !=
        if left.ends_with('!') {
            let left = left.strip_suffix('!').unwrap().trim();
            return left != right.trim();
        }
        return left.trim() == right.trim();
    }
    if let Some((left, right)) = trimmed.split_once("!=") {
        return left.trim() != right.trim();
    }
    // Truthy: non-empty and not "false" or "0"
    !trimmed.is_empty() && trimmed != "false" && trimmed != "0"
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
    let mut has_failure = false;

    for step in &routine.steps {
        // Evaluate `when` condition
        if let Some(when_expr) = &step.when {
            let resolved = ctx.resolve(when_expr, &step.id)?;
            if !evaluate_condition(&resolved) {
                let result = StepResult {
                    step_id: step.id.clone(),
                    status: StepStatus::Skipped,
                    exit_code: None,
                    stdout: String::new(),
                    stderr: String::new(),
                    execution_time_ms: 0,
                };
                ctx.add_step_output(
                    step.id.clone(),
                    StepOutput {
                        stdout: String::new(),
                        stderr: String::new(),
                        exit_code: None,
                    },
                );
                step_results.push(result);
                continue;
            }
        }

        let result = match step.step_type {
            StepType::Cli => execute_cli_step(step, &ctx, routine.strict_mode)?,
            StepType::Api => execute_api_step(step, &ctx)?,
        };

        let status = result.status.clone();
        ctx.add_step_output(
            step.id.clone(),
            StepOutput {
                stdout: result.stdout.trim().to_string(),
                stderr: result.stderr.trim().to_string(),
                exit_code: result.exit_code,
            },
        );
        step_results.push(result);

        if status == StepStatus::Failed {
            if step.on_fail == OnFail::Continue {
                has_failure = true;
            } else {
                return Ok(RunResult {
                    status: RunStatus::Failed,
                    step_results,
                });
            }
        }
    }

    Ok(RunResult {
        status: if has_failure {
            RunStatus::Failed
        } else {
            RunStatus::Success
        },
        step_results,
    })
}

/// Execute a single CLI step.
fn execute_cli_step(step: &Step, ctx: &Context, strict_mode: bool) -> Result<StepResult> {
    let command_template = step.command.as_deref().unwrap_or_default();
    let command = ctx.resolve(command_template, &step.id)?;
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
    let working_dir = match &step.working_dir {
        Some(tmpl) => Some(ctx.resolve(tmpl, &step.id)?),
        None => None,
    };

    let start = Instant::now();

    let mut cmd = Command::new(&command);
    cmd.args(&args)
        .envs(&env)
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

    // Handle timeout via polling
    let output = if let Some(timeout_secs) = step.timeout {
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
                            step_id: step.id.clone(),
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

/// Execute a single API (HTTP) step.
fn execute_api_step(step: &Step, ctx: &Context) -> Result<StepResult> {
    let url_template = step.url.as_deref().unwrap_or_default();
    let url = ctx.resolve(url_template, &step.id)?;
    let method = ctx.resolve(&step.method, &step.id)?;

    let start = Instant::now();

    // Build agent with timeout
    let config = if let Some(timeout_secs) = step.timeout {
        ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(timeout_secs)))
            .build()
    } else {
        ureq::config::Config::default()
    };
    let agent = ureq::Agent::new_with_config(config);

    // Resolve headers
    let mut resolved_headers: Vec<(String, String)> = Vec::new();
    for (key, val_template) in &step.headers {
        let val = ctx.resolve(val_template, &step.id)?;
        resolved_headers.push((key.clone(), val));
    }

    // Resolve body
    let body_data = match &step.body {
        Some(tmpl) => Some(ctx.resolve(tmpl, &step.id)?),
        None => None,
    };

    // Build and send request
    let send_result = (|| -> std::result::Result<(u16, String), ureq::Error> {
        let method_upper = method.to_uppercase();

        // Build request using method dispatch
        macro_rules! build_req {
            ($builder:expr) => {{
                let mut req = $builder;
                for (k, v) in &resolved_headers {
                    req = req.header(k.as_str(), v.as_str());
                }
                req
            }};
        }

        let (status_code, body) = match method_upper.as_str() {
            "POST" => {
                let req = build_req!(agent.post(&url));
                let mut resp = if let Some(b) = &body_data {
                    req.send(b.as_bytes())?
                } else {
                    req.send_empty()?
                };
                (
                    resp.status(),
                    resp.body_mut().read_to_string().unwrap_or_default(),
                )
            }
            "PUT" => {
                let req = build_req!(agent.put(&url));
                let mut resp = if let Some(b) = &body_data {
                    req.send(b.as_bytes())?
                } else {
                    req.send_empty()?
                };
                (
                    resp.status(),
                    resp.body_mut().read_to_string().unwrap_or_default(),
                )
            }
            "PATCH" => {
                let req = build_req!(agent.patch(&url));
                let mut resp = if let Some(b) = &body_data {
                    req.send(b.as_bytes())?
                } else {
                    req.send_empty()?
                };
                (
                    resp.status(),
                    resp.body_mut().read_to_string().unwrap_or_default(),
                )
            }
            "DELETE" => {
                let req = build_req!(agent.delete(&url));
                let mut resp = req.call()?;
                (
                    resp.status(),
                    resp.body_mut().read_to_string().unwrap_or_default(),
                )
            }
            "HEAD" => {
                let req = build_req!(agent.head(&url));
                let mut resp = req.call()?;
                (
                    resp.status(),
                    resp.body_mut().read_to_string().unwrap_or_default(),
                )
            }
            _ => {
                // Default to GET
                let req = build_req!(agent.get(&url));
                let mut resp = req.call()?;
                (
                    resp.status(),
                    resp.body_mut().read_to_string().unwrap_or_default(),
                )
            }
        };

        Ok((u16::from(status_code), body))
    })();

    let elapsed = start.elapsed().as_millis() as u64;

    match send_result {
        Ok((status_code, body)) => {
            let status_text = format!("HTTP {status_code} {method}");
            let success = (200..300).contains(&(status_code as i32));

            Ok(StepResult {
                step_id: step.id.clone(),
                status: if success {
                    StepStatus::Success
                } else {
                    StepStatus::Failed
                },
                exit_code: Some(if success { 0 } else { 1 }),
                stdout: body,
                stderr: status_text,
                execution_time_ms: elapsed,
            })
        }
        Err(e) => Ok(StepResult {
            step_id: step.id.clone(),
            status: StepStatus::Failed,
            exit_code: Some(1),
            stdout: String::new(),
            stderr: format!("HTTP error: {e}"),
            execution_time_ms: elapsed,
        }),
    }
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

    #[test]
    fn when_true_executes_step() {
        let routine = Routine::from_yaml(
            r#"
name: when_true
description: test
inputs:
  - name: ENV
    required: true
steps:
  - id: greet
    type: cli
    command: echo
    args: ["hello"]
    when: "{{ inputs.ENV }} == staging"
"#,
        )
        .unwrap();

        let mut inputs = HashMap::new();
        inputs.insert("ENV".to_string(), "staging".to_string());
        let result = run_routine(&routine, inputs, HashMap::new()).unwrap();
        assert_eq!(result.status, RunStatus::Success);
        assert_eq!(result.step_results[0].status, StepStatus::Success);
    }

    #[test]
    fn when_false_skips_step() {
        let routine = Routine::from_yaml(
            r#"
name: when_false
description: test
inputs:
  - name: ENV
    required: true
steps:
  - id: greet
    type: cli
    command: echo
    args: ["hello"]
    when: "{{ inputs.ENV }} == production"
"#,
        )
        .unwrap();

        let mut inputs = HashMap::new();
        inputs.insert("ENV".to_string(), "staging".to_string());
        let result = run_routine(&routine, inputs, HashMap::new()).unwrap();
        assert_eq!(result.status, RunStatus::Success);
        assert_eq!(result.step_results[0].status, StepStatus::Skipped);
    }

    #[test]
    fn on_fail_continue_proceeds() {
        let routine = Routine::from_yaml(
            r#"
name: continue_test
description: test
steps:
  - id: fail_step
    type: cli
    command: /bin/sh
    args: ["-c", "exit 1"]
    on_fail: continue
  - id: after
    type: cli
    command: echo
    args: ["still running"]
"#,
        )
        .unwrap();

        let result = run_routine(&routine, HashMap::new(), HashMap::new()).unwrap();
        assert_eq!(result.status, RunStatus::Failed);
        assert_eq!(result.step_results.len(), 2);
        assert_eq!(result.step_results[0].status, StepStatus::Failed);
        assert_eq!(result.step_results[1].status, StepStatus::Success);
    }

    #[test]
    fn on_fail_stop_halts() {
        let routine = Routine::from_yaml(
            r#"
name: stop_test
description: test
steps:
  - id: fail_step
    type: cli
    command: /bin/sh
    args: ["-c", "exit 1"]
  - id: never
    type: cli
    command: echo
    args: ["never reached"]
"#,
        )
        .unwrap();

        let result = run_routine(&routine, HashMap::new(), HashMap::new()).unwrap();
        assert_eq!(result.status, RunStatus::Failed);
        assert_eq!(result.step_results.len(), 1);
    }

    #[test]
    fn exit_code_template_variable() {
        let routine = Routine::from_yaml(
            r#"
name: exit_code_test
description: test
steps:
  - id: maybe_fail
    type: cli
    command: /bin/sh
    args: ["-c", "exit 42"]
    on_fail: continue
  - id: check
    type: cli
    command: echo
    args: ["code={{ maybe_fail.exit_code }}"]
"#,
        )
        .unwrap();

        let result = run_routine(&routine, HashMap::new(), HashMap::new()).unwrap();
        assert_eq!(result.step_results.len(), 2);
        assert!(result.step_results[1].stdout.contains("code=42"));
    }

    #[test]
    fn evaluate_condition_tests() {
        assert!(evaluate_condition("staging == staging"));
        assert!(!evaluate_condition("staging == production"));
        assert!(evaluate_condition("a != b"));
        assert!(!evaluate_condition("same != same"));
        assert!(evaluate_condition("nonempty"));
        assert!(!evaluate_condition(""));
        assert!(!evaluate_condition("false"));
        assert!(!evaluate_condition("0"));
    }

    #[test]
    fn api_step_parse() {
        let routine = Routine::from_yaml(
            r#"
name: api_test
description: test api
steps:
  - id: fetch
    type: api
    url: "https://httpbin.org/get"
    method: GET
    headers:
      Accept: application/json
"#,
        )
        .unwrap();

        assert_eq!(routine.steps[0].step_type, StepType::Api);
        assert_eq!(
            routine.steps[0].url.as_deref(),
            Some("https://httpbin.org/get")
        );
        assert_eq!(routine.steps[0].method, "GET");
    }

    #[test]
    fn api_step_validation_no_url() {
        let result = Routine::from_yaml(
            r#"
name: bad_api
description: test
steps:
  - id: no_url
    type: api
    method: GET
"#,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("url"));
    }

    #[test]
    fn cli_step_validation_no_command() {
        let result = Routine::from_yaml(
            r#"
name: bad_cli
description: test
steps:
  - id: no_cmd
    type: cli
"#,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("command"));
    }
}
