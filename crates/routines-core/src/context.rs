use std::collections::HashMap;

use crate::error::{Result, RoutineError};

/// Holds all resolved values available for template substitution.
#[derive(Clone)]
pub struct Context {
    inputs: HashMap<String, String>,
    secrets: HashMap<String, String>,
    step_outputs: HashMap<String, StepOutput>,
    /// Temporary iteration variables: `item` and `item_index`.
    iteration: Option<IterationVars>,
    /// Run status for finally blocks: "SUCCESS" or "FAILED".
    run_status: Option<String>,
    /// Mock responses for testing: step_id → mock output.
    mocks: HashMap<String, MockResponse>,
}

/// Mock response injected instead of real step execution during testing.
#[derive(Debug, Clone, Default)]
pub struct MockResponse {
    pub stdout: Option<String>,
    pub stderr: Option<String>,
    pub exit_code: Option<i32>,
}

/// Variables injected during a for_each iteration.
#[derive(Debug, Clone)]
pub struct IterationVars {
    pub item: String,
    pub item_index: usize,
}

/// Captured output from a completed step.
#[derive(Debug, Clone)]
pub struct StepOutput {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: Option<i32>,
    /// HTTP response headers (only populated by HTTP steps).
    #[allow(dead_code)]
    pub headers: HashMap<String, String>,
}

impl Context {
    pub fn new(inputs: HashMap<String, String>, secrets: HashMap<String, String>) -> Self {
        Self {
            inputs,
            secrets,
            step_outputs: HashMap::new(),
            iteration: None,
            run_status: None,
            mocks: HashMap::new(),
        }
    }

    /// Load mock responses for testing.
    pub fn set_mocks(&mut self, mocks: HashMap<String, MockResponse>) {
        self.mocks = mocks;
    }

    /// Get a mock response for a step, if one exists.
    pub fn get_mock(&self, step_id: &str) -> Option<&MockResponse> {
        self.mocks.get(step_id)
    }

    /// Set the run status for use in finally blocks.
    pub fn set_run_status(&mut self, status: &str) {
        self.run_status = Some(status.to_string());
    }

    /// Set iteration variables for a for_each loop. Returns previous value for restoration.
    pub fn set_iteration(&mut self, item: String, index: usize) -> Option<IterationVars> {
        self.iteration.replace(IterationVars {
            item,
            item_index: index,
        })
    }

    /// Clear iteration variables, restoring previous state.
    pub fn restore_iteration(&mut self, prev: Option<IterationVars>) {
        self.iteration = prev;
    }

    /// Register a completed step's output for later reference.
    pub fn add_step_output(&mut self, step_id: String, output: StepOutput) {
        self.step_outputs.insert(step_id, output);
    }

    /// Resolve all `{{ ... }}` templates in a string.
    /// Returns Err on undefined variables — no implicit empty values.
    pub fn resolve(&self, template: &str, current_step_id: &str) -> Result<String> {
        let mut result = String::with_capacity(template.len());
        let mut rest = template;

        while let Some(start) = rest.find("{{") {
            result.push_str(&rest[..start]);
            let after_open = &rest[start + 2..];
            let end = after_open
                .find("}}")
                .ok_or_else(|| RoutineError::UndefinedVariable {
                    step_id: current_step_id.to_string(),
                    key: "unclosed {{ template".to_string(),
                })?;
            let key = after_open[..end].trim();
            let value = self.lookup(key, current_step_id)?;
            result.push_str(&value);
            rest = &after_open[end + 2..];
        }
        result.push_str(rest);
        Ok(result)
    }

    /// Look up a single template key like `inputs.ENV`, `secrets.KEY`, `step_id.stdout`,
    /// `item`, or `item_index`.
    fn lookup(&self, key: &str, current_step_id: &str) -> Result<String> {
        // Handle bare iteration variables (no dot)
        match key {
            "item" => {
                return self
                    .iteration
                    .as_ref()
                    .map(|v| v.item.clone())
                    .ok_or_else(|| RoutineError::UndefinedVariable {
                        step_id: current_step_id.to_string(),
                        key: key.to_string(),
                    });
            }
            "item_index" => {
                return self
                    .iteration
                    .as_ref()
                    .map(|v| v.item_index.to_string())
                    .ok_or_else(|| RoutineError::UndefinedVariable {
                        step_id: current_step_id.to_string(),
                        key: key.to_string(),
                    });
            }
            _ => {}
        }

        let (prefix, suffix) =
            key.split_once('.')
                .ok_or_else(|| RoutineError::UndefinedVariable {
                    step_id: current_step_id.to_string(),
                    key: key.to_string(),
                })?;

        match prefix {
            "inputs" => {
                self.inputs
                    .get(suffix)
                    .cloned()
                    .ok_or_else(|| RoutineError::UndefinedVariable {
                        step_id: current_step_id.to_string(),
                        key: key.to_string(),
                    })
            }
            "secrets" => {
                self.secrets
                    .get(suffix)
                    .cloned()
                    .ok_or_else(|| RoutineError::UndefinedVariable {
                        step_id: current_step_id.to_string(),
                        key: key.to_string(),
                    })
            }
            "_run" => match suffix {
                "status" => {
                    self.run_status
                        .clone()
                        .ok_or_else(|| RoutineError::UndefinedVariable {
                            step_id: current_step_id.to_string(),
                            key: key.to_string(),
                        })
                }
                _ => Err(RoutineError::UndefinedVariable {
                    step_id: current_step_id.to_string(),
                    key: key.to_string(),
                }),
            },
            _ => {
                // Treat prefix as a step_id
                let output = self
                    .step_outputs
                    .get(prefix)
                    .ok_or(RoutineError::StepNotExecuted(prefix.to_string()))?;
                match suffix {
                    "stdout" => Ok(output.stdout.clone()),
                    "stderr" => Ok(output.stderr.clone()),
                    "exit_code" => Ok(output
                        .exit_code
                        .map(|c| c.to_string())
                        .unwrap_or_else(|| "-1".to_string())),
                    "stdout_lines" => {
                        let lines: Vec<&str> =
                            output.stdout.lines().filter(|l| !l.is_empty()).collect();
                        Ok(serde_json::to_string(&lines).unwrap_or_else(|_| "[]".to_string()))
                    }
                    s if s.starts_with("headers.") => {
                        let header_name = &s["headers.".len()..];
                        output.headers.get(header_name).cloned().ok_or_else(|| {
                            RoutineError::UndefinedVariable {
                                step_id: current_step_id.to_string(),
                                key: key.to_string(),
                            }
                        })
                    }
                    _ => Err(RoutineError::UndefinedVariable {
                        step_id: current_step_id.to_string(),
                        key: key.to_string(),
                    }),
                }
            }
        }
    }

    /// Get a step's output by ID.
    pub fn get_step_output(&self, step_id: &str) -> Option<&StepOutput> {
        self.step_outputs.get(step_id)
    }

    /// Return all secret values (for redaction).
    pub fn secret_values(&self) -> Vec<&str> {
        self.secrets.values().map(|s| s.as_str()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_inputs() {
        let mut inputs = HashMap::new();
        inputs.insert("ENV".to_string(), "staging".to_string());
        let ctx = Context::new(inputs, HashMap::new());
        let result = ctx.resolve("deploy to {{ inputs.ENV }}", "test").unwrap();
        assert_eq!(result, "deploy to staging");
    }

    #[test]
    fn resolve_step_output() {
        let ctx_inputs = HashMap::new();
        let mut ctx = Context::new(ctx_inputs, HashMap::new());
        ctx.add_step_output(
            "build".to_string(),
            StepOutput {
                stdout: "build-ok".to_string(),
                stderr: String::new(),
                exit_code: Some(0),
                headers: HashMap::new(),
            },
        );
        let result = ctx.resolve("result: {{ build.stdout }}", "notify").unwrap();
        assert_eq!(result, "result: build-ok");
    }

    #[test]
    fn undefined_variable_errors() {
        let ctx = Context::new(HashMap::new(), HashMap::new());
        let err = ctx.resolve("{{ inputs.MISSING }}", "test").unwrap_err();
        assert!(err.to_string().contains("MISSING"));
    }

    #[test]
    fn unexecuted_step_errors() {
        let ctx = Context::new(HashMap::new(), HashMap::new());
        let err = ctx.resolve("{{ build.stdout }}", "test").unwrap_err();
        assert!(err.to_string().contains("build"));
    }

    #[test]
    fn resolve_stdout_lines() {
        let mut ctx = Context::new(HashMap::new(), HashMap::new());
        ctx.add_step_output(
            "list".to_string(),
            StepOutput {
                stdout: "alpha\nbeta\ngamma\n".to_string(),
                stderr: String::new(),
                exit_code: Some(0),
                headers: HashMap::new(),
            },
        );
        let result = ctx.resolve("{{ list.stdout_lines }}", "test").unwrap();
        let lines: Vec<String> = serde_json::from_str(&result).unwrap();
        assert_eq!(lines, vec!["alpha", "beta", "gamma"]);
    }

    #[test]
    fn resolve_item_and_index() {
        let mut ctx = Context::new(HashMap::new(), HashMap::new());
        ctx.set_iteration("hello".to_string(), 2);
        assert_eq!(ctx.resolve("{{ item }}", "test").unwrap(), "hello");
        assert_eq!(ctx.resolve("{{ item_index }}", "test").unwrap(), "2");
    }

    #[test]
    fn item_without_iteration_errors() {
        let ctx = Context::new(HashMap::new(), HashMap::new());
        let err = ctx.resolve("{{ item }}", "test").unwrap_err();
        assert!(err.to_string().contains("item"));
    }

    #[test]
    fn resolve_run_status() {
        let mut ctx = Context::new(HashMap::new(), HashMap::new());
        ctx.set_run_status("FAILED");
        assert_eq!(ctx.resolve("{{ _run.status }}", "test").unwrap(), "FAILED");
    }

    #[test]
    fn run_status_unset_errors() {
        let ctx = Context::new(HashMap::new(), HashMap::new());
        let err = ctx.resolve("{{ _run.status }}", "test").unwrap_err();
        assert!(err.to_string().contains("_run.status"));
    }

    #[test]
    fn resolve_headers() {
        let mut ctx = Context::new(HashMap::new(), HashMap::new());
        let mut headers = HashMap::new();
        headers.insert("content-type".to_string(), "application/json".to_string());
        headers.insert(
            "set-cookie".to_string(),
            "token=abc123; sid=xyz".to_string(),
        );
        ctx.add_step_output(
            "auth".to_string(),
            StepOutput {
                stdout: "body".to_string(),
                stderr: String::new(),
                exit_code: Some(0),
                headers,
            },
        );
        assert_eq!(
            ctx.resolve("{{ auth.headers.content-type }}", "test")
                .unwrap(),
            "application/json"
        );
        assert_eq!(
            ctx.resolve("{{ auth.headers.set-cookie }}", "test")
                .unwrap(),
            "token=abc123; sid=xyz"
        );
    }

    #[test]
    fn headers_missing_key_errors() {
        let mut ctx = Context::new(HashMap::new(), HashMap::new());
        ctx.add_step_output(
            "req".to_string(),
            StepOutput {
                stdout: String::new(),
                stderr: String::new(),
                exit_code: Some(0),
                headers: HashMap::new(),
            },
        );
        let err = ctx
            .resolve("{{ req.headers.x-missing }}", "test")
            .unwrap_err();
        assert!(err.to_string().contains("req.headers.x-missing"));
    }
}
