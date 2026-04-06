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
}

impl Context {
    pub fn new(inputs: HashMap<String, String>, secrets: HashMap<String, String>) -> Self {
        Self {
            inputs,
            secrets,
            step_outputs: HashMap::new(),
            iteration: None,
        }
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
                        let lines: Vec<&str> = output
                            .stdout
                            .lines()
                            .filter(|l| !l.is_empty())
                            .collect();
                        Ok(serde_json::to_string(&lines).unwrap_or_else(|_| "[]".to_string()))
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
}
