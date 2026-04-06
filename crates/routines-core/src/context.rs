use std::collections::HashMap;

use crate::error::{Result, RoutineError};

/// Holds all resolved values available for template substitution.
pub struct Context {
    inputs: HashMap<String, String>,
    secrets: HashMap<String, String>,
    step_outputs: HashMap<String, StepOutput>,
}

/// Captured output from a completed step.
#[derive(Debug, Clone)]
pub struct StepOutput {
    pub stdout: String,
    pub stderr: String,
}

impl Context {
    pub fn new(inputs: HashMap<String, String>, secrets: HashMap<String, String>) -> Self {
        Self {
            inputs,
            secrets,
            step_outputs: HashMap::new(),
        }
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
            let end = after_open.find("}}").ok_or_else(|| RoutineError::UndefinedVariable {
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

    /// Look up a single template key like `inputs.ENV`, `secrets.KEY`, or `step_id.stdout`.
    fn lookup(&self, key: &str, current_step_id: &str) -> Result<String> {
        let (prefix, suffix) = key.split_once('.').ok_or_else(|| RoutineError::UndefinedVariable {
            step_id: current_step_id.to_string(),
            key: key.to_string(),
        })?;

        match prefix {
            "inputs" => self.inputs.get(suffix).cloned().ok_or_else(|| {
                RoutineError::UndefinedVariable {
                    step_id: current_step_id.to_string(),
                    key: key.to_string(),
                }
            }),
            "secrets" => self.secrets.get(suffix).cloned().ok_or_else(|| {
                RoutineError::UndefinedVariable {
                    step_id: current_step_id.to_string(),
                    key: key.to_string(),
                }
            }),
            _ => {
                // Treat prefix as a step_id
                let output =
                    self.step_outputs
                        .get(prefix)
                        .ok_or(RoutineError::StepNotExecuted(prefix.to_string()))?;
                match suffix {
                    "stdout" => Ok(output.stdout.clone()),
                    "stderr" => Ok(output.stderr.clone()),
                    _ => Err(RoutineError::UndefinedVariable {
                        step_id: current_step_id.to_string(),
                        key: key.to_string(),
                    }),
                }
            }
        }
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
}
