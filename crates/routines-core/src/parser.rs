use serde::{Deserialize, Serialize};

/// A complete Routine definition parsed from YAML.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Routine {
    pub name: String,
    pub description: String,
    #[serde(default)]
    pub inputs: Vec<InputDef>,
    pub steps: Vec<Step>,
    #[serde(default)]
    pub strict_mode: bool,
}

/// Input parameter declaration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputDef {
    pub name: String,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub default: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
}

/// A single execution step.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Step {
    pub id: String,
    #[serde(rename = "type")]
    pub step_type: StepType,
    // --- CLI fields ---
    /// Executable name or path (required for cli, unused for api).
    #[serde(default)]
    pub command: Option<String>,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: std::collections::HashMap<String, String>,
    /// Optional content to pipe into the subprocess stdin.
    #[serde(default)]
    pub stdin: Option<String>,
    /// Working directory for the subprocess. Supports template syntax.
    #[serde(default)]
    pub working_dir: Option<String>,
    // --- API fields ---
    /// HTTP URL (required for api). Supports template syntax.
    #[serde(default)]
    pub url: Option<String>,
    /// HTTP method (default: GET).
    #[serde(default = "default_method")]
    pub method: String,
    /// HTTP request headers. Supports template syntax in values.
    #[serde(default)]
    pub headers: std::collections::HashMap<String, String>,
    /// HTTP request body. Supports template syntax.
    #[serde(default)]
    pub body: Option<String>,
    // --- Common fields ---
    /// Timeout in seconds. Step is killed and marked FAILED on expiry.
    #[serde(default)]
    pub timeout: Option<u64>,
    /// Condition expression. Step is skipped when condition evaluates to false.
    #[serde(default)]
    pub when: Option<String>,
    /// Error strategy when step fails.
    #[serde(default)]
    pub on_fail: OnFail,
}

fn default_method() -> String {
    "GET".to_string()
}

/// Error strategy for a step.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum OnFail {
    /// Stop the entire run on failure (default).
    #[default]
    Stop,
    /// Record failure but continue executing subsequent steps.
    Continue,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum StepType {
    Cli,
    Api,
}

impl Routine {
    /// Parse a Routine from a YAML string.
    pub fn from_yaml(yaml: &str) -> crate::error::Result<Self> {
        let routine: Routine = serde_yaml::from_str(yaml)?;
        routine.validate()?;
        Ok(routine)
    }

    /// Semantic validation after deserialization.
    fn validate(&self) -> crate::error::Result<()> {
        for step in &self.steps {
            match step.step_type {
                StepType::Cli => {
                    if step.command.is_none() {
                        return Err(crate::error::RoutineError::Validation(format!(
                            "step '{}': type=cli requires 'command' field",
                            step.id
                        )));
                    }
                }
                StepType::Api => {
                    if step.url.is_none() {
                        return Err(crate::error::RoutineError::Validation(format!(
                            "step '{}': type=api requires 'url' field",
                            step.id
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    /// Parse a Routine from a YAML file.
    pub fn from_file(path: &std::path::Path) -> crate::error::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        Self::from_yaml(&content)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_deploy_frontend() {
        let yaml = include_str!("../../../docs/fixtures/deploy_frontend.yml");
        let routine = Routine::from_yaml(yaml).unwrap();
        assert_eq!(routine.name, "deploy_frontend");
        assert_eq!(routine.steps.len(), 3);
        assert_eq!(routine.inputs.len(), 2);
        assert!(routine.inputs[0].required);
        assert!(!routine.inputs[1].required);
        assert_eq!(routine.inputs[1].default.as_deref(), Some("latest"));
        // Check env on upload step
        assert!(routine.steps[1].env.contains_key("AWS_ACCESS_KEY_ID"));
    }

    #[test]
    fn parse_summarize_pr() {
        let yaml = include_str!("../../../docs/fixtures/summarize_pr.yml");
        let routine = Routine::from_yaml(yaml).unwrap();
        assert_eq!(routine.name, "summarize_pr");
        assert_eq!(routine.steps.len(), 3);
        // extract_info step should have stdin field
        assert!(routine.steps[1].stdin.is_some());
        assert_eq!(routine.steps[1].command.as_deref(), Some("jq"));
    }
}
