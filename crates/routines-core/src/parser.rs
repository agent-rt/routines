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
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: std::collections::HashMap<String, String>,
    /// Optional content to pipe into the subprocess stdin.
    #[serde(default)]
    pub stdin: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum StepType {
    Cli,
}

impl Routine {
    /// Parse a Routine from a YAML string.
    pub fn from_yaml(yaml: &str) -> crate::error::Result<Self> {
        let routine: Routine = serde_yaml::from_str(yaml)?;
        Ok(routine)
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
        assert_eq!(routine.steps[1].command, "jq");
    }
}
