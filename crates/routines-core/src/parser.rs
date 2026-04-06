use std::collections::HashMap;

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

/// A single execution step with type-safe action.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Step {
    pub id: String,
    /// Type-specific action (cli or http), flattened into step fields.
    #[serde(flatten)]
    pub action: StepAction,
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

/// Type-discriminated step action. Determines which fields are required.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum StepAction {
    Cli {
        command: String,
        #[serde(default)]
        args: Vec<String>,
        #[serde(default)]
        env: HashMap<String, String>,
        #[serde(default)]
        stdin: Option<String>,
        #[serde(default)]
        working_dir: Option<String>,
    },
    Http {
        url: String,
        #[serde(default = "default_method")]
        method: String,
        #[serde(default)]
        headers: HashMap<String, String>,
        #[serde(default)]
        body: Option<String>,
    },
    Routine {
        name: String,
        #[serde(default)]
        inputs: HashMap<String, String>,
    },
    Mcp {
        server: String,
        tool: String,
        #[serde(default)]
        arguments: HashMap<String, serde_json::Value>,
    },
}

fn default_method() -> String {
    "GET".to_string()
}

/// Error strategy for a step.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum OnFail {
    #[default]
    Stop,
    Continue,
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
        match &routine.steps[1].action {
            StepAction::Cli { env, .. } => assert!(env.contains_key("AWS_ACCESS_KEY_ID")),
            _ => panic!("expected cli step"),
        }
    }

    #[test]
    fn parse_summarize_pr() {
        let yaml = include_str!("../../../docs/fixtures/summarize_pr.yml");
        let routine = Routine::from_yaml(yaml).unwrap();
        assert_eq!(routine.name, "summarize_pr");
        assert_eq!(routine.steps.len(), 3);
        match &routine.steps[1].action {
            StepAction::Cli { stdin, command, .. } => {
                assert!(stdin.is_some());
                assert_eq!(command, "jq");
            }
            _ => panic!("expected cli step"),
        }
    }

    #[test]
    fn parse_http_step() {
        let routine = Routine::from_yaml(
            r#"
name: http_test
description: test
steps:
  - id: fetch
    type: http
    url: "https://example.com/api"
    method: POST
    headers:
      Authorization: "Bearer token"
    body: '{"key": "value"}'
"#,
        )
        .unwrap();

        match &routine.steps[0].action {
            StepAction::Http {
                url,
                method,
                headers,
                body,
            } => {
                assert_eq!(url, "https://example.com/api");
                assert_eq!(method, "POST");
                assert!(headers.contains_key("Authorization"));
                assert!(body.is_some());
            }
            _ => panic!("expected http step"),
        }
    }

    #[test]
    fn cli_without_command_fails() {
        let result = Routine::from_yaml(
            r#"
name: bad
description: test
steps:
  - id: no_cmd
    type: cli
"#,
        );
        assert!(result.is_err());
    }

    #[test]
    fn parse_mcp_step() {
        let routine = Routine::from_yaml(
            r#"
name: mcp_test
description: test
steps:
  - id: notify
    type: mcp
    server: slack
    tool: send_message
    arguments:
      channel: general
      text: "hello"
      count: 42
"#,
        )
        .unwrap();

        match &routine.steps[0].action {
            StepAction::Mcp {
                server,
                tool,
                arguments,
            } => {
                assert_eq!(server, "slack");
                assert_eq!(tool, "send_message");
                assert_eq!(arguments["channel"], serde_json::json!("general"));
                assert_eq!(arguments["count"], serde_json::json!(42));
            }
            _ => panic!("expected mcp step"),
        }
    }

    #[test]
    fn mcp_without_server_fails() {
        let result = Routine::from_yaml(
            r#"
name: bad
description: test
steps:
  - id: no_srv
    type: mcp
    tool: something
"#,
        );
        assert!(result.is_err());
    }

    #[test]
    fn http_without_url_fails() {
        let result = Routine::from_yaml(
            r#"
name: bad
description: test
steps:
  - id: no_url
    type: http
"#,
        );
        assert!(result.is_err());
    }
}
