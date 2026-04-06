use std::collections::{HashMap, HashSet, VecDeque};

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
    /// Step IDs that must complete before this step starts. Enables parallel execution.
    #[serde(default)]
    pub needs: Vec<String>,
    /// Retry configuration. Step is retried on failure before triggering on_fail.
    #[serde(default)]
    pub retry: Option<RetryConfig>,
    /// Iterate this step over a list. Each iteration injects `{{ item }}` and `{{ item_index }}`.
    #[serde(default)]
    pub for_each: Option<ForEach>,
}

/// Source of iteration items for `for_each`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ForEach {
    /// Static list of string values declared inline in YAML.
    List(Vec<String>),
    /// Template expression referencing a previous step's output (e.g. `{{ step.stdout_lines }}`).
    Template(String),
}

/// Retry configuration for a step.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Maximum number of retries (total attempts = count + 1).
    pub count: u32,
    /// Delay in seconds before each retry.
    #[serde(default = "default_delay")]
    pub delay: u64,
    /// Backoff strategy: fixed or exponential.
    #[serde(default)]
    pub backoff: BackoffStrategy,
}

fn default_delay() -> u64 {
    1
}

/// Backoff strategy for retries.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum BackoffStrategy {
    #[default]
    Fixed,
    Exponential,
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
        routine.validate_dag()?;
        Ok(routine)
    }

    /// Parse a Routine from a YAML file.
    pub fn from_file(path: &std::path::Path) -> crate::error::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        Self::from_yaml(&content)
    }

    /// Returns true if any step declares `needs`, enabling DAG parallel mode.
    pub fn has_dag(&self) -> bool {
        self.steps.iter().any(|s| !s.needs.is_empty())
    }

    /// Validate the step dependency graph: no self-refs, all refs exist, no cycles.
    fn validate_dag(&self) -> crate::error::Result<()> {
        let step_ids: HashSet<&str> = self.steps.iter().map(|s| s.id.as_str()).collect();

        for step in &self.steps {
            for dep in &step.needs {
                if dep == &step.id {
                    return Err(crate::error::RoutineError::InvalidNeeds {
                        step_id: step.id.clone(),
                        reason: "step cannot depend on itself".to_string(),
                    });
                }
                if !step_ids.contains(dep.as_str()) {
                    return Err(crate::error::RoutineError::InvalidNeeds {
                        step_id: step.id.clone(),
                        reason: format!("depends on unknown step '{dep}'"),
                    });
                }
            }
        }

        // Topological sort to detect cycles (Kahn's algorithm)
        if self.has_dag() {
            let mut in_degree: HashMap<&str, usize> = HashMap::new();
            let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
            for step in &self.steps {
                in_degree.entry(step.id.as_str()).or_insert(0);
                adj.entry(step.id.as_str()).or_default();
                for dep in &step.needs {
                    adj.entry(dep.as_str()).or_default().push(&step.id);
                    *in_degree.entry(step.id.as_str()).or_insert(0) += 1;
                }
            }

            let mut queue: VecDeque<&str> = in_degree
                .iter()
                .filter(|&(_, &deg)| deg == 0)
                .map(|(&id, _)| id)
                .collect();
            let mut visited = 0usize;

            while let Some(node) = queue.pop_front() {
                visited += 1;
                for &downstream in adj.get(node).unwrap_or(&vec![]) {
                    let deg = in_degree.get_mut(downstream).unwrap();
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push_back(downstream);
                    }
                }
            }

            if visited != self.steps.len() {
                return Err(crate::error::RoutineError::CyclicDependency);
            }
        }

        Ok(())
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
    fn parse_needs_field() {
        let routine = Routine::from_yaml(
            r#"
name: parallel
description: test
steps:
  - id: a
    type: cli
    command: echo
    args: ["a"]
  - id: b
    type: cli
    command: echo
    args: ["b"]
  - id: c
    type: cli
    command: echo
    args: ["c"]
    needs: [a, b]
"#,
        )
        .unwrap();

        assert!(routine.has_dag());
        assert!(routine.steps[0].needs.is_empty());
        assert!(routine.steps[1].needs.is_empty());
        assert_eq!(routine.steps[2].needs, vec!["a", "b"]);
    }

    #[test]
    fn no_needs_is_sequential() {
        let routine = Routine::from_yaml(
            r#"
name: seq
description: test
steps:
  - id: a
    type: cli
    command: echo
    args: ["a"]
  - id: b
    type: cli
    command: echo
    args: ["b"]
"#,
        )
        .unwrap();

        assert!(!routine.has_dag());
    }

    #[test]
    fn needs_self_reference_fails() {
        let result = Routine::from_yaml(
            r#"
name: bad
description: test
steps:
  - id: loop
    type: cli
    command: echo
    needs: [loop]
"#,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("itself"));
    }

    #[test]
    fn needs_unknown_step_fails() {
        let result = Routine::from_yaml(
            r#"
name: bad
description: test
steps:
  - id: a
    type: cli
    command: echo
    needs: [nonexistent]
"#,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unknown step"));
    }

    #[test]
    fn cyclic_dependency_fails() {
        let result = Routine::from_yaml(
            r#"
name: bad
description: test
steps:
  - id: a
    type: cli
    command: echo
    needs: [b]
  - id: b
    type: cli
    command: echo
    needs: [a]
"#,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Cyclic"));
    }

    #[test]
    fn parse_retry_config() {
        let routine = Routine::from_yaml(
            r#"
name: retry_test
description: test
steps:
  - id: flaky
    type: cli
    command: curl
    args: ["-f", "https://example.com"]
    retry:
      count: 3
      delay: 2
      backoff: exponential
"#,
        )
        .unwrap();

        let retry = routine.steps[0].retry.as_ref().unwrap();
        assert_eq!(retry.count, 3);
        assert_eq!(retry.delay, 2);
        assert_eq!(retry.backoff, BackoffStrategy::Exponential);
    }

    #[test]
    fn parse_retry_defaults() {
        let routine = Routine::from_yaml(
            r#"
name: retry_default
description: test
steps:
  - id: flaky
    type: cli
    command: curl
    retry:
      count: 2
"#,
        )
        .unwrap();

        let retry = routine.steps[0].retry.as_ref().unwrap();
        assert_eq!(retry.count, 2);
        assert_eq!(retry.delay, 1);
        assert_eq!(retry.backoff, BackoffStrategy::Fixed);
    }

    #[test]
    fn no_retry_is_none() {
        let routine = Routine::from_yaml(
            r#"
name: no_retry
description: test
steps:
  - id: once
    type: cli
    command: echo
    args: ["hello"]
"#,
        )
        .unwrap();

        assert!(routine.steps[0].retry.is_none());
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

    #[test]
    fn parse_for_each_static_list() {
        let routine = Routine::from_yaml(
            r#"
name: foreach_test
description: test
steps:
  - id: deploy
    type: cli
    command: kubectl
    args: ["apply", "-f", "{{ item }}"]
    for_each:
      - svc-a.yml
      - svc-b.yml
      - svc-c.yml
"#,
        )
        .unwrap();

        match &routine.steps[0].for_each {
            Some(ForEach::List(items)) => {
                assert_eq!(items.len(), 3);
                assert_eq!(items[0], "svc-a.yml");
                assert_eq!(items[2], "svc-c.yml");
            }
            other => panic!("expected ForEach::List, got {other:?}"),
        }
    }

    #[test]
    fn parse_for_each_template() {
        let routine = Routine::from_yaml(
            r#"
name: foreach_template
description: test
steps:
  - id: list
    type: cli
    command: ls
  - id: process
    type: cli
    command: echo
    args: ["{{ item }}"]
    for_each: "{{ list.stdout_lines }}"
"#,
        )
        .unwrap();

        match &routine.steps[1].for_each {
            Some(ForEach::Template(t)) => {
                assert_eq!(t, "{{ list.stdout_lines }}");
            }
            other => panic!("expected ForEach::Template, got {other:?}"),
        }
    }

    #[test]
    fn no_for_each_is_none() {
        let routine = Routine::from_yaml(
            r#"
name: no_foreach
description: test
steps:
  - id: once
    type: cli
    command: echo
    args: ["hello"]
"#,
        )
        .unwrap();

        assert!(routine.steps[0].for_each.is_none());
    }
}
