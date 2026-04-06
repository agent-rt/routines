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
    /// Cleanup steps that always execute after main steps, regardless of success/failure.
    #[serde(default)]
    pub finally: Vec<Step>,
    /// Output template expression, resolved after all steps complete.
    #[serde(default)]
    pub output: Option<String>,
    /// Output format hint for CLI rendering.
    #[serde(default)]
    pub output_format: OutputFormat,
    /// Secrets injection into CLI subprocess environment variables.
    #[serde(default)]
    pub secrets_env: SecretsEnv,
    /// Maximum execution time for the entire routine in seconds.
    #[serde(default, rename = "timeout")]
    pub routine_timeout: Option<u64>,
}

/// Secrets injection mode for CLI subprocess environment variables.
#[derive(Debug, Clone, Serialize, PartialEq, Default)]
pub enum SecretsEnv {
    /// No automatic injection (default, current behavior).
    #[default]
    None,
    /// Inject all secrets as same-name environment variables.
    Auto,
    /// Inject only the listed secret names.
    List(Vec<String>),
}

impl<'de> serde::Deserialize<'de> for SecretsEnv {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de;

        struct SecretsEnvVisitor;

        impl<'de> de::Visitor<'de> for SecretsEnvVisitor {
            type Value = SecretsEnv;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("'none', 'auto', or a list of secret names")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<Self::Value, E> {
                match v {
                    "none" => Ok(SecretsEnv::None),
                    "auto" => Ok(SecretsEnv::Auto),
                    _ => Err(E::custom(format!("unknown secrets_env value: '{v}', expected 'none' or 'auto'"))),
                }
            }

            fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error> {
                let mut items = Vec::new();
                while let Some(item) = seq.next_element::<String>()? {
                    items.push(item);
                }
                Ok(SecretsEnv::List(items))
            }
        }

        deserializer.deserialize_any(SecretsEnvVisitor)
    }
}

/// Output format for CLI rendering.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum OutputFormat {
    #[default]
    Plain,
    Table,
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
    /// Type constraint for validation before execution.
    #[serde(default, rename = "type")]
    pub input_type: InputType,
    /// Allowed values when input_type is Enum.
    #[serde(default)]
    pub enum_values: Option<Vec<String>>,
}

/// Input parameter type for validation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum InputType {
    #[default]
    String,
    Int,
    Float,
    Bool,
    Date,
    Enum,
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
    /// Max concurrent iterations for for_each. Default None (=1, serial). 0 = unlimited.
    #[serde(default)]
    pub concurrency: Option<u32>,
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
    Transform {
        /// Template expression that resolves to a JSON string.
        input: String,
        /// JSON path to select data from input. If pointing to an array, mapping applies per element.
        #[serde(default)]
        select: Option<String>,
        /// Field mapping: output_key → path + filter pipeline. Uses IndexMap to preserve order.
        #[serde(default)]
        mapping: Option<indexmap::IndexMap<String, String>>,
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
    /// Also checks that finally step IDs don't collide with main step IDs.
    fn validate_dag(&self) -> crate::error::Result<()> {
        let step_ids: HashSet<&str> = self.steps.iter().map(|s| s.id.as_str()).collect();

        // Check finally step IDs don't collide with main step IDs
        for step in &self.finally {
            if step_ids.contains(step.id.as_str()) {
                return Err(crate::error::RoutineError::InvalidNeeds {
                    step_id: step.id.clone(),
                    reason: "finally step ID conflicts with main step ID".to_string(),
                });
            }
        }

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

    #[test]
    fn parse_finally_block() {
        let routine = Routine::from_yaml(
            r#"
name: with_finally
description: test
steps:
  - id: deploy
    type: cli
    command: echo
    args: ["deploying"]
finally:
  - id: cleanup
    type: cli
    command: echo
    args: ["cleaning up"]
  - id: notify
    type: cli
    command: echo
    args: ["done"]
"#,
        )
        .unwrap();

        assert_eq!(routine.steps.len(), 1);
        assert_eq!(routine.finally.len(), 2);
        assert_eq!(routine.finally[0].id, "cleanup");
        assert_eq!(routine.finally[1].id, "notify");
    }

    #[test]
    fn no_finally_is_empty() {
        let routine = Routine::from_yaml(
            r#"
name: no_finally
description: test
steps:
  - id: run
    type: cli
    command: echo
"#,
        )
        .unwrap();

        assert!(routine.finally.is_empty());
    }

    #[test]
    fn finally_id_collision_fails() {
        let result = Routine::from_yaml(
            r#"
name: bad
description: test
steps:
  - id: deploy
    type: cli
    command: echo
finally:
  - id: deploy
    type: cli
    command: echo
"#,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("conflicts"));
    }

    #[test]
    fn parse_output_field() {
        let routine = Routine::from_yaml(
            r#"
name: with_output
description: test
steps:
  - id: run
    type: cli
    command: echo
    args: ["hello"]
output: "{{ run.stdout }}"
"#,
        )
        .unwrap();

        assert_eq!(routine.output.as_deref(), Some("{{ run.stdout }}"));
        assert_eq!(routine.output_format, OutputFormat::Plain);
    }

    #[test]
    fn parse_output_format_table() {
        let routine = Routine::from_yaml(
            r#"
name: with_table
description: test
steps:
  - id: run
    type: cli
    command: echo
output: "{{ run.stdout }}"
output_format: table
"#,
        )
        .unwrap();

        assert_eq!(routine.output_format, OutputFormat::Table);
    }

    #[test]
    fn parse_transform_step() {
        let routine = Routine::from_yaml(
            r#"
name: transform_test
description: test
steps:
  - id: extract
    type: transform
    input: "{{ search.stdout }}"
    select: ".data.items"
    mapping:
      name: ".name"
      price: ".price | to_int"
"#,
        )
        .unwrap();

        match &routine.steps[0].action {
            StepAction::Transform {
                input,
                select,
                mapping,
            } => {
                assert_eq!(input, "{{ search.stdout }}");
                assert_eq!(select.as_deref(), Some(".data.items"));
                let m = mapping.as_ref().unwrap();
                assert_eq!(m.len(), 2);
                assert_eq!(m["name"], ".name");
                assert_eq!(m["price"], ".price | to_int");
                // Verify order preserved
                let keys: Vec<&String> = m.keys().collect();
                assert_eq!(keys, vec!["name", "price"]);
            }
            other => panic!("expected Transform, got {other:?}"),
        }
    }

    #[test]
    fn parse_transform_no_mapping() {
        let routine = Routine::from_yaml(
            r#"
name: transform_select_only
description: test
steps:
  - id: extract
    type: transform
    input: "{{ search.stdout }}"
    select: ".data.items"
"#,
        )
        .unwrap();

        match &routine.steps[0].action {
            StepAction::Transform {
                select, mapping, ..
            } => {
                assert_eq!(select.as_deref(), Some(".data.items"));
                assert!(mapping.is_none());
            }
            other => panic!("expected Transform, got {other:?}"),
        }
    }

    #[test]
    fn parse_transform_no_select() {
        let routine = Routine::from_yaml(
            r#"
name: transform_identity
description: test
steps:
  - id: pass
    type: transform
    input: "{{ prev.stdout }}"
"#,
        )
        .unwrap();

        match &routine.steps[0].action {
            StepAction::Transform {
                select, mapping, ..
            } => {
                assert!(select.is_none());
                assert!(mapping.is_none());
            }
            other => panic!("expected Transform, got {other:?}"),
        }
    }

    #[test]
    fn transform_missing_input_fails() {
        let result = Routine::from_yaml(
            r#"
name: bad
description: test
steps:
  - id: no_input
    type: transform
    select: ".data"
"#,
        );
        assert!(result.is_err());
    }

    #[test]
    fn no_output_is_none() {
        let routine = Routine::from_yaml(
            r#"
name: no_output
description: test
steps:
  - id: run
    type: cli
    command: echo
"#,
        )
        .unwrap();

        assert!(routine.output.is_none());
        assert_eq!(routine.output_format, OutputFormat::Plain);
    }

    #[test]
    fn parse_secrets_env_auto() {
        let routine = Routine::from_yaml(
            r#"
name: env_test
description: test
secrets_env: auto
steps:
  - id: run
    type: cli
    command: echo
"#,
        )
        .unwrap();

        assert_eq!(routine.secrets_env, SecretsEnv::Auto);
    }

    #[test]
    fn parse_secrets_env_list() {
        let routine = Routine::from_yaml(
            r#"
name: env_test
description: test
secrets_env:
  - AWS_ACCESS_KEY_ID
  - AWS_SECRET_ACCESS_KEY
steps:
  - id: run
    type: cli
    command: echo
"#,
        )
        .unwrap();

        match &routine.secrets_env {
            SecretsEnv::List(items) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0], "AWS_ACCESS_KEY_ID");
                assert_eq!(items[1], "AWS_SECRET_ACCESS_KEY");
            }
            other => panic!("expected SecretsEnv::List, got {other:?}"),
        }
    }

    #[test]
    fn parse_input_types() {
        let routine = Routine::from_yaml(
            r#"
name: typed
description: test
inputs:
  - name: COUNT
    type: int
  - name: RATE
    type: float
  - name: VERBOSE
    type: bool
  - name: DATE
    type: date
  - name: NAME
    type: string
steps:
  - id: run
    type: cli
    command: echo
"#,
        )
        .unwrap();

        assert_eq!(routine.inputs[0].input_type, InputType::Int);
        assert_eq!(routine.inputs[1].input_type, InputType::Float);
        assert_eq!(routine.inputs[2].input_type, InputType::Bool);
        assert_eq!(routine.inputs[3].input_type, InputType::Date);
        assert_eq!(routine.inputs[4].input_type, InputType::String);
    }

    #[test]
    fn parse_input_enum() {
        let routine = Routine::from_yaml(
            r#"
name: enum_test
description: test
inputs:
  - name: SORT
    type: enum
    enum_values: ["1", "2", "3"]
    default: "3"
steps:
  - id: run
    type: cli
    command: echo
"#,
        )
        .unwrap();

        assert_eq!(routine.inputs[0].input_type, InputType::Enum);
        assert_eq!(
            routine.inputs[0].enum_values.as_deref(),
            Some(vec!["1".to_string(), "2".to_string(), "3".to_string()]).as_deref()
        );
    }

    #[test]
    fn no_type_defaults_to_string() {
        let routine = Routine::from_yaml(
            r#"
name: no_type
description: test
inputs:
  - name: FOO
    required: true
steps:
  - id: run
    type: cli
    command: echo
"#,
        )
        .unwrap();

        assert_eq!(routine.inputs[0].input_type, InputType::String);
        assert!(routine.inputs[0].enum_values.is_none());
    }

    #[test]
    fn parse_routine_timeout() {
        let routine = Routine::from_yaml(
            r#"
name: timed
description: test
timeout: 300
steps:
  - id: run
    type: cli
    command: echo
"#,
        )
        .unwrap();

        assert_eq!(routine.routine_timeout, Some(300));
    }

    #[test]
    fn no_routine_timeout_is_none() {
        let routine = Routine::from_yaml(
            r#"
name: untimed
description: test
steps:
  - id: run
    type: cli
    command: echo
"#,
        )
        .unwrap();

        assert!(routine.routine_timeout.is_none());
    }

    #[test]
    fn no_secrets_env_is_none() {
        let routine = Routine::from_yaml(
            r#"
name: no_env
description: test
steps:
  - id: run
    type: cli
    command: echo
"#,
        )
        .unwrap();

        assert_eq!(routine.secrets_env, SecretsEnv::None);
    }
}
