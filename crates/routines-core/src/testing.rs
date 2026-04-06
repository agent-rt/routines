use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{Result, RoutineError};
use crate::executor::{self, RunStatus, StepStatus};
use crate::parser::Routine;
use crate::resolve::resolve_routine_path;
use crate::secrets;

/// Mock response for a step, injected instead of real execution.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MockResponse {
    #[serde(default)]
    pub stdout: Option<String>,
    #[serde(default)]
    pub stderr: Option<String>,
    #[serde(default)]
    pub exit_code: Option<i32>,
}

/// Assertions to verify after a test case runs.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TestAssert {
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub output_contains: Option<String>,
    #[serde(default)]
    pub output_equals: Option<String>,
    #[serde(default)]
    pub step_status: Option<HashMap<String, String>>,
}

/// A single test case with inputs, mocks, and assertions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestCase {
    pub name: String,
    #[serde(default)]
    pub inputs: HashMap<String, String>,
    #[serde(default)]
    pub mocks: HashMap<String, MockResponse>,
    #[serde(default, rename = "assert")]
    pub assertions: TestAssert,
}

/// A test suite targeting a specific routine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestSuite {
    pub routine: String,
    pub tests: Vec<TestCase>,
}

/// Result of running a single test case.
#[derive(Debug)]
pub struct TestResult {
    pub name: String,
    pub passed: bool,
    pub failures: Vec<String>,
}

impl TestSuite {
    pub fn from_file(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(RoutineError::Io)?;
        Self::from_yaml(&content)
    }

    pub fn from_yaml(yaml: &str) -> Result<Self> {
        serde_yaml::from_str(yaml)
            .map_err(RoutineError::YamlParse)
    }
}

/// Run all test cases in a suite.
pub fn run_test_suite(suite: &TestSuite, routines_dir: &Path) -> Vec<TestResult> {
    let yaml_path = resolve_routine_path(&suite.routine, routines_dir);
    let routine = match Routine::from_file(&yaml_path) {
        Ok(r) => r,
        Err(e) => {
            return vec![TestResult {
                name: format!("(load {})", suite.routine),
                passed: false,
                failures: vec![format!("Failed to load routine: {e}")],
            }];
        }
    };

    suite
        .tests
        .iter()
        .map(|case| run_test_case(&routine, case, routines_dir))
        .collect()
}

/// Run a single test case against a routine.
fn run_test_case(routine: &Routine, case: &TestCase, routines_dir: &Path) -> TestResult {
    let secret_map = secrets::load_secrets(&routines_dir.join(".env"));

    let result = executor::run_routine_with_mocks(
        routine,
        case.inputs.clone(),
        secret_map,
        Some(&case.mocks),
    );

    let mut failures = Vec::new();

    match result {
        Ok(run_result) => {
            // Assert: status
            if let Some(expected_status) = &case.assertions.status {
                let actual = match run_result.status {
                    RunStatus::Success => "success",
                    RunStatus::Failed => "failed",
                };
                if actual != expected_status.to_lowercase() {
                    failures.push(format!(
                        "status: expected {expected_status}, got {actual}"
                    ));
                }
            }

            // Assert: output_contains
            if let Some(needle) = &case.assertions.output_contains {
                let output = run_result.output.as_deref().unwrap_or("");
                if !output.contains(needle.as_str()) {
                    failures.push(format!(
                        "output_contains: '{}' not found in output",
                        needle
                    ));
                }
            }

            // Assert: output_equals
            if let Some(expected) = &case.assertions.output_equals {
                let output = run_result.output.as_deref().unwrap_or("").trim();
                if output != expected.trim() {
                    failures.push(format!(
                        "output_equals: expected '{}', got '{}'",
                        expected.trim(),
                        output
                    ));
                }
            }

            // Assert: step_status
            if let Some(step_statuses) = &case.assertions.step_status {
                for (step_id, expected) in step_statuses {
                    let actual = run_result
                        .step_results
                        .iter()
                        .find(|s| &s.step_id == step_id)
                        .map(|s| match s.status {
                            StepStatus::Success => "success",
                            StepStatus::Failed => "failed",
                            StepStatus::Skipped => "skipped",
                        });
                    match actual {
                        Some(a) if a == expected.to_lowercase() => {}
                        Some(a) => {
                            failures.push(format!(
                                "step_status[{step_id}]: expected {expected}, got {a}"
                            ));
                        }
                        None => {
                            failures.push(format!(
                                "step_status[{step_id}]: step not found in results"
                            ));
                        }
                    }
                }
            }
        }
        Err(e) => {
            // If we expected failure, check if that matches
            if case.assertions.status.as_deref() == Some("failed") {
                // Execution error counts as "failed" — but we can't check other asserts
            } else {
                failures.push(format!("execution error: {e}"));
            }
        }
    }

    TestResult {
        name: case.name.clone(),
        passed: failures.is_empty(),
        failures,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_test_suite() {
        let yaml = r#"
routine: greet
tests:
  - name: basic
    inputs:
      who: world
    mocks:
      say_hi:
        stdout: "Hello world"
        exit_code: 0
    assert:
      status: success
      output_contains: "Hello"
"#;
        let suite = TestSuite::from_yaml(yaml).unwrap();
        assert_eq!(suite.routine, "greet");
        assert_eq!(suite.tests.len(), 1);
        assert_eq!(suite.tests[0].name, "basic");
        assert_eq!(suite.tests[0].mocks["say_hi"].stdout, Some("Hello world".into()));
        assert_eq!(suite.tests[0].mocks["say_hi"].exit_code, Some(0));
        assert_eq!(suite.tests[0].assertions.status, Some("success".into()));
        assert_eq!(suite.tests[0].assertions.output_contains, Some("Hello".into()));
    }

    #[test]
    fn parse_test_suite_minimal() {
        let yaml = r#"
routine: test
tests:
  - name: empty
    mocks:
      step1:
        stdout: "ok"
"#;
        let suite = TestSuite::from_yaml(yaml).unwrap();
        assert_eq!(suite.tests[0].inputs.len(), 0);
        assert_eq!(suite.tests[0].assertions.status, None);
    }

    #[test]
    fn parse_step_status_assert() {
        let yaml = r#"
routine: multi
tests:
  - name: check_steps
    mocks:
      a:
        stdout: "1"
      b:
        stdout: "2"
    assert:
      step_status:
        a: success
        b: success
"#;
        let suite = TestSuite::from_yaml(yaml).unwrap();
        let step_status = suite.tests[0].assertions.step_status.as_ref().unwrap();
        assert_eq!(step_status["a"], "success");
        assert_eq!(step_status["b"], "success");
    }

    /// Helper: create a temp dir with a routine YAML file, run test suite, return results.
    fn run_test_with_routine(routine_yaml: &str, test_yaml: &str) -> Vec<TestResult> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp = std::env::temp_dir().join(format!(
            "routines_test_{}_{id}",
            std::process::id()
        ));
        let hub = tmp.join("hub");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&hub).unwrap();

        let suite = TestSuite::from_yaml(test_yaml).unwrap();

        // Write routine file
        let routine_path = hub.join(format!("{}.yml", suite.routine));
        std::fs::write(&routine_path, routine_yaml.trim()).unwrap();

        let results = run_test_suite(&suite, &tmp);

        let _ = std::fs::remove_dir_all(&tmp);
        results
    }

    #[test]
    fn mock_cli_step_pass() {
        let routine = r#"
name: greet
description: Say hello
inputs:
  - name: who
    required: true
steps:
  - id: say_hi
    type: cli
    command: echo
    args: ["Hello {{ inputs.who }}"]
output: "{{ say_hi.stdout }}"
"#;
        let test = r#"
routine: greet
tests:
  - name: basic
    inputs:
      who: world
    mocks:
      say_hi:
        stdout: "Hello world"
        exit_code: 0
    assert:
      status: success
      output_contains: "Hello world"
"#;
        let results = run_test_with_routine(routine, test);
        assert_eq!(results.len(), 1);
        assert!(results[0].passed, "failures: {:?}", results[0].failures);
    }

    #[test]
    fn mock_cli_step_fail_status() {
        let routine = r#"
name: greet
description: Say hello
steps:
  - id: step1
    type: cli
    command: echo
    args: ["hi"]
output: "{{ step1.stdout }}"
"#;
        let test = r#"
routine: greet
tests:
  - name: expect_fail
    mocks:
      step1:
        stdout: ""
        exit_code: 1
    assert:
      status: failed
"#;
        let results = run_test_with_routine(routine, test);
        assert!(results[0].passed, "failures: {:?}", results[0].failures);
    }

    #[test]
    fn mock_with_transform_step() {
        let routine = r#"
name: transform_test
description: Test transform after mock
steps:
  - id: fetch
    type: cli
    command: curl
    args: ["http://example.com"]
  - id: parse
    type: transform
    input: "{{ fetch.stdout }}"
    select: ".items"
    mapping:
      name: ".name"
output: "{{ parse.stdout }}"
"#;
        let test = r#"
routine: transform_test
tests:
  - name: transform_works
    mocks:
      fetch:
        stdout: '{"items":[{"name":"alice"},{"name":"bob"}]}'
        exit_code: 0
    assert:
      status: success
      output_contains: "alice"
"#;
        let results = run_test_with_routine(routine, test);
        assert!(results[0].passed, "failures: {:?}", results[0].failures);
    }

    #[test]
    fn output_equals_assert() {
        let routine = r#"
name: simple
description: simple
steps:
  - id: s1
    type: cli
    command: echo
    args: ["ok"]
output: "{{ s1.stdout }}"
"#;
        let test = r#"
routine: simple
tests:
  - name: exact_match
    mocks:
      s1:
        stdout: "exact_value"
        exit_code: 0
    assert:
      output_equals: "exact_value"
"#;
        let results = run_test_with_routine(routine, test);
        assert!(results[0].passed, "failures: {:?}", results[0].failures);
    }

    #[test]
    fn output_equals_mismatch() {
        let routine = r#"
name: simple
description: simple
steps:
  - id: s1
    type: cli
    command: echo
    args: ["ok"]
output: "{{ s1.stdout }}"
"#;
        let test = r#"
routine: simple
tests:
  - name: mismatch
    mocks:
      s1:
        stdout: "actual"
        exit_code: 0
    assert:
      output_equals: "expected"
"#;
        let results = run_test_with_routine(routine, test);
        assert!(!results[0].passed);
        assert!(results[0].failures[0].contains("output_equals"));
    }

    #[test]
    fn step_status_assert_pass() {
        let routine = r#"
name: multi
description: multi step
steps:
  - id: a
    type: cli
    command: echo
    args: ["1"]
  - id: b
    type: cli
    command: echo
    args: ["2"]
output: "{{ b.stdout }}"
"#;
        let test = r#"
routine: multi
tests:
  - name: both_ok
    mocks:
      a:
        stdout: "1"
        exit_code: 0
      b:
        stdout: "2"
        exit_code: 0
    assert:
      step_status:
        a: success
        b: success
"#;
        let results = run_test_with_routine(routine, test);
        assert!(results[0].passed, "failures: {:?}", results[0].failures);
    }

    #[test]
    fn multiple_test_cases() {
        let routine = r#"
name: simple
description: simple
steps:
  - id: s1
    type: cli
    command: echo
    args: ["ok"]
output: "{{ s1.stdout }}"
"#;
        let test = r#"
routine: simple
tests:
  - name: pass_case
    mocks:
      s1:
        stdout: "hello"
        exit_code: 0
    assert:
      status: success
  - name: fail_case
    mocks:
      s1:
        stdout: ""
        exit_code: 1
    assert:
      status: failed
"#;
        let results = run_test_with_routine(routine, test);
        assert_eq!(results.len(), 2);
        assert!(results[0].passed, "pass_case failed: {:?}", results[0].failures);
        assert!(results[1].passed, "fail_case failed: {:?}", results[1].failures);
    }
}
