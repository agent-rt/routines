mod cli;
mod http;
mod mcp;
mod routine;

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, Condvar};

use crate::context::{Context, StepOutput};
use crate::error::{Result, RoutineError};
use crate::parser::{OnFail, Routine, Step, StepAction};

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
pub(crate) fn evaluate_condition(expr: &str) -> bool {
    let trimmed = expr.trim();
    if let Some((left, right)) = trimmed.split_once("==") {
        if left.ends_with('!') {
            let left = left.strip_suffix('!').unwrap().trim();
            return left != right.trim();
        }
        return left.trim() == right.trim();
    }
    if let Some((left, right)) = trimmed.split_once("!=") {
        return left.trim() != right.trim();
    }
    !trimmed.is_empty() && trimmed != "false" && trimmed != "0"
}

fn default_routines_dir() -> PathBuf {
    std::env::var("ROUTINES_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            std::env::var("HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("."))
                .join(".routines")
        })
}

/// Execute a full routine with the given inputs and secrets.
pub fn run_routine(
    routine: &Routine,
    inputs: HashMap<String, String>,
    secrets: HashMap<String, String>,
) -> Result<RunResult> {
    run_routine_with_depth(routine, inputs, secrets, default_routines_dir(), 0)
}

/// Execute a routine with depth tracking for recursion protection.
pub(crate) fn run_routine_with_depth(
    routine: &Routine,
    inputs: HashMap<String, String>,
    secrets: HashMap<String, String>,
    routines_dir: PathBuf,
    depth: u32,
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

    let ctx = Context::new(resolved_inputs, secrets.clone());

    if routine.has_dag() {
        run_dag(routine, ctx, secrets, routines_dir, depth)
    } else {
        run_sequential(routine, ctx, secrets, routines_dir, depth)
    }
}

/// Execute a single step against the given context.
fn execute_step(
    step: &Step,
    routine: &Routine,
    ctx: &Context,
    secrets: &HashMap<String, String>,
    routines_dir: &PathBuf,
    depth: u32,
) -> Result<StepResult> {
    // Evaluate `when` condition
    if let Some(when_expr) = &step.when {
        let resolved = ctx.resolve(when_expr, &step.id)?;
        if !evaluate_condition(&resolved) {
            return Ok(StepResult {
                step_id: step.id.clone(),
                status: StepStatus::Skipped,
                exit_code: None,
                stdout: String::new(),
                stderr: String::new(),
                execution_time_ms: 0,
            });
        }
    }

    match &step.action {
        StepAction::Cli {
            command,
            args,
            env,
            stdin,
            working_dir,
        } => cli::execute(
            &cli::CliParams {
                step_id: &step.id,
                command,
                args,
                env,
                stdin_template: stdin.as_deref(),
                working_dir_template: working_dir.as_deref(),
                timeout: step.timeout,
                strict_mode: routine.strict_mode,
            },
            ctx,
        ),
        StepAction::Http {
            url,
            method,
            headers,
            body,
        } => http::execute(
            &step.id,
            url,
            method,
            headers,
            body.as_deref(),
            step.timeout,
            ctx,
        ),
        StepAction::Routine {
            name,
            inputs: input_templates,
        } => routine::execute(
            &routine::RoutineParams {
                step_id: &step.id,
                name,
                input_templates,
                timeout: step.timeout,
                depth,
                secrets,
                routines_dir,
            },
            ctx,
        ),
        StepAction::Mcp {
            server,
            tool,
            arguments,
        } => mcp::execute(
            &mcp::McpParams {
                step_id: &step.id,
                server,
                tool,
                arguments,
                timeout: step.timeout,
                routines_dir,
                secrets,
            },
            ctx,
        ),
    }
}

/// Sequential execution path (no `needs` declared — original behavior).
fn run_sequential(
    routine: &Routine,
    mut ctx: Context,
    secrets: HashMap<String, String>,
    routines_dir: PathBuf,
    depth: u32,
) -> Result<RunResult> {
    let mut step_results = Vec::new();
    let mut has_failure = false;

    for step in &routine.steps {
        let result = execute_step(step, routine, &ctx, &secrets, &routines_dir, depth)?;

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

/// DAG-based parallel execution (at least one step has `needs`).
fn run_dag(
    routine: &Routine,
    ctx: Context,
    secrets: HashMap<String, String>,
    routines_dir: PathBuf,
    depth: u32,
) -> Result<RunResult> {
    let step_map: HashMap<&str, &Step> = routine.steps.iter().map(|s| (s.id.as_str(), s)).collect();

    // Build in-degree and downstream adjacency
    let mut in_degree: HashMap<&str, usize> = HashMap::new();
    let mut downstream: HashMap<&str, Vec<&str>> = HashMap::new();
    for step in &routine.steps {
        in_degree.entry(step.id.as_str()).or_insert(0);
        downstream.entry(step.id.as_str()).or_default();
        for dep in &step.needs {
            downstream.entry(dep.as_str()).or_default().push(&step.id);
            *in_degree.entry(step.id.as_str()).or_insert(0) += 1;
        }
    }

    // Shared state for DAG scheduling
    let ctx = Arc::new(Mutex::new(ctx));
    let completed: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    let results: Arc<Mutex<HashMap<String, StepResult>>> = Arc::new(Mutex::new(HashMap::new()));
    let in_degree = Arc::new(Mutex::new(in_degree));
    let aborted = Arc::new(Mutex::new(false));
    let notify = Arc::new(Condvar::new());

    // Find initially ready steps (in_degree == 0)
    let mut ready: VecDeque<String> = {
        let deg = in_degree.lock().unwrap();
        deg.iter()
            .filter(|&(_, &d)| d == 0)
            .map(|(&id, _)| id.to_string())
            .collect()
    };

    std::thread::scope(|scope| {
        let mut handles: Vec<std::thread::ScopedJoinHandle<'_, ()>> = Vec::new();

        loop {
            // Check if all steps are done
            {
                let comp = completed.lock().unwrap();
                if comp.len() == routine.steps.len() {
                    break;
                }
            }

            // Check abort
            if *aborted.lock().unwrap() {
                // Wait for running threads to finish
                break;
            }

            // Launch all ready steps
            while let Some(step_id) = ready.pop_front() {
                if *aborted.lock().unwrap() {
                    // Mark remaining ready as skipped
                    let mut res = results.lock().unwrap();
                    let mut comp = completed.lock().unwrap();
                    res.insert(
                        step_id.clone(),
                        StepResult {
                            step_id: step_id.clone(),
                            status: StepStatus::Skipped,
                            exit_code: None,
                            stdout: String::new(),
                            stderr: "Skipped due to upstream failure".to_string(),
                            execution_time_ms: 0,
                        },
                    );
                    comp.insert(step_id);
                    continue;
                }

                let step = *step_map.get(step_id.as_str()).unwrap();
                let ctx = Arc::clone(&ctx);
                let completed = Arc::clone(&completed);
                let results_ref = Arc::clone(&results);
                let aborted = Arc::clone(&aborted);
                let notify = Arc::clone(&notify);
                let secrets = secrets.clone();
                let routines_dir = routines_dir.clone();

                handles.push(scope.spawn(move || {
                    let ctx_snapshot = ctx.lock().unwrap().clone();
                    let result = execute_step(
                        step, routine, &ctx_snapshot, &secrets, &routines_dir, depth,
                    );

                    let step_result = match result {
                        Ok(r) => r,
                        Err(e) => StepResult {
                            step_id: step.id.clone(),
                            status: StepStatus::Failed,
                            exit_code: Some(1),
                            stdout: String::new(),
                            stderr: e.to_string(),
                            execution_time_ms: 0,
                        },
                    };

                    // Write output to context
                    {
                        let mut ctx = ctx.lock().unwrap();
                        ctx.add_step_output(
                            step.id.clone(),
                            StepOutput {
                                stdout: step_result.stdout.trim().to_string(),
                                stderr: step_result.stderr.trim().to_string(),
                                exit_code: step_result.exit_code,
                            },
                        );
                    }

                    // Check failure
                    if step_result.status == StepStatus::Failed
                        && step.on_fail != OnFail::Continue
                    {
                        *aborted.lock().unwrap() = true;
                    }

                    results_ref.lock().unwrap().insert(step.id.clone(), step_result);
                    completed.lock().unwrap().insert(step.id.clone());
                    notify.notify_all();
                }));
            }

            // Wait for any step to complete, then check for newly ready steps
            {
                let mut comp = completed.lock().unwrap();
                let prev_count = comp.len();
                while comp.len() == prev_count && comp.len() < routine.steps.len() {
                    comp = notify.wait(comp).unwrap();
                }
            }

            // Find newly ready steps
            {
                let deg = &mut in_degree.lock().unwrap();
                let comp = completed.lock().unwrap();
                for step in &routine.steps {
                    if comp.contains(&step.id) {
                        // Update downstream in-degrees
                        if let Some(ds) = downstream.get(step.id.as_str()) {
                            for &d in ds {
                                if let Some(count) = deg.get_mut(d)
                                    && *count > 0
                                {
                                    *count -= 1;
                                    if *count == 0 && !comp.contains(d) && !ready.iter().any(|r| r == d) {
                                        ready.push_back(d.to_string());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Wait for all spawned threads
        for h in handles {
            let _ = h.join();
        }
    });

    // Collect results in step order
    let results_map = results.lock().unwrap();
    let mut step_results = Vec::new();
    let mut has_failure = false;
    let was_aborted = *aborted.lock().unwrap();

    for step in &routine.steps {
        if let Some(result) = results_map.get(&step.id) {
            if result.status == StepStatus::Failed {
                has_failure = true;
            }
            step_results.push(result.clone());
        } else {
            // Step never ran (aborted before reaching it)
            step_results.push(StepResult {
                step_id: step.id.clone(),
                status: StepStatus::Skipped,
                exit_code: None,
                stdout: String::new(),
                stderr: "Skipped due to upstream failure".to_string(),
                execution_time_ms: 0,
            });
        }
    }

    Ok(RunResult {
        status: if has_failure || was_aborted {
            RunStatus::Failed
        } else {
            RunStatus::Success
        },
        step_results,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::Routine;

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
    fn routine_step_calls_sub_routine() {
        // Create temp hub with a sub-routine
        let tmp = std::env::temp_dir().join("routines_test_composition");
        let hub = tmp.join("hub");
        std::fs::create_dir_all(&hub).unwrap();
        std::fs::write(
            hub.join("greeter.yml"),
            r#"
name: greeter
description: greet someone
inputs:
  - name: WHO
    required: true
steps:
  - id: greet
    type: cli
    command: echo
    args: ["Hello {{ inputs.WHO }}"]
"#,
        )
        .unwrap();

        // Parent routine calls sub-routine
        let parent = Routine::from_yaml(
            r#"
name: parent
description: test
inputs:
  - name: NAME
    required: true
steps:
  - id: call_greeter
    type: routine
    name: greeter
    inputs:
      WHO: "{{ inputs.NAME }}"
"#,
        )
        .unwrap();

        let mut inputs = HashMap::new();
        inputs.insert("NAME".to_string(), "World".to_string());
        let result =
            run_routine_with_depth(&parent, inputs, HashMap::new(), tmp.clone(), 0).unwrap();
        assert_eq!(result.status, RunStatus::Success);
        assert!(result.step_results[0].stdout.contains("Hello World"));

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn routine_step_not_found() {
        let tmp = std::env::temp_dir().join("routines_test_notfound");
        let hub = tmp.join("hub");
        std::fs::create_dir_all(&hub).unwrap();

        let routine = Routine::from_yaml(
            r#"
name: test
description: test
steps:
  - id: missing
    type: routine
    name: nonexistent
"#,
        )
        .unwrap();

        let result =
            run_routine_with_depth(&routine, HashMap::new(), HashMap::new(), tmp.clone(), 0)
                .unwrap();
        assert_eq!(result.status, RunStatus::Failed);
        assert!(result.step_results[0].stderr.contains("not found"));

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn routine_step_max_depth() {
        let tmp = std::env::temp_dir().join("routines_test_depth");
        let hub = tmp.join("hub");
        std::fs::create_dir_all(&hub).unwrap();
        // Self-recursive routine
        std::fs::write(
            hub.join("recurse.yml"),
            r#"
name: recurse
description: infinite loop
steps:
  - id: loop
    type: routine
    name: recurse
"#,
        )
        .unwrap();

        let routine = Routine::from_yaml(
            r#"
name: start
description: test
steps:
  - id: go
    type: routine
    name: recurse
"#,
        )
        .unwrap();

        let result =
            run_routine_with_depth(&routine, HashMap::new(), HashMap::new(), tmp.clone(), 0)
                .unwrap();
        assert_eq!(result.status, RunStatus::Failed);
        assert!(result.step_results[0].stderr.contains("depth"));

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn parallel_steps_execute_concurrently() {
        // Two independent steps that each sleep 100ms should complete in ~100ms total (not 200ms)
        let routine = Routine::from_yaml(
            r#"
name: parallel_test
description: test parallel execution
steps:
  - id: a
    type: cli
    command: /bin/sh
    args: ["-c", "sleep 0.1 && echo a_done"]
    needs: []
  - id: b
    type: cli
    command: /bin/sh
    args: ["-c", "sleep 0.1 && echo b_done"]
    needs: []
  - id: c
    type: cli
    command: echo
    args: ["{{ a.stdout }} {{ b.stdout }}"]
    needs: [a, b]
"#,
        )
        .unwrap();

        let start = std::time::Instant::now();
        let result = run_routine(&routine, HashMap::new(), HashMap::new()).unwrap();
        let elapsed = start.elapsed().as_millis();

        assert_eq!(result.status, RunStatus::Success);
        assert_eq!(result.step_results.len(), 3);
        assert_eq!(result.step_results[0].status, StepStatus::Success);
        assert_eq!(result.step_results[1].status, StepStatus::Success);
        assert_eq!(result.step_results[2].status, StepStatus::Success);
        // c should see outputs from a and b
        assert!(result.step_results[2].stdout.contains("a_done"));
        assert!(result.step_results[2].stdout.contains("b_done"));
        // Should complete in ~100ms, not 200ms (allow some margin)
        assert!(elapsed < 300, "Expected <300ms but took {elapsed}ms");
    }

    #[test]
    fn dag_on_fail_stop_cancels_downstream() {
        let routine = Routine::from_yaml(
            r#"
name: dag_fail
description: test
steps:
  - id: a
    type: cli
    command: /bin/sh
    args: ["-c", "exit 1"]
    needs: []
  - id: b
    type: cli
    command: echo
    args: ["should not run"]
    needs: [a]
"#,
        )
        .unwrap();

        let result = run_routine(&routine, HashMap::new(), HashMap::new()).unwrap();
        assert_eq!(result.status, RunStatus::Failed);
        assert_eq!(result.step_results[0].status, StepStatus::Failed);
        assert_eq!(result.step_results[1].status, StepStatus::Skipped);
    }

    #[test]
    fn dag_on_fail_continue_runs_downstream() {
        let routine = Routine::from_yaml(
            r#"
name: dag_continue
description: test
steps:
  - id: a
    type: cli
    command: /bin/sh
    args: ["-c", "exit 1"]
    on_fail: continue
    needs: []
  - id: b
    type: cli
    command: echo
    args: ["ran after failure"]
    needs: [a]
"#,
        )
        .unwrap();

        let result = run_routine(&routine, HashMap::new(), HashMap::new()).unwrap();
        assert_eq!(result.status, RunStatus::Failed);
        assert_eq!(result.step_results[0].status, StepStatus::Failed);
        assert_eq!(result.step_results[1].status, StepStatus::Success);
        assert!(result.step_results[1].stdout.contains("ran after failure"));
    }

    #[test]
    fn dag_diamond_dependency() {
        // Diamond: a → b, a → c, b+c → d
        let routine = Routine::from_yaml(
            r#"
name: diamond
description: test
steps:
  - id: a
    type: cli
    command: echo
    args: ["start"]
    needs: []
  - id: b
    type: cli
    command: echo
    args: ["b={{ a.stdout }}"]
    needs: [a]
  - id: c
    type: cli
    command: echo
    args: ["c={{ a.stdout }}"]
    needs: [a]
  - id: d
    type: cli
    command: echo
    args: ["{{ b.stdout }},{{ c.stdout }}"]
    needs: [b, c]
"#,
        )
        .unwrap();

        let result = run_routine(&routine, HashMap::new(), HashMap::new()).unwrap();
        assert_eq!(result.status, RunStatus::Success);
        assert_eq!(result.step_results.len(), 4);
        assert!(result.step_results[3].stdout.contains("b=start"));
        assert!(result.step_results[3].stdout.contains("c=start"));
    }
}
