//! Run manager: executes routines in background threads, emitting events.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;

use routines_engine::executor::{self, RunResult, RunStatus};
use routines_engine::parser::{Routine, SecretsEnv};
use routines_engine::secrets;
use routines_protocol::types::*;

use crate::DaemonState;

/// Execute a routine run in the background. Updates state and emits events.
pub async fn execute_run(
    run_id: String,
    routine: Routine,
    inputs: HashMap<String, String>,
    _cancel_rx: tokio::sync::oneshot::Receiver<()>,
    state: Arc<DaemonState>,
) {
    // Mark as running
    {
        let mut runs = state.runs.write().await;
        if let Some(entry) = runs.get_mut(&run_id) {
            entry.snapshot.status = RunState::Running;
            entry.snapshot.started_at = Some(Utc::now());
        }
    }

    // Load secrets
    let secret_map = load_secrets(&state.routines_dir, &routine);

    // Execute in a blocking thread (engine is synchronous)
    let event_tx = state.event_tx.clone();

    let result = tokio::task::spawn_blocking(move || {
        executor::run_routine(&routine, inputs, secret_map)
    })
    .await;

    let run_result = match result {
        Ok(Ok(run_result)) => run_result,
        Ok(Err(e)) => {
            // Engine error — create a failed RunResult
            let err_result = RunResult {
                status: RunStatus::Failed,
                step_results: Vec::new(),
                output: Some(format!("Engine error: {e}")),
                output_config: None,
            };

            emit_run_completed(&run_id, &err_result, &state, &event_tx).await;
            return;
        }
        Err(e) => {
            // Task join error (panic, cancellation)
            let err_result = RunResult {
                status: RunStatus::Failed,
                step_results: Vec::new(),
                output: Some(format!("Execution panicked: {e}")),
                output_config: None,
            };

            emit_run_completed(&run_id, &err_result, &state, &event_tx).await;
            return;
        }
    };

    // Emit step events (retroactively — engine runs synchronously)
    for step in &run_result.step_results {
        let _ = event_tx.send(RunEvent::StepCompleted {
            run_id: run_id.clone(),
            step_id: step.step_id.clone(),
            result: Box::new(step.clone()),
        });

        // Update snapshot progress
        let mut runs = state.runs.write().await;
        if let Some(entry) = runs.get_mut(&run_id) {
            entry.snapshot.steps_completed += 1;
        }
    }

    emit_run_completed(&run_id, &run_result, &state, &event_tx).await;
}

async fn emit_run_completed(
    run_id: &str,
    result: &RunResult,
    state: &Arc<DaemonState>,
    event_tx: &tokio::sync::broadcast::Sender<RunEvent>,
) {
    // Update state
    {
        let mut runs = state.runs.write().await;
        if let Some(entry) = runs.get_mut(run_id) {
            entry.snapshot.status = match result.status {
                RunStatus::Success => RunState::Completed,
                RunStatus::Failed => RunState::Failed,
            };
            entry.snapshot.ended_at = Some(Utc::now());
            entry.cancel_tx = None;
        }
    }

    // Emit completion event
    let _ = event_tx.send(RunEvent::RunCompleted {
        run_id: run_id.to_string(),
        result: Box::new(result.clone()),
    });
}

fn load_secrets(
    routines_dir: &std::path::Path,
    routine: &Routine,
) -> HashMap<String, String> {
    let env_path = routines_dir.join(".env");
    let all_secrets = secrets::load_secrets(&env_path);

    match &routine.secrets_env {
        SecretsEnv::None => HashMap::new(),
        SecretsEnv::Auto => all_secrets,
        SecretsEnv::List(names) => {
            all_secrets
                .into_iter()
                .filter(|(k, _)| names.contains(k))
                .collect()
        }
    }
}
