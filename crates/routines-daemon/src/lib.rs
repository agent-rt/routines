use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::Utc;
use tokio::net::UnixListener;
use tokio::sync::{broadcast, Mutex, RwLock};

use routines_engine::parser::Routine;
use routines_engine::resolve::resolve_routine_path;
use routines_protocol::codec;
use routines_protocol::types::*;

pub mod runner;

// ---------------------------------------------------------------------------
// Daemon state
// ---------------------------------------------------------------------------

/// Shared daemon state, accessible from all connection handlers.
pub struct DaemonState {
    /// Active runs keyed by run_id.
    pub runs: RwLock<HashMap<String, RunEntry>>,
    /// Event bus: subscribers get cloned receivers.
    pub event_tx: broadcast::Sender<RunEvent>,
    /// Routines home directory.
    pub routines_dir: PathBuf,
}

/// An entry in the run table.
pub struct RunEntry {
    pub snapshot: RunSnapshot,
    /// Handle to cancel the run.
    pub cancel_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl DaemonState {
    fn new(routines_dir: PathBuf) -> Self {
        let (event_tx, _) = broadcast::channel(1024);
        Self {
            runs: RwLock::new(HashMap::new()),
            event_tx,
            routines_dir,
        }
    }
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Start the daemon server, listening on the given Unix socket path.
/// This function runs until the process is terminated.
pub async fn start_server(sock_path: PathBuf) {
    let routines_dir = sock_path
        .parent()
        .unwrap_or(std::path::Path::new("/tmp"))
        .to_path_buf();

    // Ensure directory exists
    std::fs::create_dir_all(&routines_dir).ok();

    // Clean stale socket
    if sock_path.exists() {
        std::fs::remove_file(&sock_path).ok();
    }

    // Write PID file
    let pid_path = routines_dir.join("routinesd.pid");
    std::fs::write(&pid_path, std::process::id().to_string()).ok();

    let state = Arc::new(DaemonState::new(routines_dir));
    let listener = UnixListener::bind(&sock_path).expect("failed to bind UDS");

    eprintln!("routinesd listening on {}", sock_path.display());

    // Graceful shutdown on SIGTERM/SIGINT
    let state_shutdown = Arc::clone(&state);
    let sock_path_clone = sock_path.clone();
    let pid_path_clone = pid_path.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        eprintln!("routinesd shutting down...");
        // Cleanup
        std::fs::remove_file(&sock_path_clone).ok();
        std::fs::remove_file(&pid_path_clone).ok();
        // Wait briefly for active runs
        let runs = state_shutdown.runs.read().await;
        if !runs.is_empty() {
            eprintln!(
                "waiting for {} active run(s) to finish...",
                runs.len()
            );
            drop(runs);
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
        std::process::exit(0);
    });

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let state = Arc::clone(&state);
                tokio::spawn(handle_connection(stream, state));
            }
            Err(e) => {
                eprintln!("accept error: {e}");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Connection handler
// ---------------------------------------------------------------------------

async fn handle_connection(stream: tokio::net::UnixStream, state: Arc<DaemonState>) {
    let (mut reader, writer) = stream.into_split();
    let writer = Arc::new(Mutex::new(writer));

    loop {
        let req: Request = match codec::read_message(&mut reader).await {
            Ok(req) => req,
            Err(codec::CodecError::ConnectionClosed) => break,
            Err(e) => {
                eprintln!("read error: {e}");
                break;
            }
        };

        let response = handle_request(req, &state, Arc::clone(&writer)).await;
        let mut w = writer.lock().await;
        if let Err(e) = codec::write_message(&mut *w, &response).await {
            eprintln!("write error: {e}");
            break;
        }
    }
}

async fn handle_request(
    req: Request,
    state: &Arc<DaemonState>,
    writer: Arc<Mutex<tokio::net::unix::OwnedWriteHalf>>,
) -> Response {
    match req {
        Request::Ping => Response::Pong,

        Request::Submit { routine, inputs } => {
            handle_submit(&routine, inputs, state).await
        }

        Request::Subscribe { run_id } => {
            // Spawn a task to stream events to this client
            let mut rx = state.event_tx.subscribe();
            let run_id_clone = run_id.clone();
            tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(event) => {
                            let event_run_id = match &event {
                                RunEvent::StepStarted { run_id, .. } => run_id,
                                RunEvent::StepCompleted { run_id, .. } => run_id,
                                RunEvent::RunCompleted { run_id, .. } => run_id,
                                RunEvent::Log { run_id, .. } => run_id,
                            };
                            if *event_run_id != run_id_clone {
                                continue;
                            }
                            let is_completed = matches!(&event, RunEvent::RunCompleted { .. });
                            let mut w = writer.lock().await;
                            if codec::write_message(&mut *w, &Response::Event(Box::new(event)))
                                .await
                                .is_err()
                            {
                                break;
                            }
                            if is_completed {
                                let _ = codec::write_message(
                                    &mut *w,
                                    &Response::StreamEnd {
                                        run_id: run_id_clone.clone(),
                                    },
                                )
                                .await;
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            eprintln!("subscriber lagged by {n} events");
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            });
            // Immediately respond with acknowledgement
            Response::Submitted {
                run_id: run_id.clone(),
            }
        }

        Request::Cancel { run_id } => {
            let mut runs = state.runs.write().await;
            if let Some(entry) = runs.get_mut(&run_id) {
                if let Some(cancel_tx) = entry.cancel_tx.take() {
                    cancel_tx.send(()).ok();
                    entry.snapshot.status = RunState::Cancelled;
                }
                Response::Snapshot(Box::new(entry.snapshot.clone()))
            } else {
                Response::Error {
                    code: 404,
                    message: format!("run {run_id} not found"),
                }
            }
        }

        Request::Status { run_id } => {
            let runs = state.runs.read().await;
            if let Some(entry) = runs.get(&run_id) {
                Response::Snapshot(Box::new(entry.snapshot.clone()))
            } else {
                Response::Error {
                    code: 404,
                    message: format!("run {run_id} not found"),
                }
            }
        }

        Request::ListActive => {
            let runs = state.runs.read().await;
            let active: Vec<RunSnapshot> = runs
                .values()
                .filter(|e| {
                    matches!(
                        e.snapshot.status,
                        RunState::Queued | RunState::Running
                    )
                })
                .map(|e| e.snapshot.clone())
                .collect();
            Response::ActiveList { runs: active }
        }

        Request::Meta { action, payload } => {
            handle_meta(&action, payload, state).await
        }

        Request::Shutdown => {
            eprintln!("shutdown requested via protocol");
            tokio::spawn(async {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                std::process::exit(0);
            });
            Response::Pong
        }
    }
}

// ---------------------------------------------------------------------------
// Submit handler
// ---------------------------------------------------------------------------

async fn handle_submit(
    routine_name: &str,
    inputs: HashMap<String, String>,
    state: &Arc<DaemonState>,
) -> Response {
    let run_id = uuid::Uuid::new_v4().to_string();
    let now = Utc::now();

    // Resolve routine file
    let routine_path = resolve_routine_path(routine_name, &state.routines_dir);
    if !routine_path.exists() {
        return Response::Error {
            code: 404,
            message: format!("routine '{routine_name}' not found at {}", routine_path.display()),
        };
    }

    let yaml = match std::fs::read_to_string(&routine_path) {
        Ok(y) => y,
        Err(e) => {
            return Response::Error {
                code: 400,
                message: format!("cannot read routine file: {e}"),
            };
        }
    };

    let routine: Routine = match serde_yaml::from_str(&yaml) {
        Ok(r) => r,
        Err(e) => {
            return Response::Error {
                code: 400,
                message: format!("YAML parse error: {e}"),
            };
        }
    };

    let steps_total = routine.steps.len();

    // Register run
    let snapshot = RunSnapshot {
        run_id: run_id.clone(),
        routine: routine_name.to_string(),
        status: RunState::Queued,
        submitted_at: now,
        started_at: None,
        ended_at: None,
        steps_completed: 0,
        steps_total,
        current_step: None,
    };

    let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();

    {
        let mut runs = state.runs.write().await;
        runs.insert(
            run_id.clone(),
            RunEntry {
                snapshot: snapshot.clone(),
                cancel_tx: Some(cancel_tx),
            },
        );
    }

    // Spawn execution
    let state_clone = Arc::clone(state);
    let run_id_clone = run_id.clone();
    tokio::spawn(async move {
        runner::execute_run(
            run_id_clone,
            routine,
            inputs,
            cancel_rx,
            state_clone,
        )
        .await;
    });

    Response::Submitted { run_id }
}

// ---------------------------------------------------------------------------
// Meta handler (pass-through for non-execution operations)
// ---------------------------------------------------------------------------

async fn handle_meta(
    action: &str,
    payload: serde_json::Value,
    state: &Arc<DaemonState>,
) -> Response {
    // Meta operations that don't need daemon state — delegate to engine
    match action {
        "list" => {
            let routines = list_routines(&state.routines_dir);
            Response::MetaResult {
                data: serde_json::to_value(routines).unwrap_or_default(),
            }
        }
        "validate" => {
            let yaml = payload
                .get("yaml")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let result: Result<Routine, _> = serde_yaml::from_str(yaml);
            match result {
                Ok(_) => Response::MetaResult {
                    data: serde_json::json!({"valid": true}),
                },
                Err(e) => Response::MetaResult {
                    data: serde_json::json!({"valid": false, "error": e.to_string()}),
                },
            }
        }
        _ => Response::Error {
            code: 400,
            message: format!("unknown meta action: {action}"),
        },
    }
}

fn list_routines(routines_dir: &std::path::Path) -> Vec<String> {
    let hub = routines_dir.join("hub");
    let mut names = Vec::new();
    if let Ok(entries) = walkdir(&hub, "") {
        names.extend(entries);
    }
    names
}

fn walkdir(dir: &std::path::Path, prefix: &str) -> std::io::Result<Vec<String>> {
    let mut results = Vec::new();
    if !dir.is_dir() {
        return Ok(results);
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();
        if path.is_dir() {
            let ns = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{prefix}:{name}")
            };
            results.extend(walkdir(&path, &ns)?);
        } else if name.ends_with(".yml") || name.ends_with(".yaml") {
            let stem = name.trim_end_matches(".yml").trim_end_matches(".yaml");
            let full = if prefix.is_empty() {
                stem.to_string()
            } else {
                format!("{prefix}:{stem}")
            };
            results.push(full);
        }
    }
    Ok(results)
}
