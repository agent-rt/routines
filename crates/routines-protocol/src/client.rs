//! Daemon client: connects to routinesd over Unix Domain Socket.
//!
//! Auto-starts the daemon if it's not running.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use tokio::net::UnixStream;

use crate::codec::{self, CodecError};
use crate::types::{Request, Response, RunEvent, RunSnapshot};

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("Codec error: {0}")]
    Codec(#[from] CodecError),

    #[error("Daemon not running and auto-start failed: {0}")]
    DaemonStartFailed(String),

    #[error("Daemon returned error {code}: {message}")]
    DaemonError { code: u32, message: String },

    #[error("Unexpected response from daemon")]
    UnexpectedResponse,

    #[error("Connection failed after {attempts} attempts")]
    ConnectionFailed { attempts: u32 },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub struct DaemonClient {
    stream: UnixStream,
}

impl DaemonClient {
    /// Connect to the daemon. If it's not running, attempt to start it.
    pub async fn connect() -> Result<Self, ClientError> {
        let sock = crate::types::default_socket_path();
        Self::connect_at(&sock).await
    }

    /// Connect to a specific socket path. Auto-starts daemon if needed.
    pub async fn connect_at(sock_path: &Path) -> Result<Self, ClientError> {
        // Try connecting directly first
        if let Ok(stream) = UnixStream::connect(sock_path).await {
            return Ok(Self { stream });
        }

        // Daemon not running — try to start it
        Self::auto_start_daemon(sock_path)?;

        // Retry connection with backoff
        let mut attempts = 0u32;
        let max_attempts = 10;
        loop {
            tokio::time::sleep(Duration::from_millis(100 * (attempts as u64 + 1))).await;
            match UnixStream::connect(sock_path).await {
                Ok(stream) => return Ok(Self { stream }),
                Err(_) if attempts < max_attempts => {
                    attempts += 1;
                    continue;
                }
                Err(_) => {
                    return Err(ClientError::ConnectionFailed {
                        attempts: max_attempts,
                    });
                }
            }
        }
    }

    /// Connect without auto-starting the daemon. Returns None if not running.
    pub async fn try_connect() -> Result<Option<Self>, ClientError> {
        let sock = crate::types::default_socket_path();
        Self::try_connect_at(&sock).await
    }

    /// Try connecting to a specific path without auto-start.
    pub async fn try_connect_at(sock_path: &Path) -> Result<Option<Self>, ClientError> {
        match UnixStream::connect(sock_path).await {
            Ok(stream) => Ok(Some(Self { stream })),
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionRefused
                || e.kind() == std::io::ErrorKind::NotFound =>
            {
                Ok(None)
            }
            Err(e) => Err(ClientError::Io(e)),
        }
    }

    /// Send a request and receive a single response.
    pub async fn request(&mut self, req: &Request) -> Result<Response, ClientError> {
        let (mut reader, mut writer) = self.stream.split();
        codec::write_message(&mut writer, req).await?;
        let resp: Response = codec::read_message(&mut reader).await?;
        if let Response::Error { code, message } = &resp {
            return Err(ClientError::DaemonError {
                code: *code,
                message: message.clone(),
            });
        }
        Ok(resp)
    }

    /// Submit a routine and return the run_id.
    pub async fn submit(
        &mut self,
        routine: &str,
        inputs: std::collections::HashMap<String, String>,
    ) -> Result<String, ClientError> {
        let resp = self
            .request(&Request::Submit {
                routine: routine.to_string(),
                inputs,
            })
            .await?;
        match resp {
            Response::Submitted { run_id } => Ok(run_id),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Subscribe to events for a run. Calls the handler for each event
    /// until RunCompleted or StreamEnd is received.
    pub async fn subscribe<F>(
        &mut self,
        run_id: &str,
        mut on_event: F,
    ) -> Result<(), ClientError>
    where
        F: FnMut(&RunEvent),
    {
        let (mut reader, mut writer) = self.stream.split();
        codec::write_message(
            &mut writer,
            &Request::Subscribe {
                run_id: run_id.to_string(),
            },
        )
        .await?;

        loop {
            let resp: Response = codec::read_message(&mut reader).await?;
            match resp {
                Response::Event(event) => {
                    let is_completed = matches!(&*event, RunEvent::RunCompleted { .. });
                    on_event(&event);
                    if is_completed {
                        break;
                    }
                }
                Response::StreamEnd { .. } => break,
                Response::Error { code, message } => {
                    return Err(ClientError::DaemonError { code, message });
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Get status snapshot of a run.
    pub async fn status(&mut self, run_id: &str) -> Result<RunSnapshot, ClientError> {
        let resp = self
            .request(&Request::Status {
                run_id: run_id.to_string(),
            })
            .await?;
        match resp {
            Response::Snapshot(snap) => Ok(*snap),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// List all active runs.
    pub async fn list_active(&mut self) -> Result<Vec<RunSnapshot>, ClientError> {
        let resp = self.request(&Request::ListActive).await?;
        match resp {
            Response::ActiveList { runs } => Ok(runs),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Cancel a run.
    pub async fn cancel(&mut self, run_id: &str) -> Result<(), ClientError> {
        self.request(&Request::Cancel {
            run_id: run_id.to_string(),
        })
        .await?;
        Ok(())
    }

    /// Ping the daemon.
    pub async fn ping(&mut self) -> Result<(), ClientError> {
        let resp = self.request(&Request::Ping).await?;
        match resp {
            Response::Pong => Ok(()),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Auto-start the daemon by spawning `routinesd` as a detached process.
    fn auto_start_daemon(sock_path: &Path) -> Result<(), ClientError> {
        // Determine the daemon binary path.
        // Try sibling of current exe first, then PATH.
        let daemon_bin = find_daemon_binary();

        let routines_dir = sock_path
            .parent()
            .unwrap_or(Path::new("/tmp"));
        std::fs::create_dir_all(routines_dir).ok();

        // Clean up stale socket file
        if sock_path.exists() {
            std::fs::remove_file(sock_path).ok();
        }

        let child = Command::new(&daemon_bin)
            .arg("--socket")
            .arg(sock_path)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            // Detach: on Unix this creates a new process group
            .spawn();

        match child {
            Ok(_) => Ok(()),
            Err(e) => Err(ClientError::DaemonStartFailed(format!(
                "failed to spawn {}: {e}",
                daemon_bin.display()
            ))),
        }
    }
}

/// Find the routinesd binary: first try next to current exe, then fall back to PATH.
fn find_daemon_binary() -> PathBuf {
    if let Ok(exe) = std::env::current_exe() {
        let sibling = exe.with_file_name("routinesd");
        if sibling.exists() {
            return sibling;
        }
    }
    PathBuf::from("routinesd")
}
