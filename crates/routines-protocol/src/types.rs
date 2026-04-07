use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use routines_engine::executor::{RunResult, StepResult};

// ---------------------------------------------------------------------------
// Request / Response envelope
// ---------------------------------------------------------------------------

/// All requests a client can send to the daemon over UDS.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Request {
    /// Submit a routine for execution. Returns immediately with run_id.
    Submit {
        routine: String,
        #[serde(default)]
        inputs: HashMap<String, String>,
    },

    /// Subscribe to real-time events for a run.
    Subscribe { run_id: String },

    /// Cancel a running or queued routine.
    Cancel { run_id: String },

    /// Get current status snapshot of a run.
    Status { run_id: String },

    /// List all active (queued + running) runs.
    ListActive,

    /// Transparent pass-through for meta operations (create, validate, etc.).
    Meta {
        action: String,
        #[serde(default)]
        payload: serde_json::Value,
    },

    /// Ping — health check.
    Ping,

    /// Request graceful shutdown.
    Shutdown,
}

/// All responses the daemon can send back.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Response {
    /// Acknowledgement with run_id after Submit.
    Submitted { run_id: String },

    /// A stream event (sent after Subscribe).
    Event(Box<RunEvent>),

    /// Status snapshot of a single run.
    Snapshot(Box<RunSnapshot>),

    /// List of active runs.
    ActiveList { runs: Vec<RunSnapshot> },

    /// Result of a meta operation.
    MetaResult { data: serde_json::Value },

    /// Pong response.
    Pong,

    /// Error response.
    Error { code: u32, message: String },

    /// Stream ended (no more events for this subscription).
    StreamEnd { run_id: String },
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

/// Real-time events emitted during routine execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event")]
pub enum RunEvent {
    /// A step has started executing.
    StepStarted {
        run_id: String,
        step_id: String,
        started_at: DateTime<Utc>,
    },

    /// A step has completed (success, failure, or skip).
    StepCompleted {
        run_id: String,
        step_id: String,
        result: Box<StepResult>,
    },

    /// The entire routine run has completed.
    RunCompleted {
        run_id: String,
        result: Box<RunResult>,
    },

    /// Informational log message.
    Log {
        run_id: String,
        level: LogLevel,
        message: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogLevel {
    Info,
    Warn,
    Error,
}

// ---------------------------------------------------------------------------
// Snapshots
// ---------------------------------------------------------------------------

/// Point-in-time snapshot of a run's state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunSnapshot {
    pub run_id: String,
    pub routine: String,
    pub status: RunState,
    pub submitted_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub ended_at: Option<DateTime<Utc>>,
    pub steps_completed: usize,
    pub steps_total: usize,
    pub current_step: Option<String>,
}

/// High-level state of a run in the daemon.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunState {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
    Interrupted,
}

// ---------------------------------------------------------------------------
// Socket path
// ---------------------------------------------------------------------------

/// Returns the default daemon socket path: `~/.routines/routinesd.sock`
pub fn default_socket_path() -> std::path::PathBuf {
    routines_dir().join("routinesd.sock")
}

/// Returns the default daemon PID file path: `~/.routines/routinesd.pid`
pub fn default_pid_path() -> std::path::PathBuf {
    routines_dir().join("routinesd.pid")
}

/// Returns the routines home directory.
pub fn routines_dir() -> std::path::PathBuf {
    std::env::var("ROUTINES_HOME")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| {
            dirs_home().join(".routines")
        })
}

fn dirs_home() -> std::path::PathBuf {
    std::env::var("HOME")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("/tmp"))
}
