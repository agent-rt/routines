use rusqlite::Connection;

use crate::error::Result;
use crate::executor::{RunResult, RunStatus, StepResult, StepStatus};
use crate::secrets;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS routine_runs (
    id          TEXT PRIMARY KEY,
    routine_name TEXT NOT NULL,
    status      TEXT NOT NULL,
    input_vars  TEXT,
    started_at  TEXT NOT NULL,
    ended_at    TEXT
);

CREATE TABLE IF NOT EXISTS step_logs (
    id                TEXT PRIMARY KEY,
    run_id            TEXT NOT NULL REFERENCES routine_runs(id),
    step_id           TEXT NOT NULL,
    status            TEXT NOT NULL,
    exit_code         INTEGER,
    stdout            TEXT,
    stderr            TEXT,
    input_vars        TEXT,
    execution_time_ms INTEGER NOT NULL,
    started_at        TEXT NOT NULL
);
";

/// Audit database backed by SQLite.
pub struct AuditDb {
    conn: Connection,
}

impl AuditDb {
    /// Open (or create) the audit database at the given path.
    pub fn open(db_path: &std::path::Path) -> Result<Self> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(db_path)?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self { conn })
    }

    /// Open an in-memory database (for testing).
    pub fn in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self { conn })
    }

    /// Insert a new routine run record.
    pub fn insert_run(
        &self,
        run_id: &str,
        routine_name: &str,
        input_vars_json: &str,
        started_at: &str,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO routine_runs (id, routine_name, status, input_vars, started_at) VALUES (?1, ?2, 'RUNNING', ?3, ?4)",
            rusqlite::params![run_id, routine_name, input_vars_json, started_at],
        )?;
        Ok(())
    }

    /// Insert a step log record. Secrets in stdout/stderr are redacted.
    pub fn insert_step_log(
        &self,
        run_id: &str,
        step: &StepResult,
        secret_values: &[&str],
        started_at: &str,
    ) -> Result<()> {
        let id = uuid::Uuid::new_v4().to_string();
        let status = match step.status {
            StepStatus::Success => "SUCCESS",
            StepStatus::Failed => "FAILED",
        };
        let stdout = secrets::redact(&step.stdout, secret_values);
        let stderr = secrets::redact(&step.stderr, secret_values);

        self.conn.execute(
            "INSERT INTO step_logs (id, run_id, step_id, status, exit_code, stdout, stderr, input_vars, execution_time_ms, started_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            rusqlite::params![id, run_id, step.step_id, status, step.exit_code, stdout, stderr, "", step.execution_time_ms, started_at],
        )?;
        Ok(())
    }

    /// Finalize a run record with end status and timestamp.
    pub fn finalize_run(&self, run_id: &str, result: &RunResult, ended_at: &str) -> Result<()> {
        let status = match result.status {
            RunStatus::Success => "SUCCESS",
            RunStatus::Failed => "FAILED",
        };
        self.conn.execute(
            "UPDATE routine_runs SET status = ?1, ended_at = ?2 WHERE id = ?3",
            rusqlite::params![status, ended_at, run_id],
        )?;
        Ok(())
    }

    /// Query a run and its step logs by run_id.
    pub fn get_run_log(&self, run_id: &str) -> Result<Option<RunLog>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, routine_name, status, input_vars, started_at, ended_at FROM routine_runs WHERE id = ?1",
        )?;
        let run = stmt
            .query_row(rusqlite::params![run_id], |row| {
                Ok(RunLog {
                    run_id: row.get(0)?,
                    routine_name: row.get(1)?,
                    status: row.get(2)?,
                    input_vars: row.get(3)?,
                    started_at: row.get(4)?,
                    ended_at: row.get(5)?,
                    steps: Vec::new(),
                })
            })
            .ok();

        if let Some(mut run) = run {
            let mut stmt = self.conn.prepare(
                "SELECT step_id, status, exit_code, stdout, stderr, execution_time_ms, started_at FROM step_logs WHERE run_id = ?1 ORDER BY started_at",
            )?;
            let steps = stmt.query_map(rusqlite::params![run_id], |row| {
                Ok(StepLog {
                    step_id: row.get(0)?,
                    status: row.get(1)?,
                    exit_code: row.get(2)?,
                    stdout: row.get(3)?,
                    stderr: row.get(4)?,
                    execution_time_ms: row.get(5)?,
                    started_at: row.get(6)?,
                })
            })?;
            for step in steps {
                run.steps.push(step?);
            }
            Ok(Some(run))
        } else {
            Ok(None)
        }
    }
}

/// A complete run log with all step details.
#[derive(Debug, serde::Serialize)]
pub struct RunLog {
    pub run_id: String,
    pub routine_name: String,
    pub status: String,
    pub input_vars: Option<String>,
    pub started_at: String,
    pub ended_at: Option<String>,
    pub steps: Vec<StepLog>,
}

/// A single step's log entry.
#[derive(Debug, serde::Serialize)]
pub struct StepLog {
    pub step_id: String,
    pub status: String,
    pub exit_code: Option<i32>,
    pub stdout: Option<String>,
    pub stderr: Option<String>,
    pub execution_time_ms: i64,
    pub started_at: String,
}
