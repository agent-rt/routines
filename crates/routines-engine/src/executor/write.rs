use std::collections::HashMap;
use std::path::Path;

use crate::context::Context;
use crate::error::Result;
use crate::parser::WriteMode;

use super::{Diagnostic, DiagnosticType, StepResult, StepStatus};

/// Sensitive paths blocked in strict_mode.
const SENSITIVE_PREFIXES: &[&str] = &["/etc", "/usr", "/bin", "/sbin", "/var"];
const SENSITIVE_HOME_DIRS: &[&str] = &[".ssh", ".gnupg", ".aws/credentials"];

pub(super) fn execute(
    step_id: &str,
    path_template: &str,
    content_template: &str,
    mode: &WriteMode,
    strict_mode: bool,
    ctx: &Context,
) -> Result<StepResult> {
    let resolved_path = ctx.resolve(path_template, step_id)?;
    let resolved_content = ctx.resolve(content_template, step_id)?;

    let start = std::time::Instant::now();

    // Strict mode: block sensitive paths
    if strict_mode {
        let path = Path::new(&resolved_path);
        let canonical = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
        let path_str = canonical.to_string_lossy();

        for prefix in SENSITIVE_PREFIXES {
            if path_str.starts_with(prefix) {
                return Ok(StepResult {
                    step_id: step_id.to_string(),
                    status: StepStatus::Failed,
                    exit_code: Some(1),
                    stdout: String::new(),
                    stderr: format!(
                        "write blocked in strict_mode: path '{resolved_path}' is under {prefix}"
                    ),
                    execution_time_ms: start.elapsed().as_millis() as u64,
                    diagnostic: Some(Diagnostic {
                        step_id: step_id.to_string(),
                        error_type: DiagnosticType::CliNotFound,
                        status_code: None,
                        resolved_url: None,
                        suggestion: "disable strict_mode or use a safe output path".to_string(),
                        fix_hint: None,
                    }),
                    headers: HashMap::new(),
                });
            }
        }

        // Check home-relative sensitive dirs
        if let Ok(home) = std::env::var("HOME") {
            for dir in SENSITIVE_HOME_DIRS {
                let sensitive = format!("{home}/{dir}");
                if path_str.starts_with(&sensitive) {
                    return Ok(StepResult {
                        step_id: step_id.to_string(),
                        status: StepStatus::Failed,
                        exit_code: Some(1),
                        stdout: String::new(),
                        stderr: format!(
                            "write blocked in strict_mode: path '{resolved_path}' is under ~/{dir}"
                        ),
                        execution_time_ms: start.elapsed().as_millis() as u64,
                        diagnostic: None,
                        headers: HashMap::new(),
                    });
                }
            }
        }
    }

    // Create parent directory if needed
    if let Some(parent) = Path::new(&resolved_path).parent()
        && !parent.exists()
        && let Err(e) = std::fs::create_dir_all(parent)
    {
        return Ok(StepResult {
            step_id: step_id.to_string(),
            status: StepStatus::Failed,
            exit_code: Some(1),
            stdout: String::new(),
            stderr: format!("failed to create directory: {e}"),
            execution_time_ms: start.elapsed().as_millis() as u64,
            diagnostic: None,
            headers: HashMap::new(),
        });
    }

    // Write or append
    let write_result = match mode {
        WriteMode::Overwrite => std::fs::write(&resolved_path, &resolved_content),
        WriteMode::Append => {
            use std::io::Write;
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&resolved_path)
                .and_then(|mut f| f.write_all(resolved_content.as_bytes()))
        }
    };

    let elapsed = start.elapsed().as_millis() as u64;

    match write_result {
        Ok(()) => {
            let bytes = resolved_content.len();
            let mode_str = match mode {
                WriteMode::Overwrite => "wrote",
                WriteMode::Append => "appended",
            };
            Ok(StepResult {
                step_id: step_id.to_string(),
                status: StepStatus::Success,
                exit_code: Some(0),
                stdout: resolved_path,
                stderr: format!("{mode_str} {bytes} bytes"),
                execution_time_ms: elapsed,
                diagnostic: None,
                headers: HashMap::new(),
            })
        }
        Err(e) => Ok(StepResult {
            step_id: step_id.to_string(),
            status: StepStatus::Failed,
            exit_code: Some(1),
            stdout: String::new(),
            stderr: format!("write failed: {e}"),
            execution_time_ms: elapsed,
            diagnostic: None,
            headers: HashMap::new(),
        }),
    }
}
