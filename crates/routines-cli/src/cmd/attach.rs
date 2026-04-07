use routines_protocol::DaemonClient;
use routines_protocol::types::RunEvent;

pub fn cmd_attach(run_id: &str) -> routines_core::error::Result<()> {
    use colored::Colorize;

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let mut client = DaemonClient::try_connect()
            .await
            .map_err(|e| {
                routines_core::error::RoutineError::Io(std::io::Error::other(e.to_string()))
            })?
            .ok_or_else(|| {
                routines_core::error::RoutineError::Io(std::io::Error::other(
                    "daemon not running",
                ))
            })?;

        eprintln!("Attached to run {}", &run_id[..8.min(run_id.len())].dimmed());

        client
            .subscribe(run_id, |event| match event {
                RunEvent::StepStarted { step_id, .. } => {
                    eprintln!("{} {step_id}", "▶".blue());
                }
                RunEvent::StepCompleted {
                    step_id, result, ..
                } => {
                    let icon = match result.status {
                        routines_engine::executor::StepStatus::Success => "✓".green(),
                        routines_engine::executor::StepStatus::Failed => "✗".red(),
                        routines_engine::executor::StepStatus::Skipped => "⊘".yellow(),
                    };
                    eprintln!(
                        "{icon} {step_id} ({}ms)",
                        result.execution_time_ms
                    );
                    if !result.stdout.is_empty() {
                        let trimmed = result.stdout.trim();
                        if trimmed.len() <= 200 {
                            eprintln!("  {trimmed}");
                        } else {
                            eprintln!("  {}...", &trimmed[..200]);
                        }
                    }
                }
                RunEvent::RunCompleted { result, .. } => {
                    match result.status {
                        routines_engine::executor::RunStatus::Success => {
                            eprintln!("{}", "Run completed successfully".green());
                        }
                        routines_engine::executor::RunStatus::Failed => {
                            eprintln!("{}", "Run failed".red());
                        }
                    }
                    if let Some(output) = &result.output {
                        let trimmed = output.trim();
                        if !trimmed.is_empty() {
                            println!("{trimmed}");
                        }
                    }
                }
                RunEvent::Log { message, .. } => {
                    eprintln!("  {message}");
                }
            })
            .await
            .map_err(|e| {
                routines_core::error::RoutineError::Io(std::io::Error::other(e.to_string()))
            })?;

        Ok(())
    })
}
