use std::collections::HashMap;

use routines_core::audit::AuditDb;
use routines_core::executor::{self, RunStatus, StepStatus};
use routines_core::parser::Routine;
use routines_core::resolve::resolve_routine_path;
use routines_core::secrets;

use routines_protocol::types::RunEvent;
use routines_protocol::DaemonClient;

use crate::routines_dir;

pub fn cmd_run(
    name: &str,
    raw_inputs: &[String],
    quiet: bool,
    verbose: bool,
    detach: bool,
) -> routines_core::error::Result<()> {
    use colored::Colorize;
    use std::io::IsTerminal;

    // Locate routine YAML (validate locally before sending to daemon)
    let yaml_path = resolve_routine_path(name, &routines_dir());
    if !yaml_path.exists() {
        eprintln!("Routine not found: {}", yaml_path.display());
        std::process::exit(1);
    }

    let routine = Routine::from_file(&yaml_path)?;
    let is_tty = std::io::stdout().is_terminal() && !quiet;

    // Parse KEY=VALUE inputs
    let mut inputs: HashMap<String, String> = raw_inputs
        .iter()
        .filter_map(|s| {
            let (k, v) = s.split_once('=')?;
            Some((k.to_string(), v.to_string()))
        })
        .collect();

    // TTY mode: interactively prompt for missing inputs
    if std::io::stdin().is_terminal() && !quiet {
        inputs = crate::prompt_missing_inputs(&routine.inputs, &inputs)?;
    }

    // Try daemon mode first, fall back to direct execution
    let rt = tokio::runtime::Runtime::new()?;
    match rt.block_on(try_run_via_daemon(name, &inputs, detach)) {
        Ok(DaemonRunResult::Detached { run_id }) => {
            if is_tty {
                eprintln!("Submitted to daemon: {}", &run_id[..8].dimmed());
            } else {
                println!("{run_id}");
            }
            return Ok(());
        }
        Ok(DaemonRunResult::Completed { result }) => {
            if is_tty {
                print_result_tty(&routine, &result, verbose);
            } else {
                print_result_pipe(&result);
            }
            if result.status == RunStatus::Failed {
                std::process::exit(1);
            }
            return Ok(());
        }
        Err(_) => {
            // Daemon not available — fall back to direct execution
        }
    }

    // Direct execution (fallback when daemon is not running)
    run_direct(&routine, name, inputs, quiet, verbose, is_tty)
}

enum DaemonRunResult {
    Detached { run_id: String },
    Completed { result: executor::RunResult },
}

async fn try_run_via_daemon(
    name: &str,
    inputs: &HashMap<String, String>,
    detach: bool,
) -> Result<DaemonRunResult, routines_protocol::client::ClientError> {
    let mut client = DaemonClient::try_connect().await?.ok_or(
        routines_protocol::client::ClientError::ConnectionFailed { attempts: 0 },
    )?;

    let run_id = client.submit(name, inputs.clone()).await?;

    if detach {
        return Ok(DaemonRunResult::Detached { run_id });
    }

    // Subscribe and wait for completion
    let mut final_result = None;
    client
        .subscribe(&run_id, |event| {
            if let RunEvent::RunCompleted { result, .. } = event {
                final_result = Some(result.as_ref().clone());
            }
        })
        .await?;

    match final_result {
        Some(result) => {
            // Convert protocol RunResult to engine RunResult
            Ok(DaemonRunResult::Completed { result })
        }
        None => Err(routines_protocol::client::ClientError::UnexpectedResponse),
    }
}

fn print_result_tty(
    routine: &Routine,
    result: &executor::RunResult,
    verbose: bool,
) {
    use colored::Colorize;

    let is_failed = result.status == RunStatus::Failed;
    let total_executions = result.step_results.len();
    let logical_steps = {
        let mut seen = std::collections::HashSet::new();
        for s in &result.step_results {
            let base = s.step_id.split('[').next().unwrap_or(&s.step_id);
            seen.insert(base);
        }
        seen.len()
    };
    let ok_steps = result
        .step_results
        .iter()
        .filter(|s| s.status == StepStatus::Success)
        .count();
    let total_ms: u64 = result
        .step_results
        .iter()
        .map(|s| s.execution_time_ms)
        .sum();

    if verbose || is_failed {
        let mut table = comfy_table::Table::new();
        table.load_preset(comfy_table::presets::UTF8_FULL);
        table.set_header(vec!["#", "Step", "Status", "Exit", "Time"]);
        for (i, step) in result.step_results.iter().enumerate() {
            use comfy_table::{Cell, Color as TColor};
            let status_cell = match step.status {
                StepStatus::Success => Cell::new("OK").fg(TColor::Green),
                StepStatus::Failed => Cell::new("FAIL").fg(TColor::Red),
                StepStatus::Skipped => Cell::new("SKIP").fg(TColor::Yellow),
            };
            table.add_row(vec![
                Cell::new(i + 1),
                Cell::new(&step.step_id),
                status_cell,
                Cell::new(step.exit_code.map(|c| c.to_string()).unwrap_or("-".into())),
                Cell::new(format!("{}ms", step.execution_time_ms)),
            ]);
        }
        eprintln!("{table}");

        if is_failed {
            for step in &result.step_results {
                if step.status == StepStatus::Failed && !step.stderr.is_empty() {
                    eprintln!("{} {}", "stderr:".red(), step.stderr.trim());
                }
            }
        }
    } else {
        let time_str = if total_ms >= 1000 {
            format!("{:.1}s", total_ms as f64 / 1000.0)
        } else {
            format!("{total_ms}ms")
        };
        let steps_str = if total_executions > logical_steps {
            format!("{ok_steps}/{logical_steps} steps ({total_executions} executions)")
        } else {
            format!("{ok_steps}/{logical_steps} steps")
        };
        eprintln!(
            "{} {} {} {}",
            "✓".green().bold(),
            routine.name.bold(),
            steps_str,
            time_str.dimmed(),
        );
    }

    if let Some(output) = &result.output {
        let trimmed = output.trim();
        if !trimmed.is_empty() {
            render_output(trimmed, routine.output.as_ref(), true);
        }
    }

    if is_failed {
        eprintln!("Result: {}", "FAILED".red().bold());
    }
}

fn print_result_pipe(result: &executor::RunResult) {
    if let Some(output) = &result.output {
        let trimmed = output.trim();
        if !trimmed.is_empty() {
            render_output(trimmed, None, false);
        }
    }
}

/// Direct execution fallback when daemon is not available.
fn run_direct(
    routine: &Routine,
    _name: &str,
    inputs: HashMap<String, String>,
    _quiet: bool,
    verbose: bool,
    is_tty: bool,
) -> routines_core::error::Result<()> {
    use colored::Colorize;
    use routines_core::parser::AuditLevel;

    let secrets = secrets::load_secrets(&routines_dir().join(".env"));
    let audit_level = &routine.audit;
    let secret_values: Vec<&str> = secrets.values().map(|s| s.as_str()).collect();

    let db = if *audit_level != AuditLevel::None {
        Some(AuditDb::open(&routines_dir().join("data.db"))?)
    } else {
        None
    };

    let run_id = uuid::Uuid::new_v4().to_string();
    let started_at = chrono::Utc::now().to_rfc3339();

    if let Some(db) = &db {
        let input_vars_json = serde_json::to_string(&inputs).unwrap_or_default();
        let input_vars_redacted = routines_core::secrets::redact(&input_vars_json, &secret_values);
        db.insert_run(&run_id, &routine.name, &input_vars_redacted, &started_at)?;
    }

    if is_tty && verbose {
        eprintln!(
            "Running routine: {} (run_id: {})",
            routine.name.bold(),
            &run_id[..8].dimmed()
        );
    }

    let result = executor::run_routine(routine, inputs, secrets.clone())?;

    if let Some(db) = &db {
        match audit_level {
            AuditLevel::Full => {
                for step in &result.step_results {
                    let step_started = chrono::Utc::now().to_rfc3339();
                    db.insert_step_log(&run_id, step, &secret_values, &step_started)?;
                }
            }
            AuditLevel::Summary => {
                for step in &result.step_results {
                    if step.status == StepStatus::Failed {
                        let step_started = chrono::Utc::now().to_rfc3339();
                        db.insert_step_log(&run_id, step, &secret_values, &step_started)?;
                    }
                }
            }
            AuditLevel::None => unreachable!(),
        }
        let ended_at = chrono::Utc::now().to_rfc3339();
        db.finalize_run(&run_id, &result, &ended_at)?;
    }

    if is_tty {
        print_result_tty(routine, &result, verbose);
        if result.status == RunStatus::Failed {
            std::process::exit(1);
        }
    } else {
        print_result_pipe(&result);
        if result.status == RunStatus::Failed {
            std::process::exit(1);
        }
    }

    Ok(())
}

pub fn render_output(
    output: &str,
    output_config: Option<&routines_core::parser::OutputConfig>,
    is_tty: bool,
) {
    use routines_core::parser::OutputFormat;

    let format = output_config
        .map(|c| &c.format)
        .unwrap_or(&OutputFormat::Plain);
    let explicit_columns = output_config.and_then(|c| c.columns.as_ref());

    match format {
        OutputFormat::Table => {
            if let Ok(rows) =
                serde_json::from_str::<Vec<serde_json::Map<String, serde_json::Value>>>(output)
            {
                if rows.is_empty() {
                    println!("(empty)");
                    return;
                }
                let inferred: Vec<&String>;
                let columns: Vec<&String> = if let Some(cols) = explicit_columns {
                    cols.iter().collect()
                } else {
                    inferred = rows[0].keys().collect();
                    inferred
                };

                if is_tty {
                    let mut table = comfy_table::Table::new();
                    table.load_preset(comfy_table::presets::UTF8_FULL);
                    table.set_content_arrangement(comfy_table::ContentArrangement::Dynamic);
                    table.set_header(columns.iter().map(|c| c.as_str()));
                    for row in &rows {
                        let cells: Vec<String> = columns
                            .iter()
                            .map(|col| match row.get(*col) {
                                Some(serde_json::Value::String(s)) => s.clone(),
                                Some(v) => v.to_string(),
                                None => String::new(),
                            })
                            .collect();
                        table.add_row(cells);
                    }
                    println!("{table}");
                } else {
                    println!(
                        "{}",
                        columns
                            .iter()
                            .map(|c| c.as_str())
                            .collect::<Vec<_>>()
                            .join("\t")
                    );
                    for row in &rows {
                        let cells: Vec<String> = columns
                            .iter()
                            .map(|col| match row.get(*col) {
                                Some(serde_json::Value::String(s)) => s.clone(),
                                Some(v) => v.to_string(),
                                None => String::new(),
                            })
                            .collect();
                        println!("{}", cells.join("\t"));
                    }
                }
            } else {
                println!("{output}");
            }
        }
        OutputFormat::Plain => {
            println!("{output}");
        }
    }
}
