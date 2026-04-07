use routines_core::audit::AuditDb;

use crate::routines_dir;

pub fn cmd_log(run_id: &str, full: bool) -> routines_core::error::Result<()> {
    let db = AuditDb::open(&routines_dir().join("data.db"))?;

    let log = db.get_run_log(run_id)?;
    let Some(log) = log else {
        eprintln!("Run not found: {run_id}");
        std::process::exit(1);
    };

    // Header
    println!("Routine: {}", log.routine_name);
    println!("Run ID:  {}", log.run_id);
    println!("Status:  {}", log.status);
    println!("Started: {}", log.started_at);
    if let Some(ended) = &log.ended_at {
        println!("Ended:   {ended}");
    }
    if let Some(inputs) = &log.input_vars
        && inputs != "{}"
    {
        println!("Inputs:  {inputs}");
    }
    println!("{}", "-".repeat(60));

    // Steps
    let max_lines = if full { usize::MAX } else { 10 };

    for (i, step) in log.steps.iter().enumerate() {
        let icon = if step.status == "SUCCESS" {
            "OK"
        } else {
            "FAIL"
        };
        let tokens = match (step.stdout_tokens, step.stderr_tokens) {
            (Some(out), Some(err)) => format!(", ~{}tok", out + err),
            _ => String::new(),
        };
        println!(
            "\n[{icon}] Step {num}: {id}  (exit={exit}, {ms}ms{tokens})",
            num = i + 1,
            id = step.step_id,
            exit = step.exit_code.unwrap_or(-1),
            ms = step.execution_time_ms,
        );

        if let Some(stdout) = &step.stdout {
            let trimmed = stdout.trim();
            if !trimmed.is_empty() {
                print_truncated("  stdout: ", trimmed, max_lines);
            }
        }
        if let Some(stderr) = &step.stderr {
            let trimmed = stderr.trim();
            if !trimmed.is_empty() {
                print_truncated("  stderr: ", trimmed, max_lines);
            }
        }
    }

    // Output
    if let Some(output) = &log.output {
        let trimmed = output.trim();
        if !trimmed.is_empty() {
            println!("\n{}", "-".repeat(60));
            println!("Output:");
            println!("{trimmed}");
        }
    }

    // Token summary
    let total_tokens: i64 = log
        .steps
        .iter()
        .map(|s| s.stdout_tokens.unwrap_or(0) + s.stderr_tokens.unwrap_or(0))
        .sum();
    if total_tokens > 0 {
        println!("\nTotal output: ~{total_tokens} tokens (estimated)");
    }

    println!();
    Ok(())
}

fn print_truncated(prefix: &str, text: &str, max_lines: usize) {
    let lines: Vec<&str> = text.lines().collect();
    if lines.len() <= max_lines {
        println!("{prefix}{text}");
    } else {
        for (i, line) in lines.iter().enumerate() {
            if i == 0 {
                println!("{prefix}{line}");
            } else if i < max_lines {
                println!("  {line}");
            } else {
                println!(
                    "  ... ({} more lines, use --full to show all)",
                    lines.len() - max_lines
                );
                break;
            }
        }
    }
}
