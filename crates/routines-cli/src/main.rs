use std::collections::HashMap;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use routines_core::audit::AuditDb;
use routines_core::executor::{self, RunStatus, StepStatus};
use routines_core::parser::Routine;
use routines_core::secrets;
use routines_core::server::RoutinesMcpServer;

fn routines_dir() -> PathBuf {
    dirs_home().join(".routines")
}

fn dirs_home() -> PathBuf {
    std::env::var("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("."))
}

#[derive(Parser)]
#[command(
    name = "routines",
    version,
    about = "Deterministic workflow engine for AI agents"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Execute a routine by name
    Run {
        /// Routine name (looks up ~/.routines/hub/<name>.yml)
        name: String,
        /// Input key=value pairs
        #[arg(trailing_var_arg = true)]
        inputs: Vec<String>,
    },
    /// Start MCP server (stdio transport)
    Serve,
    /// Show audit log for a routine run
    Log {
        /// Run ID (UUID) to display
        run_id: String,
    },
}

fn main() {
    let cli = Cli::parse();
    if let Err(e) = dispatch(cli) {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}

fn dispatch(cli: Cli) -> routines_core::error::Result<()> {
    match cli.command {
        Commands::Run { name, inputs } => cmd_run(&name, &inputs),
        Commands::Serve => cmd_serve(),
        Commands::Log { run_id } => cmd_log(&run_id),
    }
}

fn cmd_serve() -> routines_core::error::Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let server = RoutinesMcpServer::new();
        let service = rmcp::ServiceExt::serve(server, rmcp::transport::stdio())
            .await
            .map_err(|e| {
                routines_core::error::RoutineError::Io(std::io::Error::other(e.to_string()))
            })?;
        service.waiting().await.map_err(|e| {
            routines_core::error::RoutineError::Io(std::io::Error::other(e.to_string()))
        })?;
        Ok(())
    })
}

fn cmd_run(name: &str, raw_inputs: &[String]) -> routines_core::error::Result<()> {
    // Locate routine YAML
    let yaml_path = routines_dir().join("hub").join(format!("{name}.yml"));
    if !yaml_path.exists() {
        eprintln!("Routine not found: {}", yaml_path.display());
        std::process::exit(1);
    }

    let routine = Routine::from_file(&yaml_path)?;

    // Parse KEY=VALUE inputs
    let inputs: HashMap<String, String> = raw_inputs
        .iter()
        .filter_map(|s| {
            let (k, v) = s.split_once('=')?;
            Some((k.to_string(), v.to_string()))
        })
        .collect();

    // Load secrets
    let secrets = secrets::load_secrets(&routines_dir().join(".env"));

    // Open audit DB
    let db = AuditDb::open(&routines_dir().join("data.db"))?;

    // Generate run ID and timestamp
    let run_id = uuid::Uuid::new_v4().to_string();
    let started_at = chrono::Utc::now().to_rfc3339();

    // Redact secrets in input_vars before storing
    let secret_values: Vec<&str> = secrets.values().map(|s| s.as_str()).collect();
    let input_vars_json = serde_json::to_string(&inputs).unwrap_or_default();
    let input_vars_redacted = routines_core::secrets::redact(&input_vars_json, &secret_values);

    db.insert_run(&run_id, &routine.name, &input_vars_redacted, &started_at)?;

    println!("Running routine: {} (run_id: {})", routine.name, run_id);
    println!("---");

    // Execute
    let result = executor::run_routine(&routine, inputs, secrets.clone())?;

    // Write step logs
    for step in &result.step_results {
        let step_started = chrono::Utc::now().to_rfc3339();
        db.insert_step_log(&run_id, step, &secret_values, &step_started)?;

        let icon = match step.status {
            StepStatus::Success => "OK",
            StepStatus::Failed => "FAIL",
            StepStatus::Skipped => "SKIP",
        };
        println!(
            "[{icon}] {step_id} (exit={exit}, {ms}ms)",
            step_id = step.step_id,
            exit = step.exit_code.unwrap_or(-1),
            ms = step.execution_time_ms,
        );
    }

    // Finalize run
    let ended_at = chrono::Utc::now().to_rfc3339();
    db.finalize_run(&run_id, &result, &ended_at)?;

    println!("---");
    match result.status {
        RunStatus::Success => println!("Result: SUCCESS"),
        RunStatus::Failed => {
            println!("Result: FAILED");
            std::process::exit(1);
        }
    }

    Ok(())
}

fn cmd_log(run_id: &str) -> routines_core::error::Result<()> {
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
                println!("  stdout: {trimmed}");
            }
        }
        if let Some(stderr) = &step.stderr {
            let trimmed = stderr.trim();
            if !trimmed.is_empty() {
                println!("  stderr: {trimmed}");
            }
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
