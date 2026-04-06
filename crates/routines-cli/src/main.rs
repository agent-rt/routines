use std::collections::HashMap;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use routines_core::audit::AuditDb;
use routines_core::executor::{self, RunStatus, StepStatus};
use routines_core::mcp_config::{McpConfig, McpServerConfig};
use routines_core::parser::Routine;
use routines_core::registry::{self, Registries, RegistryConfig};
use routines_core::resolve::resolve_routine_path;
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
        /// Only output the routine's output field, no chrome
        #[arg(short, long)]
        quiet: bool,
        /// Show step execution table (hidden by default, shown on failure)
        #[arg(short, long)]
        verbose: bool,
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
        /// Show full stdout/stderr without truncation
        #[arg(long)]
        full: bool,
    },
    /// Manage MCP server configurations
    Mcp {
        #[command(subcommand)]
        action: McpAction,
    },
    /// Manage routine registries (remote sources)
    Registry {
        #[command(subcommand)]
        action: RegistryAction,
    },
    /// List all available routines
    List,
    /// Validate a routine YAML file
    Validate {
        /// Path to the YAML file
        file: PathBuf,
    },
    /// Run routine tests with mock data
    Test {
        /// Test YAML file path (or --all to scan ~/.routines/tests/)
        file: Option<PathBuf>,
        /// Run all tests in ~/.routines/tests/
        #[arg(long)]
        all: bool,
    },
}

#[derive(Subcommand)]
enum McpAction {
    /// List configured MCP servers
    List,
    /// Add an MCP server
    Add {
        /// Server name
        name: String,
        /// Command to start the server
        command: String,
        /// Arguments for the command
        #[arg(trailing_var_arg = true)]
        args: Vec<String>,
        /// Environment variable in KEY=VALUE format (repeatable)
        #[arg(short, long = "env", value_name = "KEY=VALUE")]
        envs: Vec<String>,
    },
    /// Remove an MCP server
    Remove {
        /// Server name to remove
        name: String,
    },
    /// Show server details and test connection
    Get {
        /// Server name
        name: String,
    },
}

#[derive(Subcommand)]
enum RegistryAction {
    /// List configured registries
    List,
    /// Add a registry from a Git URL
    Add {
        /// Registry name
        name: String,
        /// Git repository URL
        url: String,
        /// Branch or tag (default: main)
        #[arg(long = "ref", default_value = "main")]
        git_ref: String,
    },
    /// Remove a registry
    Remove {
        /// Registry name
        name: String,
    },
    /// Sync registries (git clone/pull)
    Sync {
        /// Registry name (omit to sync all)
        name: Option<String>,
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
        Commands::Run {
            name,
            quiet,
            verbose,
            inputs,
        } => cmd_run(&name, &inputs, quiet, verbose),
        Commands::Serve => cmd_serve(),
        Commands::Log { run_id, full } => cmd_log(&run_id, full),
        Commands::Mcp { action } => cmd_mcp(action),
        Commands::Registry { action } => cmd_registry(action),
        Commands::List => cmd_list(),
        Commands::Validate { file } => cmd_validate(&file),
        Commands::Test { file, all } => cmd_test(file.as_deref(), all),
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

fn cmd_list() -> routines_core::error::Result<()> {
    use colored::Colorize;
    use std::io::IsTerminal;

    let is_tty = std::io::stdout().is_terminal();
    let rdir = routines_dir();
    let hub_dir = rdir.join("hub");
    let mut has_required = false;
    let mut has_output = false;

    if hub_dir.exists() {
        let entries =
            routines_core::server::collect_routines_recursive(&hub_dir, "");
        for (ref_name, routine) in &entries {
            let inputs_desc = format_inputs_cli(&routine.inputs, &mut has_required);
            if is_tty {
                println!("{:<20} — {}{inputs_desc}", ref_name.bold(), routine.description);
            } else {
                println!("{ref_name} — {}{inputs_desc}", routine.description);
            }
            has_output = true;
        }
    }

    let reg_dir = rdir.join("registries");
    if reg_dir.exists()
        && let Ok(dirs) = std::fs::read_dir(&reg_dir)
    {
        for dir_entry in dirs.flatten() {
            if dir_entry.path().is_dir() {
                let reg_name = dir_entry.file_name().to_string_lossy().to_string();
                let entries =
                    routines_core::server::collect_routines_recursive(&dir_entry.path(), "");
                if !entries.is_empty() && has_output {
                    println!();
                }
                for (name, routine) in &entries {
                    let inputs_desc = format_inputs_cli(&routine.inputs, &mut has_required);
                    let full_name = format!("@{reg_name}/{name}");
                    if is_tty {
                        println!("{:<20} — {}{inputs_desc}", full_name.bold(), routine.description);
                    } else {
                        println!("{full_name} — {}{inputs_desc}", routine.description);
                    }
                    has_output = true;
                }
            }
        }
    }

    if has_required {
        println!("\n(* = required)");
    }

    if !has_output {
        println!("No routines found");
    }

    Ok(())
}

fn format_inputs_cli(inputs: &[routines_core::parser::InputDef], has_required: &mut bool) -> String {
    if inputs.is_empty() {
        return String::new();
    }
    let parts: Vec<String> = inputs
        .iter()
        .map(|i| {
            if i.required {
                *has_required = true;
                format!("{}*", i.name)
            } else {
                i.name.clone()
            }
        })
        .collect();
    format!(" (inputs: {})", parts.join(", "))
}

fn cmd_validate(file: &std::path::Path) -> routines_core::error::Result<()> {
    use colored::Colorize;
    use std::io::IsTerminal;

    let is_tty = std::io::stdout().is_terminal();

    match Routine::from_file(file) {
        Ok(routine) => {
            let steps = routine.steps.len();
            let inputs = routine.inputs.len();
            if is_tty {
                println!(
                    "{} Valid: {} ({} steps, {} inputs)",
                    "✓".green(),
                    routine.name.bold(),
                    steps,
                    inputs
                );
            } else {
                println!("Valid: {} ({steps} steps, {inputs} inputs)", routine.name);
            }
            Ok(())
        }
        Err(e) => {
            if is_tty {
                eprintln!("{} {e}", "✗".red());
            } else {
                eprintln!("Error: {e}");
            }
            std::process::exit(1);
        }
    }
}

fn cmd_test(file: Option<&std::path::Path>, all: bool) -> routines_core::error::Result<()> {
    use colored::Colorize;
    use routines_core::testing::{TestSuite, run_test_suite};

    let rdir = routines_dir();
    let mut suites: Vec<(String, TestSuite)> = Vec::new();

    if let Some(path) = file {
        let suite = TestSuite::from_file(path)?;
        let label = path.display().to_string();
        suites.push((label, suite));
    } else if all {
        let test_dir = rdir.join("tests");
        if !test_dir.exists() {
            eprintln!("No tests directory found at {}", test_dir.display());
            std::process::exit(1);
        }
        let mut entries: Vec<_> = std::fs::read_dir(&test_dir)?
            .flatten()
            .filter(|e| {
                let name = e.file_name().to_string_lossy().to_string();
                name.ends_with("_test.yml") || name.ends_with("_test.yaml")
            })
            .collect();
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let suite = TestSuite::from_file(&entry.path())?;
            let label = entry.file_name().to_string_lossy().to_string();
            suites.push((label, suite));
        }
        if suites.is_empty() {
            eprintln!("No test files found in {}", test_dir.display());
            std::process::exit(1);
        }
    } else {
        eprintln!("Usage: routines test <file> or routines test --all");
        std::process::exit(1);
    }

    let mut total_pass = 0;
    let mut total_fail = 0;

    for (label, suite) in &suites {
        eprintln!("{}", label.bold());
        let results = run_test_suite(suite, &rdir);
        for result in &results {
            if result.passed {
                eprintln!("  {} {}", "PASS".green(), result.name);
                total_pass += 1;
            } else {
                eprintln!("  {} {}", "FAIL".red(), result.name);
                for failure in &result.failures {
                    eprintln!("    {}", failure);
                }
                total_fail += 1;
            }
        }
    }

    eprintln!();
    if total_fail == 0 {
        eprintln!(
            "{} {} passed",
            "✓".green(),
            total_pass
        );
    } else {
        eprintln!(
            "{} {} passed, {} failed",
            "✗".red(),
            total_pass,
            total_fail
        );
        std::process::exit(1);
    }

    Ok(())
}

fn prompt_missing_inputs(
    inputs_def: &[routines_core::parser::InputDef],
    provided: &HashMap<String, String>,
) -> routines_core::error::Result<HashMap<String, String>> {
    use routines_core::parser::InputType;

    let mut result = provided.clone();

    // Check if any required inputs are missing — if not, skip prompting entirely
    let has_missing_required = inputs_def
        .iter()
        .any(|i| i.required && !result.contains_key(&i.name));

    if !has_missing_required {
        // All required provided — just apply defaults for optional fields silently
        for input in inputs_def {
            if !result.contains_key(&input.name)
                && let Some(def) = &input.default
            {
                result.insert(input.name.clone(), def.clone());
            }
        }
        return Ok(result);
    }

    for input in inputs_def {
        if result.contains_key(&input.name) {
            continue;
        }

        let label = match &input.description {
            Some(desc) => format!("{} ({})", input.name, desc),
            None => input.name.clone(),
        };

        let value = match &input.input_type {
            InputType::Bool => {
                let default_val = input
                    .default
                    .as_deref()
                    .map(|d| d == "true")
                    .unwrap_or(false);
                let ans = inquire::Confirm::new(&label)
                    .with_default(default_val)
                    .prompt();
                match ans {
                    Ok(v) => Some(v.to_string()),
                    Err(inquire::InquireError::OperationCanceled | inquire::InquireError::OperationInterrupted) => {
                        std::process::exit(130);
                    }
                    Err(e) => return Err(routines_core::error::RoutineError::Io(
                        std::io::Error::other(e.to_string()),
                    )),
                }
            }
            InputType::Enum => {
                let Some(values) = &input.enum_values else {
                    continue;
                };
                let mut prompt = inquire::Select::new(&label, values.clone());
                if let Some(def) = &input.default
                    && let Some(idx) = values.iter().position(|v| v == def)
                {
                    prompt = prompt.with_starting_cursor(idx);
                }
                match prompt.prompt() {
                    Ok(v) => Some(v),
                    Err(inquire::InquireError::OperationCanceled | inquire::InquireError::OperationInterrupted) => {
                        std::process::exit(130);
                    }
                    Err(e) => return Err(routines_core::error::RoutineError::Io(
                        std::io::Error::other(e.to_string()),
                    )),
                }
            }
            other => {
                let mut prompt = inquire::Text::new(&label);
                if let Some(def) = &input.default {
                    prompt = prompt.with_default(def);
                }
                if !input.required && input.default.is_none() {
                    prompt = prompt.with_help_message("optional, press Enter to skip");
                }
                let ans = match other {
                    InputType::Int => prompt
                        .with_validator(|input: &str| {
                            if input.is_empty() || input.parse::<i64>().is_ok() {
                                Ok(inquire::validator::Validation::Valid)
                            } else {
                                Ok(inquire::validator::Validation::Invalid("Must be an integer".into()))
                            }
                        })
                        .prompt(),
                    InputType::Float => prompt
                        .with_validator(|input: &str| {
                            if input.is_empty() || input.parse::<f64>().is_ok() {
                                Ok(inquire::validator::Validation::Valid)
                            } else {
                                Ok(inquire::validator::Validation::Invalid("Must be a number".into()))
                            }
                        })
                        .prompt(),
                    InputType::Date => prompt
                        .with_validator(|input: &str| {
                            if input.is_empty() {
                                return Ok(inquire::validator::Validation::Valid);
                            }
                            let parts: Vec<&str> = input.split('-').collect();
                            if parts.len() == 3
                                && parts[0].len() == 4
                                && parts[1].len() == 2
                                && parts[2].len() == 2
                                && parts.iter().all(|p| p.chars().all(|c| c.is_ascii_digit()))
                            {
                                Ok(inquire::validator::Validation::Valid)
                            } else {
                                Ok(inquire::validator::Validation::Invalid("Must be YYYY-MM-DD format".into()))
                            }
                        })
                        .prompt(),
                    _ => prompt.prompt(),
                };
                match ans {
                    Ok(v) if v.is_empty() && !input.required => None,
                    Ok(v) if v.is_empty() && input.required => {
                        return Err(routines_core::error::RoutineError::InvalidInput {
                            name: input.name.clone(),
                            expected: format!("{:?}", input.input_type),
                            got: "empty".to_string(),
                        });
                    }
                    Ok(v) => Some(v),
                    Err(inquire::InquireError::OperationCanceled | inquire::InquireError::OperationInterrupted) => {
                        std::process::exit(130);
                    }
                    Err(e) => return Err(routines_core::error::RoutineError::Io(
                        std::io::Error::other(e.to_string()),
                    )),
                }
            }
        };

        if let Some(v) = value {
            result.insert(input.name.clone(), v);
        }
    }

    Ok(result)
}

fn cmd_run(name: &str, raw_inputs: &[String], quiet: bool, verbose: bool) -> routines_core::error::Result<()> {
    use colored::Colorize;
    use std::io::IsTerminal;

    // Locate routine YAML
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
        inputs = prompt_missing_inputs(&routine.inputs, &inputs)?;
    }

    // Load secrets
    let secrets = secrets::load_secrets(&routines_dir().join(".env"));

    use routines_core::parser::AuditLevel;

    let audit_level = &routine.audit;
    let secret_values: Vec<&str> = secrets.values().map(|s| s.as_str()).collect();

    // Open audit DB (skip entirely for audit: none)
    let db = if *audit_level != AuditLevel::None {
        Some(AuditDb::open(&routines_dir().join("data.db"))?)
    } else {
        None
    };

    // Generate run ID and timestamp
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

    // Execute
    let result = executor::run_routine(&routine, inputs, secrets.clone())?;

    // Write step logs based on audit level
    if let Some(db) = &db {
        match audit_level {
            AuditLevel::Full => {
                for step in &result.step_results {
                    let step_started = chrono::Utc::now().to_rfc3339();
                    db.insert_step_log(&run_id, step, &secret_values, &step_started)?;
                }
            }
            AuditLevel::Summary => {
                // Only log failed steps
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
        let is_failed = result.status == RunStatus::Failed;
        let total_steps = result.step_results.len();
        let ok_steps = result.step_results.iter().filter(|s| s.status == StepStatus::Success).count();
        let total_ms: u64 = result.step_results.iter().map(|s| s.execution_time_ms).sum();

        // Show step table only on failure or --verbose
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
                    Cell::new(
                        step.exit_code
                            .map(|c| c.to_string())
                            .unwrap_or("-".into()),
                    ),
                    Cell::new(format!("{}ms", step.execution_time_ms)),
                ]);
            }
            eprintln!("{table}");

            // On failure, show stderr of failed steps
            if is_failed {
                for step in &result.step_results {
                    if step.status == StepStatus::Failed && !step.stderr.is_empty() {
                        eprintln!("{} {}", "stderr:".red(), step.stderr.trim());
                    }
                }
            }
        } else {
            // Compact status line
            let time_str = if total_ms >= 1000 {
                format!("{:.1}s", total_ms as f64 / 1000.0)
            } else {
                format!("{total_ms}ms")
            };
            eprintln!(
                "{} {} {}/{} steps {}",
                "✓".green().bold(),
                routine.name.bold(),
                ok_steps,
                total_steps,
                time_str.dimmed(),
            );
        }

        // Output
        if let Some(output) = &result.output {
            let trimmed = output.trim();
            if !trimmed.is_empty() {
                render_output(trimmed, &routine.output_format, true);
            }
        }

        // Result (only show explicit FAILED, success is implied by ✓)
        if is_failed {
            eprintln!("Result: {}", "FAILED".red().bold());
            std::process::exit(1);
        }
    } else {
        // Pipe mode: only output
        if let Some(output) = &result.output {
            let trimmed = output.trim();
            if !trimmed.is_empty() {
                render_output(trimmed, &routine.output_format, false);
            }
        }
        if result.status == RunStatus::Failed {
            std::process::exit(1);
        }
    }

    Ok(())
}

/// Render output according to format. TTY: comfy-table for table format. Pipe: TSV.
fn render_output(
    output: &str,
    format: &routines_core::parser::OutputFormat,
    is_tty: bool,
) {
    use routines_core::parser::OutputFormat;

    match format {
        OutputFormat::Table => {
            // Try to parse as JSON array of objects
            if let Ok(rows) = serde_json::from_str::<Vec<serde_json::Map<String, serde_json::Value>>>(output) {
                if rows.is_empty() {
                    println!("(empty)");
                    return;
                }
                // Collect column names from first row
                let columns: Vec<&String> = rows[0].keys().collect();

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
                    // TSV output
                    println!(
                        "{}",
                        columns.iter().map(|c| c.as_str()).collect::<Vec<_>>().join("\t")
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
                // Not valid JSON array, fallback to plain
                println!("{output}");
            }
        }
        OutputFormat::Plain => {
            println!("{output}");
        }
    }
}

fn cmd_log(run_id: &str, full: bool) -> routines_core::error::Result<()> {
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

/// Print text with optional line truncation.
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
                println!("  ... ({} more lines, use --full to show all)", lines.len() - max_lines);
                break;
            }
        }
    }
}

fn cmd_mcp(action: McpAction) -> routines_core::error::Result<()> {
    let rdir = routines_dir();

    match action {
        McpAction::List => {
            let config = McpConfig::load(&rdir)?;
            if config.servers.is_empty() {
                println!("No MCP servers configured.");
                println!("Use `routines mcp add <name> <command> [args...]` to add one.");
                return Ok(());
            }
            for (name, srv) in &config.servers {
                let args = if srv.args.is_empty() {
                    String::new()
                } else {
                    format!(" {}", srv.args.join(" "))
                };
                let env_count = if srv.env.is_empty() {
                    String::new()
                } else {
                    format!("  ({} env vars)", srv.env.len())
                };
                println!("  {name}: {}{args}{env_count}", srv.command);
            }
        }
        McpAction::Add {
            name,
            command,
            args,
            envs,
        } => {
            let env: HashMap<String, String> = envs
                .iter()
                .filter_map(|s| {
                    let (k, v) = s.split_once('=')?;
                    Some((k.to_string(), v.to_string()))
                })
                .collect();
            let mut config = McpConfig::load(&rdir)?;
            config.add(
                name.clone(),
                McpServerConfig {
                    command,
                    args,
                    env,
                },
            );
            config.save(&rdir)?;
            println!("Added MCP server '{name}'");
        }
        McpAction::Remove { name } => {
            let mut config = McpConfig::load(&rdir)?;
            if !config.remove(&name) {
                eprintln!("MCP server '{name}' not found");
                std::process::exit(1);
            }
            config.save(&rdir)?;
            println!("Removed MCP server '{name}'");
        }
        McpAction::Get { name } => {
            let config = McpConfig::load(&rdir)?;
            let Some(srv) = config.get(&name) else {
                eprintln!("MCP server '{name}' not found");
                std::process::exit(1);
            };

            println!("Name:    {name}");
            println!("Command: {}", srv.command);
            if !srv.args.is_empty() {
                println!("Args:    {}", srv.args.join(" "));
            }
            if !srv.env.is_empty() {
                let keys: Vec<&str> = srv.env.keys().map(|k| k.as_str()).collect();
                println!("Env:     {} (values hidden)", keys.join(", "));
            }

            // Test connection
            println!("---");
            print!("Testing connection... ");
            let secrets_map = secrets::load_secrets(&rdir.join(".env"));
            let resolved_env = McpConfig::resolve_env(srv, &secrets_map);
            let command = srv.command.clone();
            let srv_args = srv.args.clone();

            let rt = tokio::runtime::Runtime::new()?;
            match rt.block_on(test_mcp_connection(&command, &srv_args, &resolved_env)) {
                Ok(tools) => {
                    println!("OK");
                    println!("Tools ({}):", tools.len());
                    for t in &tools {
                        let desc = t
                            .description
                            .as_deref()
                            .unwrap_or("");
                        println!("  {} — {desc}", t.name);
                    }
                }
                Err(e) => {
                    println!("FAILED");
                    eprintln!("  {e}");
                    std::process::exit(1);
                }
            }
        }
    }

    Ok(())
}

async fn test_mcp_connection(
    command: &str,
    args: &[String],
    env: &HashMap<String, String>,
) -> std::result::Result<Vec<rmcp::model::Tool>, String> {
    use rmcp::ServiceExt;
    use rmcp::transport::TokioChildProcess;
    use tokio::process::Command;

    let mut cmd = Command::new(command);
    cmd.args(args);
    for (k, v) in env {
        cmd.env(k, v);
    }

    let transport =
        TokioChildProcess::new(cmd).map_err(|e| format!("Failed to spawn: {e}"))?;

    let client = tokio::time::timeout(std::time::Duration::from_secs(30), ().serve(transport))
        .await
        .map_err(|_| "Initialization timed out (30s)".to_string())?
        .map_err(|e| format!("Initialization failed: {e}"))?;

    let tools = tokio::time::timeout(std::time::Duration::from_secs(10), client.list_all_tools())
        .await
        .map_err(|_| "list_tools timed out (10s)".to_string())?
        .map_err(|e| format!("list_tools failed: {e}"))?;

    let _ = client.cancel().await;
    Ok(tools)
}

fn cmd_registry(action: RegistryAction) -> routines_core::error::Result<()> {
    let rdir = routines_dir();

    match action {
        RegistryAction::List => {
            let config = Registries::load(&rdir)?;
            if config.registries.is_empty() {
                println!("No registries configured.");
                println!("Use `routines registry add <name> <url>` to add one.");
                return Ok(());
            }
            for (name, reg) in &config.registries {
                let synced = if rdir.join("registries").join(name).join(".git").exists() {
                    "synced"
                } else {
                    "not synced"
                };
                println!("  @{name}: {} ({}) [{synced}]", reg.url, reg.git_ref);
            }
        }
        RegistryAction::Add { name, url, git_ref } => {
            let mut config = Registries::load(&rdir)?;
            config.add(name.clone(), RegistryConfig { url, git_ref });
            config.save(&rdir)?;
            println!("Added registry '@{name}'");
            println!("Run `routines registry sync {name}` to fetch routines.");
        }
        RegistryAction::Remove { name } => {
            let mut config = Registries::load(&rdir)?;
            if !config.remove(&name) {
                eprintln!("Registry '{name}' not found");
                std::process::exit(1);
            }
            config.save(&rdir)?;
            registry::remove_registry_files(&name, &rdir)?;
            println!("Removed registry '@{name}'");
        }
        RegistryAction::Sync { name } => {
            if let Some(name) = name {
                let config = Registries::load(&rdir)?;
                let Some(reg) = config.get(&name) else {
                    eprintln!("Registry '{name}' not found");
                    std::process::exit(1);
                };
                let msg = registry::sync_registry(&name, reg, &rdir)?;
                println!("{msg}");
            } else {
                let results = registry::sync_all(&rdir)?;
                if results.is_empty() {
                    println!("No registries to sync.");
                } else {
                    for msg in results {
                        println!("{msg}");
                    }
                }
            }
        }
    }

    Ok(())
}
