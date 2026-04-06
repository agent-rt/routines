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
        Commands::Run { name, inputs } => cmd_run(&name, &inputs),
        Commands::Serve => cmd_serve(),
        Commands::Log { run_id } => cmd_log(&run_id),
        Commands::Mcp { action } => cmd_mcp(action),
        Commands::Registry { action } => cmd_registry(action),
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
    let yaml_path = resolve_routine_path(name, &routines_dir());
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
