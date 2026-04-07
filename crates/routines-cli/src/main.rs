use std::collections::HashMap;
use std::path::PathBuf;

use clap::{Parser, Subcommand};

mod cmd;

fn routines_dir() -> PathBuf {
    std::env::var("ROUTINES_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| dirs_home().join(".routines"))
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
        /// Submit to daemon and return immediately with run_id
        #[arg(short, long)]
        detach: bool,
        /// Input key=value pairs
        #[arg(trailing_var_arg = true)]
        inputs: Vec<String>,
    },
    /// Start MCP server (stdio transport)
    #[command(name = "mcp")]
    McpServe,
    /// Show audit log for a routine run
    Log {
        /// Run ID (UUID) to display
        run_id: String,
        /// Show full stdout/stderr without truncation
        #[arg(long)]
        full: bool,
    },
    /// Manage MCP server configurations
    #[command(name = "mcp-config")]
    McpConfig {
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
    /// Show active runs in the daemon
    Ps,
    /// Attach to a running routine and stream output
    Attach {
        /// Run ID (or prefix)
        run_id: String,
    },
    /// Interactive terminal UI
    Tui,
    /// Manage the background daemon
    Daemon {
        #[command(subcommand)]
        action: DaemonAction,
    },
}

#[derive(Subcommand)]
pub enum McpAction {
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
pub enum RegistryAction {
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

#[derive(Subcommand)]
pub enum DaemonAction {
    /// Start the background daemon
    Start,
    /// Stop the background daemon
    Stop,
    /// Show daemon status
    Status,
    /// Run the daemon process (internal, do not call directly)
    #[command(hide = true)]
    Run,
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
            detach,
            inputs,
        } => cmd::run::cmd_run(&name, &inputs, quiet, verbose, detach),
        Commands::McpServe => cmd::serve::cmd_serve(),
        Commands::Log { run_id, full } => cmd::log::cmd_log(&run_id, full),
        Commands::McpConfig { action } => cmd::mcp::cmd_mcp(action),
        Commands::Registry { action } => cmd::registry::cmd_registry(action),
        Commands::List => cmd::list::cmd_list(),
        Commands::Validate { file } => cmd::validate::cmd_validate(&file),
        Commands::Test { file, all } => cmd::test::cmd_test(file.as_deref(), all),
        Commands::Tui => cmd::tui::cmd_tui(),
        Commands::Ps => cmd::ps::cmd_ps(),
        Commands::Attach { run_id } => cmd::attach::cmd_attach(&run_id),
        Commands::Daemon { action } => cmd::daemon::cmd_daemon(action),
    }
}

// Shared input prompting logic used by cmd::run
fn prompt_missing_inputs(
    inputs_def: &[routines_core::parser::InputDef],
    provided: &HashMap<String, String>,
) -> routines_core::error::Result<HashMap<String, String>> {
    use routines_core::parser::InputType;

    let mut result = provided.clone();

    let has_missing_required = inputs_def
        .iter()
        .any(|i| i.required && !result.contains_key(&i.name));

    if !has_missing_required {
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
                    Err(
                        inquire::InquireError::OperationCanceled
                        | inquire::InquireError::OperationInterrupted,
                    ) => {
                        std::process::exit(130);
                    }
                    Err(e) => {
                        return Err(routines_core::error::RoutineError::Io(
                            std::io::Error::other(e.to_string()),
                        ));
                    }
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
                    Err(
                        inquire::InquireError::OperationCanceled
                        | inquire::InquireError::OperationInterrupted,
                    ) => {
                        std::process::exit(130);
                    }
                    Err(e) => {
                        return Err(routines_core::error::RoutineError::Io(
                            std::io::Error::other(e.to_string()),
                        ));
                    }
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
                                Ok(inquire::validator::Validation::Invalid(
                                    "Must be an integer".into(),
                                ))
                            }
                        })
                        .prompt(),
                    InputType::Float => prompt
                        .with_validator(|input: &str| {
                            if input.is_empty() || input.parse::<f64>().is_ok() {
                                Ok(inquire::validator::Validation::Valid)
                            } else {
                                Ok(inquire::validator::Validation::Invalid(
                                    "Must be a number".into(),
                                ))
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
                                Ok(inquire::validator::Validation::Invalid(
                                    "Must be YYYY-MM-DD format".into(),
                                ))
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
                    Err(
                        inquire::InquireError::OperationCanceled
                        | inquire::InquireError::OperationInterrupted,
                    ) => {
                        std::process::exit(130);
                    }
                    Err(e) => {
                        return Err(routines_core::error::RoutineError::Io(
                            std::io::Error::other(e.to_string()),
                        ));
                    }
                }
            }
        };

        if let Some(v) = value {
            result.insert(input.name.clone(), v);
        }
    }

    Ok(result)
}
