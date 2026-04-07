use std::collections::HashMap;

use routines_core::mcp_config::{McpConfig, McpServerConfig};
use routines_core::secrets;

use crate::{McpAction, routines_dir};

pub fn cmd_mcp(action: McpAction) -> routines_core::error::Result<()> {
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
            config.add(name.clone(), McpServerConfig { command, args, env });
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
                        let desc = t.description.as_deref().unwrap_or("");
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

    let transport = TokioChildProcess::new(cmd).map_err(|e| format!("Failed to spawn: {e}"))?;

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
