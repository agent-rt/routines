use routines_protocol::types::default_socket_path;
use routines_protocol::DaemonClient;

use crate::DaemonAction;

pub fn cmd_daemon(action: DaemonAction) -> routines_core::error::Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        match action {
            DaemonAction::Start => daemon_start().await,
            DaemonAction::Stop => daemon_stop().await,
            DaemonAction::Status => daemon_status().await,
        }
    })
}

async fn daemon_start() -> routines_core::error::Result<()> {
    use colored::Colorize;

    // Check if already running
    if let Ok(Some(mut client)) = DaemonClient::try_connect().await
        && client.ping().await.is_ok()
    {
        println!("{} daemon already running", "●".green());
        return Ok(());
    }

    // Start by connecting (auto-fork)
    match DaemonClient::connect().await {
        Ok(mut client) => {
            if client.ping().await.is_ok() {
                println!("{} daemon started", "●".green());
            } else {
                eprintln!("daemon started but ping failed");
                std::process::exit(1);
            }
        }
        Err(e) => {
            eprintln!("failed to start daemon: {e}");
            std::process::exit(1);
        }
    }

    Ok(())
}

async fn daemon_stop() -> routines_core::error::Result<()> {
    use colored::Colorize;

    match DaemonClient::try_connect().await {
        Ok(Some(mut client)) => {
            let _ = client
                .request(&routines_protocol::types::Request::Shutdown)
                .await;
            println!("{} daemon stopped", "○".dimmed());
        }
        _ => {
            println!("{} daemon not running", "○".dimmed());
        }
    }

    // Clean up stale socket
    let sock = default_socket_path();
    if sock.exists() {
        std::fs::remove_file(&sock).ok();
    }

    Ok(())
}

async fn daemon_status() -> routines_core::error::Result<()> {
    use colored::Colorize;

    match DaemonClient::try_connect().await {
        Ok(Some(mut client)) => {
            if client.ping().await.is_ok() {
                let active = client.list_active().await.unwrap_or_default();
                println!(
                    "{} daemon running ({} active run{})",
                    "●".green(),
                    active.len(),
                    if active.len() == 1 { "" } else { "s" }
                );
            } else {
                println!("{} daemon not responding", "●".yellow());
            }
        }
        _ => {
            println!("{} daemon not running", "○".dimmed());
        }
    }

    Ok(())
}
