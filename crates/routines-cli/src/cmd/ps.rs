use routines_protocol::DaemonClient;
use routines_protocol::types::RunState;

pub fn cmd_ps() -> routines_core::error::Result<()> {
    use colored::Colorize;

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let client = DaemonClient::try_connect().await;
        match client {
            Ok(Some(mut client)) => {
                let runs = client.list_active().await.map_err(|e| {
                    routines_core::error::RoutineError::Io(std::io::Error::other(e.to_string()))
                })?;

                if runs.is_empty() {
                    println!("No active runs");
                    return Ok(());
                }

                let mut table = comfy_table::Table::new();
                table.load_preset(comfy_table::presets::UTF8_FULL);
                table.set_header(vec!["Run ID", "Routine", "Status", "Progress"]);

                for run in &runs {
                    use comfy_table::{Cell, Color as TColor};
                    let status_cell = match run.status {
                        RunState::Running => Cell::new("Running").fg(TColor::Green),
                        RunState::Queued => Cell::new("Queued").fg(TColor::Yellow),
                        _ => Cell::new(format!("{:?}", run.status)),
                    };
                    let progress = format!(
                        "{}/{}",
                        run.steps_completed, run.steps_total
                    );
                    table.add_row(vec![
                        Cell::new(&run.run_id[..8]),
                        Cell::new(&run.routine),
                        status_cell,
                        Cell::new(progress),
                    ]);
                }
                println!("{table}");
            }
            _ => {
                println!("{} daemon not running", "○".dimmed());
                println!("Start with: routines daemon start");
            }
        }
        Ok(())
    })
}
