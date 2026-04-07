use crate::routines_dir;

pub fn cmd_list() -> routines_core::error::Result<()> {
    use colored::Colorize;
    use std::io::IsTerminal;

    let is_tty = std::io::stdout().is_terminal();
    let rdir = routines_dir();
    let hub_dir = rdir.join("hub");
    let mut has_required = false;
    let mut has_output = false;

    if hub_dir.exists() {
        let entries = routines_core::server::collect_routines_recursive(&hub_dir, "");
        for entry in &entries {
            match entry {
                routines_core::server::RoutineEntry::Ok(ref_name, routine) => {
                    let inputs_desc = format_inputs_cli(&routine.inputs, &mut has_required);
                    if is_tty {
                        println!(
                            "{:<20} — {}{inputs_desc}",
                            ref_name.bold(),
                            routine.description
                        );
                    } else {
                        println!("{ref_name} — {}{inputs_desc}", routine.description);
                    }
                }
                routines_core::server::RoutineEntry::Err(ref_name, err) => {
                    if is_tty {
                        println!(
                            "{:<20} — {}",
                            ref_name.bold(),
                            format!("[PARSE ERROR] {err}").red()
                        );
                    } else {
                        println!("{ref_name} — [PARSE ERROR] {err}");
                    }
                }
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
                for entry in &entries {
                    match entry {
                        routines_core::server::RoutineEntry::Ok(name, routine) => {
                            let inputs_desc =
                                format_inputs_cli(&routine.inputs, &mut has_required);
                            let full_name = format!("@{reg_name}/{name}");
                            if is_tty {
                                println!(
                                    "{:<20} — {}{inputs_desc}",
                                    full_name.bold(),
                                    routine.description
                                );
                            } else {
                                println!("{full_name} — {}{inputs_desc}", routine.description);
                            }
                        }
                        routines_core::server::RoutineEntry::Err(name, err) => {
                            let full_name = format!("@{reg_name}/{name}");
                            if is_tty {
                                println!(
                                    "{:<20} — {}",
                                    full_name.bold(),
                                    format!("[PARSE ERROR] {err}").red()
                                );
                            } else {
                                println!("{full_name} — [PARSE ERROR] {err}");
                            }
                        }
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

fn format_inputs_cli(
    inputs: &[routines_core::parser::InputDef],
    has_required: &mut bool,
) -> String {
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
