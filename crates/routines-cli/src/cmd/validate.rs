use routines_core::parser::Routine;

pub fn cmd_validate(file: &std::path::Path) -> routines_core::error::Result<()> {
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
