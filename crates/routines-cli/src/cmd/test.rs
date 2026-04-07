use crate::routines_dir;

pub fn cmd_test(
    file: Option<&std::path::Path>,
    all: bool,
) -> routines_core::error::Result<()> {
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
        eprintln!("{} {} passed", "✓".green(), total_pass);
    } else {
        eprintln!("{} {} passed, {} failed", "✗".red(), total_pass, total_fail);
        std::process::exit(1);
    }

    Ok(())
}
