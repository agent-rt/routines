use routines_core::registry::{self, Registries, RegistryConfig};

use crate::{RegistryAction, routines_dir};

pub fn cmd_registry(action: RegistryAction) -> routines_core::error::Result<()> {
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
