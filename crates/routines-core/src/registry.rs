use std::collections::HashMap;
use std::path::Path;
use std::process::Command;

use serde::{Deserialize, Serialize};

use crate::error::{Result, RoutineError};

/// Configuration for all registries.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Registries {
    #[serde(default)]
    pub registries: HashMap<String, RegistryConfig>,
}

/// Configuration for a single registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryConfig {
    pub url: String,
    #[serde(default = "default_ref")]
    #[serde(rename = "ref")]
    pub git_ref: String,
}

fn default_ref() -> String {
    "main".to_string()
}

impl Registries {
    /// Load registries config from `<routines_dir>/registries.json`.
    pub fn load(routines_dir: &Path) -> Result<Self> {
        let path = routines_dir.join("registries.json");
        if !path.exists() {
            return Ok(Self::default());
        }
        let content = std::fs::read_to_string(&path)?;
        let config: Registries = serde_json::from_str(&content)
            .map_err(|e| RoutineError::McpConfig(format!("registries.json: {e}")))?;
        Ok(config)
    }

    /// Save registries config to `<routines_dir>/registries.json`.
    pub fn save(&self, routines_dir: &Path) -> Result<()> {
        std::fs::create_dir_all(routines_dir)?;
        let path = routines_dir.join("registries.json");
        let content = serde_json::to_string_pretty(self)
            .map_err(|e| RoutineError::McpConfig(e.to_string()))?;
        std::fs::write(&path, content)?;
        Ok(())
    }

    /// Get a registry config by name.
    pub fn get(&self, name: &str) -> Option<&RegistryConfig> {
        self.registries.get(name)
    }

    /// Add or update a registry.
    pub fn add(&mut self, name: String, config: RegistryConfig) {
        self.registries.insert(name, config);
    }

    /// Remove a registry. Returns true if it existed.
    pub fn remove(&mut self, name: &str) -> bool {
        self.registries.remove(name).is_some()
    }
}

/// Sync a single registry (git clone or pull).
pub fn sync_registry(name: &str, config: &RegistryConfig, routines_dir: &Path) -> Result<String> {
    let reg_dir = routines_dir.join("registries").join(name);

    if reg_dir.join(".git").exists() {
        // Pull
        let output = Command::new("git")
            .args(["pull", "--ff-only"])
            .current_dir(&reg_dir)
            .output()?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(RoutineError::McpConfig(format!(
                "git pull failed for '{name}': {stderr}"
            )));
        }

        // Checkout ref
        let output = Command::new("git")
            .args(["checkout", &config.git_ref])
            .current_dir(&reg_dir)
            .output()?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(RoutineError::McpConfig(format!(
                "git checkout '{}' failed for '{name}': {stderr}",
                config.git_ref
            )));
        }

        Ok(format!("Updated '{name}' ({})", config.git_ref))
    } else {
        // Clone
        std::fs::create_dir_all(routines_dir.join("registries"))?;
        let output = Command::new("git")
            .args([
                "clone",
                "--branch",
                &config.git_ref,
                "--depth",
                "1",
                &config.url,
                &reg_dir.to_string_lossy(),
            ])
            .output()?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(RoutineError::McpConfig(format!(
                "git clone failed for '{name}': {stderr}"
            )));
        }

        Ok(format!("Cloned '{name}' from {} ({})", config.url, config.git_ref))
    }
}

/// Sync all registries.
pub fn sync_all(routines_dir: &Path) -> Result<Vec<String>> {
    let registries = Registries::load(routines_dir)?;
    let mut results = Vec::new();
    for (name, config) in &registries.registries {
        match sync_registry(name, config, routines_dir) {
            Ok(msg) => results.push(msg),
            Err(e) => results.push(format!("Error syncing '{name}': {e}")),
        }
    }
    Ok(results)
}

/// Remove a registry's local files.
pub fn remove_registry_files(name: &str, routines_dir: &Path) -> Result<()> {
    let reg_dir = routines_dir.join("registries").join(name);
    if reg_dir.exists() {
        std::fs::remove_dir_all(&reg_dir)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_missing_returns_empty() {
        let config = Registries::load(Path::new("/nonexistent")).unwrap();
        assert!(config.registries.is_empty());
    }

    #[test]
    fn save_and_load_roundtrip() {
        let tmp = std::env::temp_dir().join("routines_registry_test");
        std::fs::create_dir_all(&tmp).unwrap();

        let mut config = Registries::default();
        config.add(
            "test-reg".to_string(),
            RegistryConfig {
                url: "https://github.com/test/repo".to_string(),
                git_ref: "v1.0".to_string(),
            },
        );
        config.save(&tmp).unwrap();

        let loaded = Registries::load(&tmp).unwrap();
        assert!(loaded.registries.contains_key("test-reg"));
        assert_eq!(loaded.registries["test-reg"].url, "https://github.com/test/repo");
        assert_eq!(loaded.registries["test-reg"].git_ref, "v1.0");

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn add_remove() {
        let mut config = Registries::default();
        config.add(
            "r1".to_string(),
            RegistryConfig {
                url: "https://example.com".to_string(),
                git_ref: "main".to_string(),
            },
        );
        assert!(config.get("r1").is_some());
        assert!(config.remove("r1"));
        assert!(config.get("r1").is_none());
        assert!(!config.remove("r1"));
    }

    #[test]
    fn default_ref_is_main() {
        let json = r#"{"registries":{"x":{"url":"https://example.com"}}}"#;
        let config: Registries = serde_json::from_str(json).unwrap();
        assert_eq!(config.registries["x"].git_ref, "main");
    }
}
