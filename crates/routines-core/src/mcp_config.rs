use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{Result, RoutineError};

/// Configuration for all MCP servers.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct McpConfig {
    #[serde(default)]
    pub servers: HashMap<String, McpServerConfig>,
}

/// Configuration for a single MCP server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpServerConfig {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
}

impl McpConfig {
    /// Load MCP config from `<routines_dir>/mcp.json`.
    /// Returns empty config if file doesn't exist.
    pub fn load(routines_dir: &Path) -> Result<Self> {
        let path = routines_dir.join("mcp.json");
        if !path.exists() {
            return Ok(Self::default());
        }
        let content = std::fs::read_to_string(&path)?;
        let config: McpConfig =
            serde_json::from_str(&content).map_err(|e| RoutineError::McpConfig(e.to_string()))?;
        Ok(config)
    }

    /// Save MCP config to `<routines_dir>/mcp.json`.
    pub fn save(&self, routines_dir: &Path) -> Result<()> {
        std::fs::create_dir_all(routines_dir)?;
        let path = routines_dir.join("mcp.json");
        let content = serde_json::to_string_pretty(self)
            .map_err(|e| RoutineError::McpConfig(e.to_string()))?;
        std::fs::write(&path, content)?;
        Ok(())
    }

    /// Get a server config by name.
    pub fn get(&self, name: &str) -> Option<&McpServerConfig> {
        self.servers.get(name)
    }

    /// Add or update a server config.
    pub fn add(&mut self, name: String, config: McpServerConfig) {
        self.servers.insert(name, config);
    }

    /// Remove a server config. Returns true if it existed.
    pub fn remove(&mut self, name: &str) -> bool {
        self.servers.remove(name).is_some()
    }

    /// Resolve `{{ secrets.X }}` templates in server env values.
    pub fn resolve_env(
        config: &McpServerConfig,
        secrets: &HashMap<String, String>,
    ) -> HashMap<String, String> {
        config
            .env
            .iter()
            .map(|(k, v)| {
                let resolved = if v.contains("{{") {
                    let mut result = v.clone();
                    for (sk, sv) in secrets {
                        result = result.replace(&format!("{{{{ secrets.{sk} }}}}"), sv);
                    }
                    result
                } else {
                    v.clone()
                };
                (k.clone(), resolved)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_missing_file_returns_empty() {
        let config = McpConfig::load(Path::new("/nonexistent")).unwrap();
        assert!(config.servers.is_empty());
    }

    #[test]
    fn save_and_load_roundtrip() {
        let tmp = std::env::temp_dir().join("routines_mcp_config_test");
        std::fs::create_dir_all(&tmp).unwrap();

        let mut config = McpConfig::default();
        config.add(
            "test-server".to_string(),
            McpServerConfig {
                command: "npx".to_string(),
                args: vec!["-y".to_string(), "test-mcp".to_string()],
                env: HashMap::new(),
            },
        );
        config.save(&tmp).unwrap();

        let loaded = McpConfig::load(&tmp).unwrap();
        assert!(loaded.servers.contains_key("test-server"));
        assert_eq!(loaded.servers["test-server"].command, "npx");

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn add_remove_server() {
        let mut config = McpConfig::default();
        config.add(
            "s1".to_string(),
            McpServerConfig {
                command: "cmd".to_string(),
                args: vec![],
                env: HashMap::new(),
            },
        );
        assert!(config.get("s1").is_some());
        assert!(config.remove("s1"));
        assert!(config.get("s1").is_none());
        assert!(!config.remove("s1"));
    }

    #[test]
    fn resolve_env_templates() {
        let config = McpServerConfig {
            command: "cmd".to_string(),
            args: vec![],
            env: HashMap::from([
                ("TOKEN".to_string(), "{{ secrets.SLACK_TOKEN }}".to_string()),
                ("PLAIN".to_string(), "value".to_string()),
            ]),
        };
        let secrets = HashMap::from([("SLACK_TOKEN".to_string(), "xoxb-123".to_string())]);
        let resolved = McpConfig::resolve_env(&config, &secrets);
        assert_eq!(resolved["TOKEN"], "xoxb-123");
        assert_eq!(resolved["PLAIN"], "value");
    }
}
