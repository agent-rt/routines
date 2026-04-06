use std::collections::HashMap;
use std::path::Path;

const REDACTED: &str = "[REDACTED]";

/// Load secrets from a .env file into a HashMap.
/// Returns empty map if file doesn't exist.
pub fn load_secrets(env_path: &Path) -> HashMap<String, String> {
    if !env_path.exists() {
        return HashMap::new();
    }
    dotenvy::from_path_iter(env_path)
        .map(|iter| {
            iter.filter_map(|r| r.ok())
                .collect::<HashMap<String, String>>()
        })
        .unwrap_or_default()
}

/// Replace all secret values in a string with [REDACTED].
/// Processes longer secrets first to avoid partial matches.
pub fn redact(text: &str, secret_values: &[&str]) -> String {
    let mut sorted: Vec<&str> = secret_values.to_vec();
    sorted.sort_by_key(|s| std::cmp::Reverse(s.len()));

    let mut result = text.to_string();
    for secret in sorted {
        if !secret.is_empty() {
            result = result.replace(secret, REDACTED);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_secrets() {
        let text = "key=AKIAIOSFODNN7EXAMPLE secret=wJalrXUtnFEMI";
        let secrets = vec!["AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI"];
        let redacted = redact(text, &secrets);
        assert_eq!(redacted, "key=[REDACTED] secret=[REDACTED]");
        assert!(!redacted.contains("AKIA"));
    }

    #[test]
    fn redact_empty_secrets() {
        let text = "nothing to redact";
        let redacted = redact(text, &[]);
        assert_eq!(redacted, text);
    }

    #[test]
    fn redact_overlapping_secrets() {
        // Longer secret should be redacted first
        let text = "token=abc123456";
        let secrets = vec!["abc", "abc123456"];
        let redacted = redact(text, &secrets);
        assert_eq!(redacted, "token=[REDACTED]");
    }
}
