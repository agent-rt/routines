use std::path::{Path, PathBuf};

/// Resolve a routine name to a YAML file path.
///
/// Rules:
/// - `@registry/name` or `@registry/ns/name` → `registries/<registry>/<path>.yml`
/// - `namespace/name` → `hub/<namespace>/<name>.yml`
/// - `name` → `hub/<name>.yml` (backward compatible)
pub fn resolve_routine_path(name: &str, routines_dir: &Path) -> PathBuf {
    // Normalize colon separator to slash: "bilibili:hot" → "bilibili/hot"
    let name = if !name.starts_with('@') {
        std::borrow::Cow::Owned(name.replacen(':', "/", 1))
    } else {
        std::borrow::Cow::Borrowed(name)
    };
    if let Some(rest) = name.strip_prefix('@') {
        // Remote registry: @registry/path
        routines_dir.join("registries").join(format!("{rest}.yml"))
    } else if name.contains('/') {
        // Local namespace: namespace/name
        routines_dir.join("hub").join(format!("{name}.yml"))
    } else {
        // Root: plain name
        routines_dir.join("hub").join(format!("{name}.yml"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_plain_name() {
        let path = resolve_routine_path("greeter", Path::new("/home/user/.routines"));
        assert_eq!(path, PathBuf::from("/home/user/.routines/hub/greeter.yml"));
    }

    #[test]
    fn resolve_namespace() {
        let path = resolve_routine_path("deploy/frontend", Path::new("/home/user/.routines"));
        assert_eq!(
            path,
            PathBuf::from("/home/user/.routines/hub/deploy/frontend.yml")
        );
    }

    #[test]
    fn resolve_nested_namespace() {
        let path = resolve_routine_path("deploy/aws/ecs", Path::new("/home/user/.routines"));
        assert_eq!(
            path,
            PathBuf::from("/home/user/.routines/hub/deploy/aws/ecs.yml")
        );
    }

    #[test]
    fn resolve_colon_separator() {
        let path = resolve_routine_path("bilibili:hot", Path::new("/home/user/.routines"));
        assert_eq!(
            path,
            PathBuf::from("/home/user/.routines/hub/bilibili/hot.yml")
        );
    }

    #[test]
    fn resolve_registry() {
        let path = resolve_routine_path("@shared-ops/notify", Path::new("/home/user/.routines"));
        assert_eq!(
            path,
            PathBuf::from("/home/user/.routines/registries/shared-ops/notify.yml")
        );
    }

    #[test]
    fn resolve_registry_with_namespace() {
        let path = resolve_routine_path(
            "@shared-ops/deploy/frontend",
            Path::new("/home/user/.routines"),
        );
        assert_eq!(
            path,
            PathBuf::from("/home/user/.routines/registries/shared-ops/deploy/frontend.yml")
        );
    }
}
