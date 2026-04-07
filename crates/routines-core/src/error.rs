use thiserror::Error;

#[derive(Debug, Error)]
pub enum RoutineError {
    #[error("YAML parse error: {0}")]
    YamlParse(#[from] serde_yaml::Error),

    #[error("Template error: undefined variable '{key}' in step '{step_id}'")]
    UndefinedVariable { step_id: String, key: String },

    #[error("Step '{step_id}' failed with exit code {exit_code}")]
    StepFailed { step_id: String, exit_code: i32 },

    #[error("Step '{step_id}' was killed by signal")]
    StepKilled { step_id: String },

    #[error("Missing required input: {0}")]
    MissingInput(String),

    #[error("Referenced step '{0}' has not been executed yet")]
    StepNotExecuted(String),

    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("Dangerous command blocked in strict_mode: '{command}' in step '{step_id}'")]
    DangerousCommand { step_id: String, command: String },

    #[error("Invalid step dependency in '{step_id}': {reason}")]
    InvalidNeeds { step_id: String, reason: String },

    #[error("Cyclic dependency detected in step graph")]
    CyclicDependency,

    #[error("MCP config error: {0}")]
    McpConfig(String),

    #[error("Invalid input '{name}': expected {expected}, got '{got}'")]
    InvalidInput {
        name: String,
        expected: String,
        got: String,
    },

    #[error("Transform error in step '{step_id}': {message}")]
    Transform { step_id: String, message: String },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, RoutineError>;
