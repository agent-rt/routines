// Re-export engine modules for backward compatibility
pub use routines_engine::context;
pub use routines_engine::error;
pub use routines_engine::executor;
pub use routines_engine::mcp_config;
pub use routines_engine::parser;
pub use routines_engine::resolve;
pub use routines_engine::secrets;
pub use routines_engine::testing;
pub use routines_engine::transform;

// Core-only modules (depend on DB or MCP server)
pub mod audit;
pub mod registry;
pub mod server;
