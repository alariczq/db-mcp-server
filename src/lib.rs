//! DB MCP Server Library
//!
//! This library provides MCP (Model Context Protocol) tools for AI assistants
//! to interact with SQL databases (SQLite, PostgreSQL, MySQL).

pub mod auth;
pub mod config;
pub mod db;
pub mod error;
pub mod mcp;
pub mod models;
pub mod tools;
pub mod transport;

pub use config::Config;
pub use error::DbError;
pub use mcp::DbService;
