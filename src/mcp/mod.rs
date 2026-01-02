//! MCP server integration module.
//!
//! This module provides the integration between the MCP protocol and
//! the database tool handlers using the rmcp framework.

pub mod service;

pub use service::DbService;
