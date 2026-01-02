//! Transport layer for the MCP server.
//!
//! This module provides different transport implementations for the MCP protocol:
//! - Stdio: Standard input/output for CLI integration
//! - HTTP: HTTP with Server-Sent Events for web clients

pub mod http;
pub mod stdio;

pub use http::HttpTransport;
pub use stdio::StdioTransport;

use crate::error::DbResult;
use std::future::Future;

/// Trait for MCP transport implementations.
///
/// Transports handle the low-level communication between the MCP server
/// and clients, abstracting away the protocol details.
pub trait Transport: Send + Sync {
    /// Start the transport and begin handling requests.
    ///
    /// This method should block until the transport is shut down.
    fn run(&self) -> impl Future<Output = DbResult<()>> + Send;

    /// Get the name of this transport for logging.
    fn name(&self) -> &'static str;
}
