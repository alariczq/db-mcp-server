//! HTTP transport with Streamable HTTP support for the MCP server.
//!
//! This transport uses HTTP with SSE streaming responses,
//! which is suitable for web-based MCP integrations.

use crate::db::{ConnectionManager, TransactionRegistry};
use crate::error::DbResult;
use crate::mcp::DbService;
use crate::transport::Transport;
use rmcp::transport::streamable_http_server::{
    StreamableHttpService, session::local::LocalSessionManager,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::signal;
use tracing::{error, info, warn};

/// HTTP transport implementation with Streamable HTTP support.
///
/// This transport provides:
/// - HTTP endpoints for MCP protocol messages
/// - Server-Sent Events for streaming responses
/// - Session management for stateful connections
pub struct HttpTransport {
    connection_manager: Arc<ConnectionManager>,
    transaction_registry: Arc<TransactionRegistry>,
    /// Host to bind to
    host: String,
    /// Port to bind to
    port: u16,
    /// MCP endpoint path
    endpoint: String,
}

impl HttpTransport {
    /// Create a new HTTP transport.
    ///
    /// # Arguments
    ///
    /// * `connection_manager` - Shared connection manager for database operations
    /// * `transaction_registry` - Shared transaction registry for transaction management
    /// * `host` - Host address to bind to
    /// * `port` - Port to bind to
    /// * `endpoint` - MCP endpoint path (e.g., "/mcp")
    pub fn new(
        connection_manager: Arc<ConnectionManager>,
        transaction_registry: Arc<TransactionRegistry>,
        host: impl Into<String>,
        port: u16,
        endpoint: impl Into<String>,
    ) -> Self {
        Self {
            connection_manager,
            transaction_registry,
            host: host.into(),
            port,
            endpoint: endpoint.into(),
        }
    }

    /// Get the bind address.
    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// Get the MCP endpoint path.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

impl Transport for HttpTransport {
    async fn run(&self) -> DbResult<()> {
        let bind_addr = self.bind_addr();
        info!("Starting MCP server with HTTP transport on {}", bind_addr);

        // Clone Arc references for the service factory closure
        let connection_manager = self.connection_manager.clone();
        let transaction_registry = self.transaction_registry.clone();

        // Create the StreamableHttpService with a factory that creates DbService instances
        let service = StreamableHttpService::new(
            move || {
                Ok(DbService::new(
                    connection_manager.clone(),
                    transaction_registry.clone(),
                ))
            },
            LocalSessionManager::default().into(),
            Default::default(),
        );

        // Build the axum router with configurable endpoint
        // Note: nest_service doesn't support root path "/", use fallback_service instead
        let app = if self.endpoint == "/" {
            axum::Router::new().fallback_service(service)
        } else {
            axum::Router::new().nest_service(&self.endpoint, service)
        };

        // Create TCP listener
        let listener = TcpListener::bind(&bind_addr).await.map_err(|e| {
            crate::error::DbError::connection(
                format!("Failed to bind to {}: {}", bind_addr, e),
                "Check that the port is available",
            )
        })?;

        info!(endpoint = %self.endpoint, "MCP endpoint ready");

        // Graceful shutdown: SSE connections may keep the server alive indefinitely,
        // so we force exit after a timeout once shutdown signal is received
        const GRACEFUL_TIMEOUT: Duration = Duration::from_secs(30);

        // Use a notify to coordinate shutdown timing
        let shutdown_notify = Arc::new(tokio::sync::Notify::new());
        let shutdown_notify_clone = shutdown_notify.clone();

        // Create shutdown signal that triggers on SIGINT or SIGTERM
        let shutdown_signal = async move {
            wait_for_signal().await;
            // Notify that shutdown was triggered
            shutdown_notify_clone.notify_one();
        };

        // Start server with graceful shutdown
        let server = axum::serve(listener, app).with_graceful_shutdown(shutdown_signal);

        // Race between: server completing normally vs forced timeout/second signal after shutdown
        tokio::select! {
            result = server => {
                match result {
                    Ok(()) => info!("HTTP server stopped"),
                    Err(e) => {
                        error!(error = %e, "HTTP server error");
                        return Err(crate::error::DbError::internal(format!(
                            "HTTP server error: {}",
                            e
                        )));
                    }
                }
            }
            _ = async {
                // Wait for shutdown signal, then wait for either timeout or second signal
                shutdown_notify.notified().await;
                info!(
                    timeout_secs = GRACEFUL_TIMEOUT.as_secs(),
                    "Waiting for connections to close (send signal again to force exit)..."
                );

                tokio::select! {
                    _ = tokio::time::sleep(GRACEFUL_TIMEOUT) => {
                        warn!("Graceful shutdown timeout, forcing exit");
                    }
                    _ = wait_for_signal() => {
                        warn!("Received second signal, forcing immediate exit");
                    }
                }
            } => {
                // Timeout or second signal reached - server will be dropped
            }
        }

        // Close database connections
        info!("Closing database connections");
        self.connection_manager.close_all().await;

        Ok(())
    }

    fn name(&self) -> &'static str {
        "http"
    }
}

/// Wait for a shutdown signal (SIGINT or SIGTERM).
async fn wait_for_signal() {
    let ctrl_c = signal::ctrl_c();

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("Received SIGINT"),
        _ = terminate => info!("Received SIGTERM"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_transport_creation() {
        let manager = Arc::new(ConnectionManager::new());
        let registry = Arc::new(TransactionRegistry::new());
        let transport = HttpTransport::new(manager, registry, "127.0.0.1", 8080, "/mcp");
        assert_eq!(transport.name(), "http");
        assert_eq!(transport.bind_addr(), "127.0.0.1:8080");
    }

    #[test]
    fn test_http_transport_bind_addr() {
        let manager = Arc::new(ConnectionManager::new());
        let registry = Arc::new(TransactionRegistry::new());
        let transport = HttpTransport::new(manager, registry, "0.0.0.0", 3000, "/api/mcp");
        assert_eq!(transport.bind_addr(), "0.0.0.0:3000");
    }

    #[test]
    fn test_http_transport_custom_endpoint() {
        let manager = Arc::new(ConnectionManager::new());
        let registry = Arc::new(TransactionRegistry::new());
        let transport = HttpTransport::new(manager, registry, "127.0.0.1", 8080, "/custom/path");
        assert_eq!(transport.endpoint(), "/custom/path");
    }

    #[test]
    fn test_http_transport_root_endpoint() {
        let manager = Arc::new(ConnectionManager::new());
        let registry = Arc::new(TransactionRegistry::new());
        let transport = HttpTransport::new(manager, registry, "127.0.0.1", 8080, "/");
        assert_eq!(transport.endpoint(), "/");
    }
}
