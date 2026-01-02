//! Stdio transport for the MCP server.
//!
//! This transport uses standard input/output for communication,
//! which is the standard mode for CLI-based MCP integrations.

use crate::db::{ConnectionManager, TransactionRegistry};
use crate::error::DbResult;
use crate::mcp::DbService;
use crate::transport::Transport;
use rmcp::{ServiceExt, transport::stdio};
use std::sync::Arc;
use tokio::signal;
use tracing::info;

/// Stdio transport implementation.
///
/// This transport reads JSON-RPC messages from stdin and writes
/// responses to stdout, following the MCP protocol specification.
pub struct StdioTransport {
    connection_manager: Arc<ConnectionManager>,
    transaction_registry: Arc<TransactionRegistry>,
}

impl StdioTransport {
    /// Create a new stdio transport with the given connection manager.
    ///
    /// # Arguments
    ///
    /// * `connection_manager` - Shared connection manager for database operations
    /// * `transaction_registry` - Shared transaction registry for transaction management
    pub fn new(
        connection_manager: Arc<ConnectionManager>,
        transaction_registry: Arc<TransactionRegistry>,
    ) -> Self {
        Self {
            connection_manager,
            transaction_registry,
        }
    }
}

impl Transport for StdioTransport {
    async fn run(&self) -> DbResult<()> {
        info!("Starting MCP server with stdio transport");

        // Create the DbService
        let service = DbService::new(
            self.connection_manager.clone(),
            self.transaction_registry.clone(),
        );

        // Create the stdio transport and run the service
        let transport = stdio();
        let running_service = service.serve(transport).await.map_err(|e| {
            crate::error::DbError::internal(format!("Failed to start stdio transport: {}", e))
        })?;

        let shutdown_requested = tokio::select! {
            result = running_service.waiting() => {
                match result {
                    Ok(_quit_reason) => {
                        info!("Stdio transport completed normally");
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "Stdio transport error");
                        return Err(crate::error::DbError::internal(format!(
                            "Stdio transport error: {}",
                            e
                        )));
                    }
                }
                false
            }
            _ = wait_for_signal() => {
                info!("Shutdown signal received (send again to force exit)");
                true
            }
        };

        if shutdown_requested {
            // Spawn a task to listen for second signal and force exit
            tokio::spawn(async {
                wait_for_signal().await;
                tracing::warn!("Received second signal, forcing immediate exit");
                std::process::exit(1);
            });
        }

        // Close all database connections on shutdown
        info!("Closing all database connections");
        self.connection_manager.close_all().await;

        if shutdown_requested {
            // Force exit since stdio may still be blocking on stdin
            // tokio::select! cannot interrupt blocking stdin reads
            info!("Exiting process");
            std::process::exit(0);
        }

        Ok(())
    }

    fn name(&self) -> &'static str {
        "stdio"
    }
}

/// Wait for a shutdown signal (SIGINT or SIGTERM).
async fn wait_for_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("Received SIGINT");
        }
        _ = terminate => {
            info!("Received SIGTERM");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stdio_transport_creation() {
        let manager = Arc::new(ConnectionManager::new());
        let registry = Arc::new(TransactionRegistry::new());
        let transport = StdioTransport::new(manager, registry);
        assert_eq!(transport.name(), "stdio");
    }
}
