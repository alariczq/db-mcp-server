//! Transaction management tools.
//!
//! This module implements MCP tools for transaction management:
//! - `begin_transaction`: Start a new transaction
//! - `commit`: Commit an active transaction
//! - `rollback`: Rollback an active transaction
//!
//! Transactions are managed by the TransactionRegistry, which maintains
//! transaction state across multiple tool calls.

use crate::db::transaction_registry::{
    DEFAULT_TRANSACTION_TIMEOUT_SECS, MAX_TRANSACTION_TIMEOUT_SECS,
};
use crate::db::{ConnectionManager, DbPool, TransactionRegistry};
use crate::error::{DbError, DbResult};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::info;

/// Input for the begin_transaction tool.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct BeginTransactionInput {
    /// Database connection ID from list_connections. Must be a read-write connection (read_only: false).
    pub connection_id: String,
    /// Transaction timeout in seconds. Auto-rollback if exceeded. Default: 60, max: 300
    #[serde(default)]
    pub timeout_secs: Option<u32>,
}

/// Output from the begin_transaction tool.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct BeginTransactionOutput {
    /// Unique transaction ID to use with query/execute/commit/rollback
    pub transaction_id: String,
    /// Effective timeout in seconds
    pub timeout_secs: u32,
    /// Human-readable status message
    pub message: String,
}

/// Input for the commit tool.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct CommitInput {
    /// Database connection ID from list_connections
    pub connection_id: String,
    /// Transaction ID from begin_transaction
    pub transaction_id: String,
}

/// Output from the commit tool.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct CommitOutput {
    /// Whether the commit succeeded
    pub success: bool,
    /// The committed transaction ID
    pub transaction_id: String,
    /// Human-readable status message
    pub message: String,
}

/// Input for the rollback tool.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct RollbackInput {
    /// Database connection ID from list_connections
    pub connection_id: String,
    /// Transaction ID from begin_transaction
    pub transaction_id: String,
}

/// Output from the rollback tool.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct RollbackOutput {
    /// Whether the rollback succeeded
    pub success: bool,
    /// The rolled-back transaction ID
    pub transaction_id: String,
    /// Human-readable status message
    pub message: String,
}

/// Handler for transaction management tools.
pub struct TransactionToolHandler {
    connection_manager: Arc<ConnectionManager>,
    transaction_registry: Arc<TransactionRegistry>,
}

impl TransactionToolHandler {
    /// Create a new transaction tool handler.
    pub fn new(
        connection_manager: Arc<ConnectionManager>,
        transaction_registry: Arc<TransactionRegistry>,
    ) -> Self {
        Self {
            connection_manager,
            transaction_registry,
        }
    }

    /// Handle the begin_transaction tool call.
    pub async fn begin_transaction(
        &self,
        input: BeginTransactionInput,
    ) -> DbResult<BeginTransactionOutput> {
        // Check if connection exists and allows writes
        let is_writable = self
            .connection_manager
            .is_writable(&input.connection_id)
            .await?;

        if !is_writable {
            return Err(DbError::permission(
                "begin transaction",
                "Cannot start transaction: connection is not writable. Use ?writable=true in the connection URL",
            ));
        }

        // Calculate effective timeout
        let timeout_secs = input
            .timeout_secs
            .map(|t| t.min(MAX_TRANSACTION_TIMEOUT_SECS))
            .unwrap_or(DEFAULT_TRANSACTION_TIMEOUT_SECS);

        // Get the pool and start a transaction
        let pool = self
            .connection_manager
            .get_pool(&input.connection_id)
            .await?;

        let transaction_id = match pool {
            DbPool::MySql(ref p) => {
                self.transaction_registry
                    .begin_mysql(p, input.connection_id.clone(), Some(timeout_secs))
                    .await?
            }
            DbPool::Postgres(ref p) => {
                self.transaction_registry
                    .begin_postgres(p, input.connection_id.clone(), Some(timeout_secs))
                    .await?
            }
            DbPool::SQLite(ref p) => {
                self.transaction_registry
                    .begin_sqlite(p, input.connection_id.clone(), Some(timeout_secs))
                    .await?
            }
        };

        info!(
            connection_id = %input.connection_id,
            transaction_id = %transaction_id,
            timeout_secs = timeout_secs,
            "Transaction started"
        );

        Ok(BeginTransactionOutput {
            transaction_id,
            timeout_secs,
            message: format!(
                "Transaction started. Use this transaction_id for subsequent operations. \
                 Auto-rollback after {}s of inactivity.",
                timeout_secs
            ),
        })
    }

    /// Handle the commit tool call.
    pub async fn commit(&self, input: CommitInput) -> DbResult<CommitOutput> {
        self.transaction_registry
            .commit(&input.transaction_id, &input.connection_id)
            .await?;

        info!(
            transaction_id = %input.transaction_id,
            connection_id = %input.connection_id,
            "Transaction committed"
        );

        Ok(CommitOutput {
            success: true,
            transaction_id: input.transaction_id,
            message: "Transaction committed successfully".to_string(),
        })
    }

    /// Handle the rollback tool call.
    pub async fn rollback(&self, input: RollbackInput) -> DbResult<RollbackOutput> {
        self.transaction_registry
            .rollback(&input.transaction_id, &input.connection_id)
            .await?;

        info!(
            transaction_id = %input.transaction_id,
            connection_id = %input.connection_id,
            "Transaction rolled back"
        );

        Ok(RollbackOutput {
            success: true,
            transaction_id: input.transaction_id,
            message: "Transaction rolled back successfully".to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_begin_transaction_input_defaults() {
        let json = r#"{"connection_id": "conn1"}"#;
        let input: BeginTransactionInput = serde_json::from_str(json).unwrap();

        assert_eq!(input.connection_id, "conn1");
        assert!(input.timeout_secs.is_none());
    }

    #[test]
    fn test_begin_transaction_input_with_timeout() {
        let json = r#"{"connection_id": "conn1", "timeout_secs": 120}"#;
        let input: BeginTransactionInput = serde_json::from_str(json).unwrap();

        assert_eq!(input.timeout_secs, Some(120));
    }

    #[test]
    fn test_commit_input() {
        let json = r#"{"connection_id": "conn1", "transaction_id": "tx_123"}"#;
        let input: CommitInput = serde_json::from_str(json).unwrap();

        assert_eq!(input.connection_id, "conn1");
        assert_eq!(input.transaction_id, "tx_123");
    }

    #[test]
    fn test_rollback_input() {
        let json = r#"{"connection_id": "conn1", "transaction_id": "tx_456"}"#;
        let input: RollbackInput = serde_json::from_str(json).unwrap();

        assert_eq!(input.connection_id, "conn1");
        assert_eq!(input.transaction_id, "tx_456");
    }

    #[test]
    fn test_output_serialization() {
        let output = CommitOutput {
            success: true,
            transaction_id: "tx_123".to_string(),
            message: "Done".to_string(),
        };

        let json = serde_json::to_string(&output).unwrap();
        assert!(json.contains("\"success\":true"));
        assert!(json.contains("\"transaction_id\":\"tx_123\""));
    }
}
