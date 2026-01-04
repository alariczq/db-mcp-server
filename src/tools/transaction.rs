//! Transaction management tools.
//!
//! This module implements MCP tools for transaction management:
//! - `begin_transaction`: Start a new transaction
//! - `commit`: Commit an active transaction
//! - `rollback`: Rollback an active transaction
//! - `list_transactions`: List all active transactions
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
    /// Database connection ID from list_connections
    pub connection_id: String,
    /// Transaction timeout in seconds. Auto-rollback if exceeded. Default: 60, max: 300
    #[serde(default)]
    pub timeout_secs: Option<u32>,
    /// Target database for transaction. Optional for server-level connections - omit for server-level transactions. Specify to create transaction on a specific database.
    #[serde(default)]
    pub database: Option<String>,
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

/// Information about an active transaction.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct TransactionInfo {
    /// Unique transaction identifier
    pub transaction_id: String,
    /// Database connection this transaction belongs to
    pub connection_id: String,
    /// ISO8601 timestamp when transaction started
    pub started_at: String,
    /// Seconds since transaction started
    pub duration_secs: u64,
    /// Configured timeout for this transaction
    pub timeout_secs: u32,
    /// True if duration exceeds 5 minutes (300 seconds)
    pub is_long_running: bool,
}

/// Threshold in seconds for marking a transaction as long-running.
pub const LONG_RUNNING_THRESHOLD_SECS: u64 = 300;

/// Input for the list_transactions tool.
#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
pub struct ListTransactionsInput {
    /// Filter by connection ID. If not specified, returns transactions from all connections.
    #[serde(default)]
    pub connection_id: Option<String>,
}

/// Output from the list_transactions tool.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct ListTransactionsOutput {
    /// List of active transactions
    pub transactions: Vec<TransactionInfo>,
    /// Number of transactions returned
    pub count: usize,
    /// Optional informational message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

pub struct TransactionToolHandler {
    connection_manager: Arc<ConnectionManager>,
    transaction_registry: Arc<TransactionRegistry>,
}

impl TransactionToolHandler {
    pub fn new(
        connection_manager: Arc<ConnectionManager>,
        transaction_registry: Arc<TransactionRegistry>,
    ) -> Self {
        Self {
            connection_manager,
            transaction_registry,
        }
    }

    pub async fn begin_transaction(
        &self,
        input: BeginTransactionInput,
    ) -> DbResult<BeginTransactionOutput> {
        // Note: Readonly connections can begin transactions (read-only transactions)
        // The writable check will be performed when executing write operations within the transaction

        let timeout_secs = input
            .timeout_secs
            .map(|t| t.min(MAX_TRANSACTION_TIMEOUT_SECS))
            .unwrap_or(DEFAULT_TRANSACTION_TIMEOUT_SECS);

        let database = input.database.as_deref();
        let pool = self
            .connection_manager
            .get_pool_for_database(&input.connection_id, database)
            .await?;

        let result = match pool {
            DbPool::MySql(ref p) => {
                self.transaction_registry
                    .begin_mysql(p, input.connection_id.clone(), Some(timeout_secs))
                    .await
            }
            DbPool::Postgres(ref p) => {
                self.transaction_registry
                    .begin_postgres(p, input.connection_id.clone(), Some(timeout_secs))
                    .await
            }
            DbPool::SQLite(ref p) => {
                self.transaction_registry
                    .begin_sqlite(p, input.connection_id.clone(), Some(timeout_secs))
                    .await
            }
        };

        self.connection_manager
            .release_pool_for_database(&input.connection_id, database)
            .await;

        let transaction_id = result?;

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

    pub async fn list_transactions(
        &self,
        input: ListTransactionsInput,
    ) -> DbResult<ListTransactionsOutput> {
        let all_transactions = self.transaction_registry.list_all().await;
        let mut transactions: Vec<TransactionInfo> = all_transactions
            .into_iter()
            .map(|meta| TransactionInfo {
                transaction_id: meta.transaction_id,
                connection_id: meta.connection_id,
                started_at: meta.started_at.to_rfc3339(),
                duration_secs: meta.duration_secs,
                timeout_secs: meta.timeout_secs,
                is_long_running: meta.duration_secs >= LONG_RUNNING_THRESHOLD_SECS,
            })
            .collect();

        if let Some(ref filter_conn_id) = input.connection_id {
            if !self.connection_manager.exists(filter_conn_id).await {
                return Err(DbError::connection_not_found(filter_conn_id));
            }
            transactions.retain(|t| &t.connection_id == filter_conn_id);
        }

        let count = transactions.len();
        let message = if count == 0 {
            Some("No active transactions".to_string())
        } else {
            None
        };

        Ok(ListTransactionsOutput {
            transactions,
            count,
            message,
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
