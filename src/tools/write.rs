//! Write operation tools.
//!
//! This module implements the `execute` MCP tool for executing
//! INSERT, UPDATE, and DELETE operations.

use crate::db::{ConnectionManager, QueryExecutor, TransactionRegistry};
use crate::error::{DbError, DbResult};
use crate::models::{QueryParam, QueryParamInput};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

/// Input for the execute tool.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct ExecuteInput {
    /// Database connection ID from list_connections. Must be a read-write connection (read_only: false).
    pub connection_id: String,
    /// SQL statement to execute (INSERT, UPDATE, DELETE, or DDL like CREATE/DROP/ALTER/TRUNCATE)
    pub sql: String,
    /// Positional parameters for parameterized queries (use ? or $1,$2... placeholders in SQL)
    #[serde(default)]
    pub params: Vec<QueryParamInput>,
    /// Execution timeout in seconds. Default: 30
    #[serde(default)]
    pub timeout_secs: Option<u32>,
    /// Run within an existing transaction (from begin_transaction). Omit for auto-commit.
    #[serde(default)]
    pub transaction_id: Option<String>,
}

/// Output from the execute tool.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct ExecuteOutput {
    /// Number of rows affected by the operation
    pub rows_affected: u64,
    /// Execution time in milliseconds
    pub execution_time_ms: u64,
    /// Warning for dangerous operations (DROP, TRUNCATE, DELETE without WHERE)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warning: Option<String>,
}

/// Handler for write operations.
pub struct WriteToolHandler {
    connection_manager: Arc<ConnectionManager>,
    transaction_registry: Arc<TransactionRegistry>,
    executor: QueryExecutor,
}

impl WriteToolHandler {
    /// Create a new write tool handler.
    pub fn new(
        connection_manager: Arc<ConnectionManager>,
        transaction_registry: Arc<TransactionRegistry>,
    ) -> Self {
        Self {
            connection_manager,
            transaction_registry,
            executor: QueryExecutor::new(),
        }
    }

    /// Handle the execute tool call.
    pub async fn execute(&self, input: ExecuteInput) -> DbResult<ExecuteOutput> {
        // Check if connection allows writes
        let is_writable = self
            .connection_manager
            .is_writable(&input.connection_id)
            .await?;

        if !is_writable {
            return Err(DbError::permission(
                "write operation",
                "Connection is not writable. Use ?writable=true in the connection URL to enable writes",
            ));
        }

        // Check for potentially dangerous operations
        let warning = self.check_dangerous_operation(&input.sql);

        // Convert params
        let params: Vec<QueryParam> = input.params.into_iter().map(Into::into).collect();

        // If transaction_id is provided, execute within that transaction
        if let Some(ref tx_id) = input.transaction_id {
            let start = std::time::Instant::now();
            let rows_affected = self
                .transaction_registry
                .execute_in_transaction(tx_id, &input.connection_id, &input.sql, &params)
                .await?;
            let execution_time_ms = start.elapsed().as_millis() as u64;

            info!(
                connection_id = %input.connection_id,
                transaction_id = %tx_id,
                rows_affected = rows_affected,
                execution_time_ms = execution_time_ms,
                "Write operation executed in transaction"
            );

            return Ok(ExecuteOutput {
                rows_affected,
                execution_time_ms,
                warning,
            });
        }

        // Get the connection pool for non-transactional execution
        let pool = self
            .connection_manager
            .get_pool(&input.connection_id)
            .await?;

        // Calculate timeout
        let timeout = input.timeout_secs.map(|t| Duration::from_secs(t as u64));

        // Execute the operation
        let (rows_affected, execution_time_ms) = self
            .executor
            .execute_write(&pool, &input.sql, &params, timeout)
            .await?;

        info!(
            connection_id = %input.connection_id,
            rows_affected = rows_affected,
            execution_time_ms = execution_time_ms,
            "Write operation executed"
        );

        Ok(ExecuteOutput {
            rows_affected,
            execution_time_ms,
            warning,
        })
    }

    /// Check for potentially dangerous operations and return a warning if needed.
    fn check_dangerous_operation(&self, sql: &str) -> Option<String> {
        let sql_upper = sql.to_uppercase();
        let sql_trimmed = sql_upper.trim();

        // Check for DELETE without WHERE
        if sql_trimmed.starts_with("DELETE") && !sql_upper.contains("WHERE") {
            warn!("DELETE without WHERE clause detected");
            return Some("Warning: DELETE without WHERE clause will affect all rows".to_string());
        }

        // Check for UPDATE without WHERE
        if sql_trimmed.starts_with("UPDATE") && !sql_upper.contains("WHERE") {
            warn!("UPDATE without WHERE clause detected");
            return Some("Warning: UPDATE without WHERE clause will affect all rows".to_string());
        }

        // Check for TRUNCATE
        if sql_trimmed.starts_with("TRUNCATE") {
            warn!("TRUNCATE operation detected");
            return Some("Warning: TRUNCATE will remove all rows from the table".to_string());
        }

        // Check for DROP
        if sql_trimmed.starts_with("DROP") {
            warn!("DROP operation detected");
            return Some("Warning: DROP will permanently delete the database object".to_string());
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execute_input_defaults() {
        let json = r#"{
            "connection_id": "conn1",
            "sql": "INSERT INTO users (name) VALUES ($1)"
        }"#;

        let input: ExecuteInput = serde_json::from_str(json).unwrap();
        assert_eq!(input.connection_id, "conn1");
        assert!(input.params.is_empty());
        assert!(input.timeout_secs.is_none());
    }

    #[test]
    fn test_execute_input_with_params() {
        let json = r#"{
            "connection_id": "conn1",
            "sql": "INSERT INTO users (name, age) VALUES ($1, $2)",
            "params": ["Alice", 30],
            "timeout_secs": 60
        }"#;

        let input: ExecuteInput = serde_json::from_str(json).unwrap();
        assert_eq!(input.params.len(), 2);
        assert_eq!(input.timeout_secs, Some(60));
    }

    #[test]
    fn test_dangerous_operation_detection() {
        let manager = Arc::new(ConnectionManager::new());
        let registry = Arc::new(TransactionRegistry::new());
        let handler = WriteToolHandler::new(manager, registry);

        // DELETE without WHERE
        let warning = handler.check_dangerous_operation("DELETE FROM users");
        assert!(warning.is_some());
        assert!(warning.unwrap().contains("DELETE without WHERE"));

        // DELETE with WHERE is fine
        let warning = handler.check_dangerous_operation("DELETE FROM users WHERE id = 1");
        assert!(warning.is_none());

        // UPDATE without WHERE
        let warning = handler.check_dangerous_operation("UPDATE users SET active = false");
        assert!(warning.is_some());
        assert!(warning.unwrap().contains("UPDATE without WHERE"));

        // TRUNCATE
        let warning = handler.check_dangerous_operation("TRUNCATE TABLE users");
        assert!(warning.is_some());
        assert!(warning.unwrap().contains("TRUNCATE"));

        // DROP
        let warning = handler.check_dangerous_operation("DROP TABLE users");
        assert!(warning.is_some());
        assert!(warning.unwrap().contains("DROP"));

        // Normal INSERT is fine
        let warning =
            handler.check_dangerous_operation("INSERT INTO users (name) VALUES ('Alice')");
        assert!(warning.is_none());
    }

    #[test]
    fn test_execute_output_serialization() {
        let output = ExecuteOutput {
            rows_affected: 5,
            execution_time_ms: 15,
            warning: Some("Test warning".to_string()),
        };

        let json = serde_json::to_string(&output).unwrap();
        assert!(json.contains("\"rows_affected\":5"));
        assert!(json.contains("\"warning\":"));
    }
}
