//! Write operation tools.
//!
//! This module implements the `execute` MCP tool for executing
//! INSERT, UPDATE, and DELETE operations.

use crate::db::{ConnectionManager, QueryExecutor, TransactionRegistry};
use crate::error::{DbError, DbResult};
use crate::models::{QueryParam, QueryParamInput};
use crate::tools::guard::{
    DangerousOperationResult, ReadOnlyCheckResult, check_dangerous_sql, check_readonly_sql,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

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
    /// Set to true to allow dangerous operations: DROP, TRUNCATE, DELETE without WHERE, UPDATE without WHERE. These are blocked by default to prevent accidental data loss.
    #[serde(default)]
    pub dangerous_operation_allowed: bool,
}

/// Output from the execute tool.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct ExecuteOutput {
    /// Number of rows affected by the operation
    pub rows_affected: u64,
    /// Execution time in milliseconds
    pub execution_time_ms: u64,
}

pub struct WriteToolHandler {
    connection_manager: Arc<ConnectionManager>,
    transaction_registry: Arc<TransactionRegistry>,
    executor: QueryExecutor,
}

impl WriteToolHandler {
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

    pub async fn execute(&self, input: ExecuteInput) -> DbResult<ExecuteOutput> {
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

        if let ReadOnlyCheckResult::ReadOnlyOperation = check_readonly_sql(&input.sql)? {
            return Err(DbError::invalid_input(
                "This is a read-only operation (SELECT, SHOW, DESCRIBE, etc.). Use the 'query' tool instead of 'execute' for read operations.",
            ));
        }

        if let DangerousOperationResult::Dangerous(op_type) = check_dangerous_sql(&input.sql)? {
            if !input.dangerous_operation_allowed {
                return Err(DbError::dangerous_operation_blocked(
                    op_type.operation_name(),
                    op_type.reason(),
                ));
            }
        }

        let params: Vec<QueryParam> = input.params.into_iter().map(Into::into).collect();

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
            });
        }

        let pool = self
            .connection_manager
            .get_pool(&input.connection_id)
            .await?;
        let timeout = input.timeout_secs.map(|t| Duration::from_secs(t as u64));
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
        })
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
        assert!(!input.dangerous_operation_allowed);
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
    fn test_execute_input_with_dangerous_allowed() {
        let json = r#"{
            "connection_id": "conn1",
            "sql": "DROP TABLE users",
            "dangerous_operation_allowed": true
        }"#;

        let input: ExecuteInput = serde_json::from_str(json).unwrap();
        assert!(input.dangerous_operation_allowed);
    }

    #[test]
    fn test_execute_output_serialization() {
        let output = ExecuteOutput {
            rows_affected: 5,
            execution_time_ms: 15,
        };

        let json = serde_json::to_string(&output).unwrap();
        assert!(json.contains("\"rows_affected\":5"));
        assert!(json.contains("\"execution_time_ms\":15"));
        // Ensure warning field is not present
        assert!(!json.contains("warning"));
    }
}
