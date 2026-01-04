//! Query execution tool.
//!
//! This module implements the `query` MCP tool for executing SELECT queries.
//! Write operations (INSERT, UPDATE, DELETE, DDL) are blocked with clear error messages.

use crate::db::{ConnectionManager, QueryExecutor, TransactionRegistry};
use crate::error::DbResult;
use crate::models::{
    DEFAULT_ROW_LIMIT, MAX_ROW_LIMIT, QueryParam, QueryParamInput, QueryRequest, QueryResult,
};
use crate::tools::format::{ColumnInfo, OutputFormat, format_as_markdown, format_as_table};
use crate::tools::sql_validator;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::sync::Arc;
use tracing::info;

fn default_decode_binary() -> bool {
    true
}

/// Input for the query tool.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct QueryInput {
    /// Database connection ID from list_connections
    pub connection_id: String,
    /// SQL SELECT statement to execute. Write operations (INSERT/UPDATE/DELETE/DDL) are blocked.
    pub sql: String,
    /// Positional parameters for parameterized queries (use ? or $1,$2... placeholders in SQL)
    #[serde(default)]
    pub params: Vec<QueryParamInput>,
    /// Maximum rows to return. Default: 100, max: 10000
    #[serde(default)]
    pub limit: Option<u32>,
    /// Query timeout in seconds. Default: 30
    #[serde(default)]
    pub timeout_secs: Option<u32>,
    /// Output format: "json" returns structured data, "table" returns ASCII table, "markdown" returns markdown table
    #[serde(default)]
    pub format: OutputFormat,
    /// Decode binary as UTF-8 when possible (default: true). Falls back to base64.
    #[serde(default = "default_decode_binary")]
    pub decode_binary: bool,
    /// Run query within an existing transaction (from begin_transaction). Omit for auto-commit.
    #[serde(default)]
    pub transaction_id: Option<String>,
    /// Target database name. Required for server-level connections. Optional for database-specific connections (overrides URL database).
    #[serde(default)]
    pub database: Option<String>,
}

/// Output from the query tool.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct QueryOutput {
    /// Query result rows as key-value maps. Empty if format is table/markdown.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub rows: Vec<serde_json::Map<String, JsonValue>>,
    /// Pre-formatted output when format is table or markdown
    #[serde(skip_serializing_if = "Option::is_none")]
    pub formatted: Option<String>,
    /// Number of rows returned
    pub row_count: usize,
    /// Query execution time in milliseconds
    pub execution_time_ms: u64,
    /// Warning message if any issues occurred
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warning: Option<String>,
}

impl QueryOutput {
    pub fn from_result(result: QueryResult, format: OutputFormat) -> Self {
        Self::from_result_with_warning(result, format, None)
    }

    pub fn from_result_with_warning(
        result: QueryResult,
        format: OutputFormat,
        warning: Option<String>,
    ) -> Self {
        let row_count = result.rows.len();
        let execution_time_ms = result.execution_time_ms;

        match format {
            OutputFormat::Json => Self {
                rows: result.rows,
                formatted: None,
                row_count,
                execution_time_ms,
                warning,
            },
            OutputFormat::Table => {
                let cols: Vec<ColumnInfo> = result.columns.iter().map(ColumnInfo::new).collect();
                let formatted = format_as_table(&cols, &result.rows, row_count, execution_time_ms);
                Self {
                    rows: Vec::new(),
                    formatted: Some(formatted),
                    row_count,
                    execution_time_ms,
                    warning,
                }
            }
            OutputFormat::Markdown => {
                let cols: Vec<ColumnInfo> = result.columns.iter().map(ColumnInfo::new).collect();
                let formatted = format_as_markdown(&cols, &result.rows, row_count);
                Self {
                    rows: Vec::new(),
                    formatted: Some(formatted),
                    row_count,
                    execution_time_ms,
                    warning,
                }
            }
        }
    }
}

impl From<QueryResult> for QueryOutput {
    fn from(result: QueryResult) -> Self {
        Self::from_result(result, OutputFormat::Json)
    }
}

pub struct QueryToolHandler {
    connection_manager: Arc<ConnectionManager>,
    transaction_registry: Option<Arc<TransactionRegistry>>,
    executor: QueryExecutor,
}

impl QueryToolHandler {
    pub fn new(connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            connection_manager,
            transaction_registry: None,
            executor: QueryExecutor::new(),
        }
    }

    pub fn with_transaction_registry(
        connection_manager: Arc<ConnectionManager>,
        transaction_registry: Arc<TransactionRegistry>,
    ) -> Self {
        Self {
            connection_manager,
            transaction_registry: Some(transaction_registry),
            executor: QueryExecutor::new(),
        }
    }

    pub fn with_defaults(
        connection_manager: Arc<ConnectionManager>,
        transaction_registry: Arc<TransactionRegistry>,
        default_timeout_secs: u64,
        default_row_limit: u32,
    ) -> Self {
        Self {
            connection_manager,
            transaction_registry: Some(transaction_registry),
            executor: QueryExecutor::with_defaults(default_timeout_secs, default_row_limit),
        }
    }

    pub fn with_executor(
        connection_manager: Arc<ConnectionManager>,
        executor: QueryExecutor,
    ) -> Self {
        Self {
            connection_manager,
            transaction_registry: None,
            executor,
        }
    }

    /// Validates that the SQL is a read-only statement before execution.
    /// Write operations are rejected with clear error messages.
    pub async fn query(&self, input: QueryInput) -> DbResult<QueryOutput> {
        sql_validator::validate_readonly(&input.sql)?;

        let format = input.format;
        let limit_warning = if let Some(requested_limit) = input.limit {
            if requested_limit > MAX_ROW_LIMIT {
                Some(format!(
                    "Requested limit {} exceeds maximum allowed ({}). Results capped to {} rows.",
                    requested_limit, MAX_ROW_LIMIT, MAX_ROW_LIMIT
                ))
            } else {
                None
            }
        } else {
            None
        };

        if let Some(ref tx_id) = input.transaction_id {
            let registry = self.transaction_registry.as_ref().ok_or_else(|| {
                crate::error::DbError::internal(
                    "Transaction registry not configured. Query tool handler was not initialized with transaction support.",
                )
            })?;

            let params: Vec<QueryParam> = input.params.into_iter().map(Into::into).collect();
            let effective_limit = input
                .limit
                .unwrap_or(DEFAULT_ROW_LIMIT)
                .clamp(1, MAX_ROW_LIMIT);

            let result = registry
                .query_in_transaction(
                    tx_id,
                    &input.connection_id,
                    &input.sql,
                    &params,
                    effective_limit,
                    input.decode_binary,
                )
                .await?;

            info!(
                connection_id = %input.connection_id,
                transaction_id = %tx_id,
                row_count = result.rows.len(),
                execution_time_ms = result.execution_time_ms,
                "Query executed in transaction"
            );

            return Ok(QueryOutput::from_result_with_warning(
                result,
                input.format,
                limit_warning,
            ));
        }

        let database = input.database.as_deref();
        let pool = self
            .connection_manager
            .get_pool_for_database(&input.connection_id, database)
            .await?;

        let request = QueryRequest {
            connection_id: input.connection_id.clone(),
            sql: input.sql.clone(),
            params: input.params.into_iter().map(Into::into).collect(),
            limit: input.limit,
            timeout_secs: input.timeout_secs,
            decode_binary: input.decode_binary,
        };

        let result = self.executor.execute_query(&pool, &request).await;

        // Release the pool after use (decrements active_count)
        self.connection_manager
            .release_pool_for_database(&input.connection_id, database)
            .await;

        let result = result?;

        info!(
            connection_id = %input.connection_id,
            row_count = result.rows.len(),
            execution_time_ms = result.execution_time_ms,
            "Query executed"
        );

        Ok(QueryOutput::from_result_with_warning(
            result,
            format,
            limit_warning,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_input_deserialization() {
        let json = r#"{
            "connection_id": "conn_123",
            "sql": "SELECT * FROM users WHERE id = $1",
            "params": [42],
            "limit": 100
        }"#;

        let input: QueryInput = serde_json::from_str(json).unwrap();
        assert_eq!(input.connection_id, "conn_123");
        assert_eq!(input.sql, "SELECT * FROM users WHERE id = $1");
        assert_eq!(input.params.len(), 1);
        assert_eq!(input.limit, Some(100));
        // decode_binary should default to true
        assert!(input.decode_binary);
    }

    #[test]
    fn test_query_input_decode_binary_explicit() {
        // Test with decode_binary explicitly set to false
        let json = r#"{
            "connection_id": "conn_123",
            "sql": "SELECT data FROM binary_table",
            "decode_binary": false
        }"#;

        let input: QueryInput = serde_json::from_str(json).unwrap();
        assert!(!input.decode_binary);

        // Test with decode_binary explicitly set to true
        let json = r#"{
            "connection_id": "conn_123",
            "sql": "SELECT data FROM binary_table",
            "decode_binary": true
        }"#;

        let input: QueryInput = serde_json::from_str(json).unwrap();
        assert!(input.decode_binary);
    }

    #[test]
    fn test_query_param_conversion() {
        assert!(matches!(
            QueryParam::from(QueryParamInput::Null),
            QueryParam::Null
        ));
        assert!(matches!(
            QueryParam::from(QueryParamInput::Bool(true)),
            QueryParam::Bool(true)
        ));
        assert!(matches!(
            QueryParam::from(QueryParamInput::Int(42)),
            QueryParam::Int(42)
        ));
    }

    #[test]
    fn test_query_output_serialization() {
        let mut row = serde_json::Map::new();
        row.insert("id".to_string(), JsonValue::Number(1.into()));

        let output = QueryOutput {
            rows: vec![row],
            formatted: None,
            row_count: 1,
            execution_time_ms: 10,
            warning: None,
        };

        let json = serde_json::to_string(&output).unwrap();
        assert!(json.contains("\"row_count\":1"));
        assert!(json.contains("\"id\":1"));
    }
}
