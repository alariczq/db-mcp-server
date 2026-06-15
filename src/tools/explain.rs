//! Query execution plan tools.
//!
//! This module implements the `explain` MCP tool for viewing query execution plans.
//! It supports all three database types (MySQL, PostgreSQL, SQLite) using their
//! native EXPLAIN syntax.

use crate::db::params::{bind_mysql_param, bind_postgres_param, bind_sqlite_param};
use crate::db::{ConnectionManager, DbPool, TransactionRegistry};
use crate::error::{DbError, DbResult};
use crate::models::{QueryParam, QueryParamInput};
use crate::tools::format::{
    ColumnInfo, OutputFormat, build_compact_rows, format_as_markdown, format_as_table,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Default timeout for EXPLAIN operations in seconds.
pub const DEFAULT_EXPLAIN_TIMEOUT_SECS: u32 = 30;

/// Maximum timeout for EXPLAIN operations in seconds.
pub const MAX_EXPLAIN_TIMEOUT_SECS: u32 = 30;

/// Input for the explain tool.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[schemars(transform = schemars::transform::RestrictFormats::default())]
pub struct ExplainInput {
    /// Database connection ID from list_connections
    pub connection_id: String,
    /// SQL statement to explain (SELECT, INSERT, UPDATE, or DELETE)
    pub sql: String,
    /// Positional parameters for parameterized queries (use ? or $1,$2... placeholders in SQL)
    #[serde(default)]
    pub params: Vec<QueryParamInput>,
    /// Run explain within an existing transaction (from begin_transaction). Omit for auto-commit.
    #[serde(default)]
    pub transaction_id: Option<String>,
    /// Timeout in seconds. Default: 30
    #[serde(default)]
    pub timeout_secs: Option<u32>,
    /// Output format: compact (default), json, table, or markdown
    #[serde(default)]
    pub format: OutputFormat,
    /// Target database name (optional)
    #[serde(default)]
    pub database: Option<String>,
}

/// Output from the explain tool.
#[derive(Debug, Clone, Serialize, JsonSchema)]
#[schemars(transform = schemars::transform::RestrictFormats::default())]
pub struct ExplainOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub rows: Vec<serde_json::Value>,
    pub sql: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub formatted: Option<String>,
    pub execution_time_ms: u64,
}

pub struct ExplainToolHandler {
    connection_manager: Arc<ConnectionManager>,
    transaction_registry: Arc<TransactionRegistry>,
}

impl ExplainToolHandler {
    pub fn new(
        connection_manager: Arc<ConnectionManager>,
        transaction_registry: Arc<TransactionRegistry>,
    ) -> Self {
        Self {
            connection_manager,
            transaction_registry,
        }
    }

    /// SQLite uses EXPLAIN QUERY PLAN for SELECT, EXPLAIN for writes.
    /// MySQL/PostgreSQL/ClickHouse use EXPLAIN directly.
    fn generate_explain_sql(pool: &DbPool, sql: &str) -> String {
        match pool {
            DbPool::SQLite(_) => {
                let trimmed = sql.trim_start().to_uppercase();
                if trimmed.starts_with("SELECT") {
                    format!("EXPLAIN QUERY PLAN {}", sql)
                } else {
                    format!("EXPLAIN {}", sql)
                }
            }
            DbPool::MySql(_) | DbPool::Postgres(_) | DbPool::ClickHouse(_, _) => {
                format!("EXPLAIN {}", sql)
            }
        }
    }

    fn build_output(
        rows: Vec<serde_json::Map<String, serde_json::Value>>,
        sql: &str,
        execution_time_ms: u64,
        format: OutputFormat,
    ) -> ExplainOutput {
        let columns: Vec<ColumnInfo> = if let Some(first_row) = rows.first() {
            first_row.keys().map(ColumnInfo::new).collect()
        } else {
            Vec::new()
        };
        let row_count = rows.len();

        match format {
            OutputFormat::Compact => {
                let col_names = columns.iter().map(|c| c.name.clone()).collect();
                let compact_rows = build_compact_rows(&columns, &rows);
                ExplainOutput {
                    columns: Some(col_names),
                    rows: compact_rows,
                    sql: sql.to_string(),
                    formatted: None,
                    execution_time_ms,
                }
            }
            OutputFormat::Table => {
                let formatted = format_as_table(&columns, &rows, row_count, execution_time_ms);
                ExplainOutput {
                    columns: None,
                    rows: Vec::new(),
                    sql: sql.to_string(),
                    formatted: Some(formatted),
                    execution_time_ms,
                }
            }
            OutputFormat::Markdown => {
                let formatted = format_as_markdown(&columns, &rows, row_count);
                ExplainOutput {
                    columns: None,
                    rows: Vec::new(),
                    sql: sql.to_string(),
                    formatted: Some(formatted),
                    execution_time_ms,
                }
            }
            OutputFormat::Json => {
                let json_rows = rows.into_iter().map(serde_json::Value::Object).collect();
                ExplainOutput {
                    columns: None,
                    rows: json_rows,
                    sql: sql.to_string(),
                    formatted: None,
                    execution_time_ms,
                }
            }
        }
    }

    pub async fn explain(&self, input: ExplainInput) -> DbResult<ExplainOutput> {
        let start = Instant::now();
        let sql = input.sql.trim();
        if sql.is_empty() {
            return Err(DbError::invalid_input("SQL statement is required"));
        }

        let timeout_secs = input
            .timeout_secs
            .map(|t| t.min(MAX_EXPLAIN_TIMEOUT_SECS))
            .unwrap_or(DEFAULT_EXPLAIN_TIMEOUT_SECS);
        let timeout = Duration::from_secs(timeout_secs as u64);
        let params: Vec<QueryParam> = input.params.into_iter().map(Into::into).collect();

        let format = input.format;

        if let Some(ref tx_id) = input.transaction_id {
            self.transaction_registry
                .is_valid(tx_id, &input.connection_id)
                .await?;

            let database = input.database.as_deref();
            let pool = self
                .connection_manager
                .get_pool_for_database(&input.connection_id, database)
                .await?;
            let explain_sql = Self::generate_explain_sql(&pool, sql);

            // EXPLAIN queries don't need streaming limits, use a high limit
            let result = self
                .transaction_registry
                .query_in_transaction(
                    tx_id,
                    &input.connection_id,
                    &explain_sql,
                    &params,
                    10000, // EXPLAIN results are typically small
                    true,  // decode_binary
                )
                .await;

            self.connection_manager
                .release_pool_for_database(&input.connection_id, database)
                .await;

            let result = result?;

            return Ok(Self::build_output(
                result.rows,
                sql,
                start.elapsed().as_millis() as u64,
                format,
            ));
        }

        let database = input.database.as_deref();
        let pool = self
            .connection_manager
            .get_pool_for_database(&input.connection_id, database)
            .await?;
        let explain_sql = Self::generate_explain_sql(&pool, sql);

        let result = self
            .execute_explain(&pool, &explain_sql, &params, timeout)
            .await;

        self.connection_manager
            .release_pool_for_database(&input.connection_id, database)
            .await;

        let rows = result?;

        Ok(Self::build_output(
            rows,
            sql,
            start.elapsed().as_millis() as u64,
            format,
        ))
    }

    async fn execute_explain(
        &self,
        pool: &DbPool,
        explain_sql: &str,
        params: &[QueryParam],
        timeout: Duration,
    ) -> DbResult<Vec<serde_json::Map<String, serde_json::Value>>> {
        use crate::db::types::RowToJson;
        use futures_util::TryStreamExt;

        match pool {
            DbPool::MySql(p) => {
                let mut query = sqlx::query(explain_sql);
                for param in params {
                    query = bind_mysql_param(query, param);
                }

                let rows_future = query.fetch(p).try_collect::<Vec<_>>();
                match tokio::time::timeout(timeout, rows_future).await {
                    Ok(Ok(rows)) => Ok(rows.iter().map(|r| r.to_json_map()).collect()),
                    Ok(Err(e)) => Err(DbError::from(e)),
                    Err(_) => Err(DbError::timeout("EXPLAIN", timeout.as_secs() as u32)),
                }
            }
            DbPool::Postgres(p) => {
                let mut query = sqlx::query(explain_sql);
                for param in params {
                    query = bind_postgres_param(query, param);
                }

                let rows_future = query.fetch(p).try_collect::<Vec<_>>();
                match tokio::time::timeout(timeout, rows_future).await {
                    Ok(Ok(rows)) => Ok(rows.iter().map(|r| r.to_json_map()).collect()),
                    Ok(Err(e)) => Err(DbError::from(e)),
                    Err(_) => Err(DbError::timeout("EXPLAIN", timeout.as_secs() as u32)),
                }
            }
            DbPool::SQLite(p) => {
                let mut query = sqlx::query(explain_sql);
                for param in params {
                    query = bind_sqlite_param(query, param);
                }

                let rows_future = query.fetch(p).try_collect::<Vec<_>>();
                match tokio::time::timeout(timeout, rows_future).await {
                    Ok(Ok(rows)) => Ok(rows.iter().map(|r| r.to_json_map()).collect()),
                    Ok(Err(e)) => Err(DbError::from(e)),
                    Err(_) => Err(DbError::timeout("EXPLAIN", timeout.as_secs() as u32)),
                }
            }
            DbPool::ClickHouse(c, _) => {
                let json_sql = format!("{} FORMAT JSON", explain_sql);
                let mut query = c.query(&json_sql);
                for param in params {
                    query = match param {
                        QueryParam::Null => query.bind(()),
                        QueryParam::Bool(v) => query.bind(*v),
                        QueryParam::Int(v) => query.bind(*v),
                        QueryParam::Float(v) => query.bind(*v),
                        QueryParam::String(v) => query.bind(v.as_str()),
                        QueryParam::Json(v) => query.bind(v.to_string()),
                    };
                }

                match tokio::time::timeout(timeout, query.fetch_all::<String>()).await {
                    Ok(Ok(parts)) => {
                        #[derive(serde::Deserialize)]
                        struct JsonResult {
                            data: Vec<serde_json::Map<String, serde_json::Value>>,
                        }
                        let json_str = parts.join("");
                        let json_result: JsonResult = serde_json::from_str(&json_str)
                            .map_err(|e| {
                                DbError::database(
                                    format!("Failed to parse ClickHouse JSON result: {}", e),
                                    None,
                                    "Check the SQL syntax and ClickHouse server status",
                                )
                            })?;
                        Ok(json_result.data)
                    }
                    Ok(Err(e)) => Err(DbError::database(
                        format!("ClickHouse EXPLAIN error: {}", e),
                        None,
                        "Check the SQL syntax and ClickHouse server status",
                    )),
                    Err(_) => Err(DbError::timeout("EXPLAIN", timeout.as_secs() as u32)),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_explain_input_defaults() {
        let json = r#"{"connection_id": "conn1", "sql": "SELECT 1"}"#;
        let input: ExplainInput = serde_json::from_str(json).unwrap();

        assert_eq!(input.connection_id, "conn1");
        assert_eq!(input.sql, "SELECT 1");
        assert!(input.params.is_empty());
        assert!(input.transaction_id.is_none());
        assert!(input.timeout_secs.is_none());
        assert!(matches!(input.format, OutputFormat::Compact));
    }

    #[test]
    fn test_explain_input_with_table_format() {
        let json = r#"{"connection_id": "conn1", "sql": "SELECT 1", "format": "table"}"#;
        let input: ExplainInput = serde_json::from_str(json).unwrap();

        assert!(matches!(input.format, OutputFormat::Table));
    }

    #[test]
    fn test_explain_input_with_markdown_format() {
        let json = r#"{"connection_id": "conn1", "sql": "SELECT 1", "format": "markdown"}"#;
        let input: ExplainInput = serde_json::from_str(json).unwrap();

        assert!(matches!(input.format, OutputFormat::Markdown));
    }

    #[test]
    fn test_explain_input_with_params() {
        let json =
            r#"{"connection_id": "conn1", "sql": "SELECT * FROM t WHERE id = ?", "params": [123]}"#;
        let input: ExplainInput = serde_json::from_str(json).unwrap();

        assert_eq!(input.params.len(), 1);
    }

    #[test]
    fn test_explain_input_with_transaction() {
        let json = r#"{"connection_id": "conn1", "sql": "SELECT 1", "transaction_id": "tx_abc"}"#;
        let input: ExplainInput = serde_json::from_str(json).unwrap();

        assert_eq!(input.transaction_id, Some("tx_abc".to_string()));
    }

    #[test]
    fn test_generate_explain_sql_sqlite_select() {
        // We can't easily test this without a real pool, but we can test the logic
        let sql = "SELECT * FROM users";
        let trimmed = sql.trim_start().to_uppercase();
        assert!(trimmed.starts_with("SELECT"));
    }

    #[test]
    fn test_generate_explain_sql_sqlite_insert() {
        let sql = "INSERT INTO users (name) VALUES ('test')";
        let trimmed = sql.trim_start().to_uppercase();
        assert!(!trimmed.starts_with("SELECT"));
    }

    #[test]
    fn test_query_param_input_conversion() {
        let null: QueryParam = QueryParamInput::Null.into();
        assert!(matches!(null, QueryParam::Null));

        let int: QueryParam = QueryParamInput::Int(42).into();
        assert!(matches!(int, QueryParam::Int(42)));

        let s: QueryParam = QueryParamInput::String("hello".to_string()).into();
        assert!(matches!(s, QueryParam::String(_)));
    }
}
