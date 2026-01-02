//! Query execution engine.
//!
//! This module provides query execution functionality with support for:
//! - Parameterized queries
//! - Row limits (enforced via streaming - only fetches needed rows)
//! - Query timeouts
//! - Result streaming
//!
//! # Architecture
//!
//! The executor uses database-specific implementations organized in submodules:
//! - `mysql`: MySQL-specific query and write operations
//! - `postgres`: PostgreSQL-specific query and write operations
//! - `sqlite`: SQLite-specific query and write operations
//!
//! Each submodule provides identical functionality adapted to the database's type system.

use crate::db::pool::DbPool;
use crate::db::types::RowToJson;
use crate::error::{DbError, DbResult};
use crate::models::{
    DEFAULT_QUERY_TIMEOUT_SECS, DEFAULT_ROW_LIMIT, MAX_ROW_LIMIT, QueryParam, QueryRequest,
    QueryResult,
};
use futures_util::StreamExt;
use std::time::{Duration, Instant};
use tokio::time::timeout;
use tracing::{debug, warn};

/// Query executor that handles database query execution.
pub struct QueryExecutor {
    default_timeout: Duration,
    default_limit: u32,
}

impl QueryExecutor {
    /// Create a new query executor with default settings.
    pub fn new() -> Self {
        Self {
            default_timeout: Duration::from_secs(DEFAULT_QUERY_TIMEOUT_SECS as u64),
            default_limit: DEFAULT_ROW_LIMIT,
        }
    }

    /// Create a new query executor with custom settings.
    pub fn with_defaults(timeout_secs: u64, row_limit: u32) -> Self {
        Self {
            default_timeout: Duration::from_secs(timeout_secs),
            default_limit: row_limit.min(MAX_ROW_LIMIT),
        }
    }

    /// Execute a SELECT query and return results.
    pub async fn execute_query(
        &self,
        pool: &DbPool,
        request: &QueryRequest,
    ) -> DbResult<QueryResult> {
        let start = Instant::now();
        // Ensure limit is at least 1 to avoid edge cases where limit=0 causes all results to be "truncated"
        let row_limit = request
            .limit
            .map(|l| l.clamp(1, MAX_ROW_LIMIT))
            .unwrap_or(self.default_limit);
        let query_timeout = request
            .timeout_secs
            .map(|t| Duration::from_secs(t as u64))
            .unwrap_or(self.default_timeout);

        debug!(
            sql = %request.sql,
            params = ?request.params.len(),
            limit = row_limit,
            timeout_secs = ?query_timeout.as_secs(),
            "Executing query"
        );

        match pool {
            DbPool::MySql(p) => {
                let rows =
                    mysql::fetch_rows(p, &request.sql, &request.params, row_limit, query_timeout)
                        .await?;
                process_rows(rows, row_limit, start, request.decode_binary)
            }
            DbPool::Postgres(p) => {
                let rows = postgres::fetch_rows(
                    p,
                    &request.sql,
                    &request.params,
                    row_limit,
                    query_timeout,
                )
                .await?;
                process_rows(rows, row_limit, start, request.decode_binary)
            }
            DbPool::SQLite(p) => {
                let rows =
                    sqlite::fetch_rows(p, &request.sql, &request.params, row_limit, query_timeout)
                        .await?;
                process_rows(rows, row_limit, start, request.decode_binary)
            }
        }
    }

    /// Execute a write operation (INSERT, UPDATE, DELETE) and return affected rows.
    pub async fn execute_write(
        &self,
        pool: &DbPool,
        sql: &str,
        params: &[QueryParam],
        query_timeout: Option<Duration>,
    ) -> DbResult<(u64, u64)> {
        let start = Instant::now();
        let query_timeout = query_timeout.unwrap_or(self.default_timeout);

        debug!(
            sql = %sql,
            params = ?params.len(),
            timeout_secs = ?query_timeout.as_secs(),
            "Executing write operation"
        );

        let rows_affected = match pool {
            DbPool::MySql(p) => mysql::execute_write(p, sql, params, query_timeout).await?,
            DbPool::Postgres(p) => postgres::execute_write(p, sql, params, query_timeout).await?,
            DbPool::SQLite(p) => sqlite::execute_write(p, sql, params, query_timeout).await?,
        };

        let execution_time_ms = start.elapsed().as_millis() as u64;
        Ok((rows_affected, execution_time_ms))
    }
}

impl Default for QueryExecutor {
    fn default() -> Self {
        Self::new()
    }
}

/// Process rows from any database type into a QueryResult.
fn process_rows<R: RowToJson>(
    rows: Vec<R>,
    row_limit: u32,
    start: Instant,
    decode_binary: bool,
) -> DbResult<QueryResult> {
    let execution_time_ms = start.elapsed().as_millis() as u64;

    if rows.is_empty() {
        return Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: None,
            truncated: false,
            execution_time_ms,
        });
    }

    let columns = rows[0].get_column_metadata();
    let total_rows = rows.len();
    let truncated = total_rows > row_limit as usize;
    let rows_to_take = if truncated {
        row_limit as usize
    } else {
        total_rows
    };

    let json_rows: Vec<serde_json::Map<String, serde_json::Value>> = rows
        .iter()
        .take(rows_to_take)
        .map(|r| r.to_json_map_with_options(decode_binary))
        .collect();

    if truncated {
        warn!(
            total_rows = total_rows,
            limit = row_limit,
            "Query result truncated"
        );
    }

    Ok(QueryResult {
        columns,
        rows: json_rows,
        rows_affected: None,
        truncated,
        execution_time_ms,
    })
}

// =============================================================================
// Database-Specific Implementations
// =============================================================================
//
// Each module below provides the same interface adapted to its database type.
// The code structure is intentionally parallel to make differences obvious.

mod mysql {
    use super::*;
    use sqlx::MySqlPool;
    use sqlx::mysql::{MySqlArguments, MySqlRow};

    pub async fn fetch_rows(
        pool: &MySqlPool,
        sql: &str,
        params: &[QueryParam],
        row_limit: u32,
        query_timeout: Duration,
    ) -> DbResult<Vec<MySqlRow>> {
        let mut query = sqlx::query(sql);
        for param in params {
            query = bind_param(query, param);
        }

        let fetch_limit = row_limit as usize + 1;
        let stream = query.fetch(pool);
        let rows_future = stream.take(fetch_limit).collect::<Vec<_>>();

        match timeout(query_timeout, rows_future).await {
            Ok(results) => {
                let mut rows = Vec::with_capacity(results.len());
                for result in results {
                    rows.push(result.map_err(DbError::from)?);
                }
                Ok(rows)
            }
            Err(_) => Err(DbError::timeout(
                "query execution",
                query_timeout.as_secs() as u32,
            )),
        }
    }

    pub async fn execute_write(
        pool: &MySqlPool,
        sql: &str,
        params: &[QueryParam],
        query_timeout: Duration,
    ) -> DbResult<u64> {
        let mut query = sqlx::query(sql);
        for param in params {
            query = bind_param(query, param);
        }

        match timeout(query_timeout, query.execute(pool)).await {
            Ok(Ok(r)) => Ok(r.rows_affected()),
            Ok(Err(e)) => Err(DbError::from(e)),
            Err(_) => Err(DbError::timeout(
                "write operation",
                query_timeout.as_secs() as u32,
            )),
        }
    }

    fn bind_param<'q>(
        query: sqlx::query::Query<'q, sqlx::MySql, MySqlArguments>,
        param: &'q QueryParam,
    ) -> sqlx::query::Query<'q, sqlx::MySql, MySqlArguments> {
        match param {
            QueryParam::Null => query.bind(None::<String>),
            QueryParam::Bool(v) => query.bind(*v),
            QueryParam::Int(v) => query.bind(*v),
            QueryParam::Float(v) => query.bind(*v),
            QueryParam::String(v) => query.bind(v.as_str()),
            QueryParam::Bytes(v) => query.bind(v.as_slice()),
        }
    }
}

mod postgres {
    use super::*;
    use sqlx::PgPool;
    use sqlx::postgres::{PgArguments, PgRow};

    pub async fn fetch_rows(
        pool: &PgPool,
        sql: &str,
        params: &[QueryParam],
        row_limit: u32,
        query_timeout: Duration,
    ) -> DbResult<Vec<PgRow>> {
        let mut query = sqlx::query(sql);
        for param in params {
            query = bind_param(query, param);
        }

        let fetch_limit = row_limit as usize + 1;
        let stream = query.fetch(pool);
        let rows_future = stream.take(fetch_limit).collect::<Vec<_>>();

        match timeout(query_timeout, rows_future).await {
            Ok(results) => {
                let mut rows = Vec::with_capacity(results.len());
                for result in results {
                    rows.push(result.map_err(DbError::from)?);
                }
                Ok(rows)
            }
            Err(_) => Err(DbError::timeout(
                "query execution",
                query_timeout.as_secs() as u32,
            )),
        }
    }

    pub async fn execute_write(
        pool: &PgPool,
        sql: &str,
        params: &[QueryParam],
        query_timeout: Duration,
    ) -> DbResult<u64> {
        let mut query = sqlx::query(sql);
        for param in params {
            query = bind_param(query, param);
        }

        match timeout(query_timeout, query.execute(pool)).await {
            Ok(Ok(r)) => Ok(r.rows_affected()),
            Ok(Err(e)) => Err(DbError::from(e)),
            Err(_) => Err(DbError::timeout(
                "write operation",
                query_timeout.as_secs() as u32,
            )),
        }
    }

    fn bind_param<'q>(
        query: sqlx::query::Query<'q, sqlx::Postgres, PgArguments>,
        param: &'q QueryParam,
    ) -> sqlx::query::Query<'q, sqlx::Postgres, PgArguments> {
        match param {
            QueryParam::Null => query.bind(None::<String>),
            QueryParam::Bool(v) => query.bind(*v),
            QueryParam::Int(v) => query.bind(*v),
            QueryParam::Float(v) => query.bind(*v),
            QueryParam::String(v) => query.bind(v.as_str()),
            QueryParam::Bytes(v) => query.bind(v.as_slice()),
        }
    }
}

mod sqlite {
    use super::*;
    use sqlx::SqlitePool;
    use sqlx::sqlite::{SqliteArguments, SqliteRow};

    pub async fn fetch_rows(
        pool: &SqlitePool,
        sql: &str,
        params: &[QueryParam],
        row_limit: u32,
        query_timeout: Duration,
    ) -> DbResult<Vec<SqliteRow>> {
        let mut query = sqlx::query(sql);
        for param in params {
            query = bind_param(query, param);
        }

        let fetch_limit = row_limit as usize + 1;
        let stream = query.fetch(pool);
        let rows_future = stream.take(fetch_limit).collect::<Vec<_>>();

        match timeout(query_timeout, rows_future).await {
            Ok(results) => {
                let mut rows = Vec::with_capacity(results.len());
                for result in results {
                    rows.push(result.map_err(DbError::from)?);
                }
                Ok(rows)
            }
            Err(_) => Err(DbError::timeout(
                "query execution",
                query_timeout.as_secs() as u32,
            )),
        }
    }

    pub async fn execute_write(
        pool: &SqlitePool,
        sql: &str,
        params: &[QueryParam],
        query_timeout: Duration,
    ) -> DbResult<u64> {
        let mut query = sqlx::query(sql);
        for param in params {
            query = bind_param(query, param);
        }

        match timeout(query_timeout, query.execute(pool)).await {
            Ok(Ok(r)) => Ok(r.rows_affected()),
            Ok(Err(e)) => Err(DbError::from(e)),
            Err(_) => Err(DbError::timeout(
                "write operation",
                query_timeout.as_secs() as u32,
            )),
        }
    }

    fn bind_param<'q>(
        query: sqlx::query::Query<'q, sqlx::Sqlite, SqliteArguments<'q>>,
        param: &'q QueryParam,
    ) -> sqlx::query::Query<'q, sqlx::Sqlite, SqliteArguments<'q>> {
        match param {
            QueryParam::Null => query.bind(None::<String>),
            QueryParam::Bool(v) => query.bind(*v),
            QueryParam::Int(v) => query.bind(*v),
            QueryParam::Float(v) => query.bind(*v),
            QueryParam::String(v) => query.bind(v.as_str()),
            QueryParam::Bytes(v) => query.bind(v.as_slice()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_executor_defaults() {
        let executor = QueryExecutor::new();
        assert_eq!(
            executor.default_timeout,
            Duration::from_secs(DEFAULT_QUERY_TIMEOUT_SECS as u64)
        );
        assert_eq!(executor.default_limit, DEFAULT_ROW_LIMIT);
    }

    #[test]
    fn test_executor_custom_settings() {
        let executor = QueryExecutor::with_defaults(60, 500);
        assert_eq!(executor.default_timeout, Duration::from_secs(60));
        assert_eq!(executor.default_limit, 500);
    }

    #[test]
    fn test_executor_limit_capped() {
        let executor = QueryExecutor::with_defaults(30, 99999);
        assert_eq!(executor.default_limit, MAX_ROW_LIMIT);
    }
}
