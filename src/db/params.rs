//! Parameter binding utilities for database queries.
//!
//! This module provides functions to bind `QueryParam` values to database-specific
//! query objects. These are used by transaction operations and explain functionality.

use crate::models::QueryParam;
use sqlx::mysql::MySqlArguments;
use sqlx::postgres::PgArguments;
use sqlx::sqlite::SqliteArguments;
use sqlx::types::Json;
use sqlx::{MySql, Postgres, Sqlite};

/// Bind a parameter to a MySQL query.
pub(crate) fn bind_mysql_param<'q>(
    query: sqlx::query::Query<'q, MySql, MySqlArguments>,
    param: &'q QueryParam,
) -> sqlx::query::Query<'q, MySql, MySqlArguments> {
    match param {
        QueryParam::Null => query.bind(None::<String>),
        QueryParam::Bool(v) => query.bind(*v),
        QueryParam::Int(v) => query.bind(*v),
        QueryParam::Float(v) => query.bind(*v),
        QueryParam::String(v) => query.bind(v.as_str()),
        QueryParam::Json(v) => query.bind(Json(v)),
    }
}

/// Bind a parameter to a PostgreSQL query.
pub(crate) fn bind_postgres_param<'q>(
    query: sqlx::query::Query<'q, Postgres, PgArguments>,
    param: &'q QueryParam,
) -> sqlx::query::Query<'q, Postgres, PgArguments> {
    match param {
        QueryParam::Null => query.bind(None::<String>),
        QueryParam::Bool(v) => query.bind(*v),
        QueryParam::Int(v) => query.bind(*v),
        QueryParam::Float(v) => query.bind(*v),
        QueryParam::String(v) => query.bind(v.as_str()),
        QueryParam::Json(v) => query.bind(Json(v)),
    }
}

/// Bind a parameter to a SQLite query.
pub(crate) fn bind_sqlite_param<'q>(
    query: sqlx::query::Query<'q, Sqlite, SqliteArguments<'q>>,
    param: &'q QueryParam,
) -> sqlx::query::Query<'q, Sqlite, SqliteArguments<'q>> {
    match param {
        QueryParam::Null => query.bind(None::<String>),
        QueryParam::Bool(v) => query.bind(*v),
        QueryParam::Int(v) => query.bind(*v),
        QueryParam::Float(v) => query.bind(*v),
        QueryParam::String(v) => query.bind(v.as_str()),
        // SQLite doesn't have native JSON type, store as string
        QueryParam::Json(v) => query.bind(v.to_string()),
    }
}
