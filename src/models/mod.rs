//! Data models for the DB MCP Server.
//!
//! This module re-exports all model types used throughout the application.

pub mod connection;
pub mod query;
pub mod schema;

// Re-export commonly used types
pub use connection::{
    ConnectionConfig, ConnectionConfigError, ConnectionInfo, DatabaseType, TransactionInfo,
    TransactionState,
};
pub use query::{
    ColumnMetadata, DEFAULT_QUERY_TIMEOUT_SECS, DEFAULT_ROW_LIMIT, ExecuteRequest, ExecuteResult,
    MAX_QUERY_TIMEOUT_SECS, MAX_ROW_LIMIT, QueryParam, QueryParamInput, QueryRequest, QueryResult,
};
pub use schema::{
    ColumnDefinition, DescribeTableRequest, ForeignKey, ForeignKeyAction, IndexInfo,
    ListTablesRequest, TableInfo, TableSchema, TableType,
};
