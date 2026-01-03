//! MCP tool implementations.
//!
//! This module contains all database tool handlers:
//! - `query`: Execute SELECT queries
//! - `list_tables`: List tables in a database
//! - `describe_table`: Get table schema information
//! - `execute`: Execute write operations (INSERT/UPDATE/DELETE)
//! - `begin_transaction`: Start a transaction
//! - `commit`: Commit a transaction
//! - `rollback`: Rollback a transaction
//! - `list_transactions`: List all active transactions
//! - `explain`: Show query execution plans
//! - `sql_validator`: SQL statement validation for read-only enforcement
//! - `guard`: Dangerous operation detection for execute tool
//! - `format`: Shared output formatting utilities

pub mod explain;
pub mod format;
pub mod guard;
pub mod query;
pub mod schema;
pub mod sql_validator;
pub mod transaction;
pub mod write;

pub use crate::models::QueryParamInput;
pub use explain::{ExplainInput, ExplainOutput, ExplainToolHandler};
pub use format::OutputFormat;
pub use query::{QueryInput, QueryOutput, QueryToolHandler};
pub use schema::{
    DescribeTableInput, DescribeTableOutput, ListTablesInput, ListTablesOutput, SchemaToolHandler,
};
pub use transaction::{
    BeginTransactionInput, BeginTransactionOutput, CommitInput, CommitOutput,
    ListTransactionsInput, ListTransactionsOutput, RollbackInput, RollbackOutput, TransactionInfo,
    TransactionToolHandler,
};
pub use write::{ExecuteInput, ExecuteOutput, WriteToolHandler};
