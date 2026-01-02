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
//! - `sql_validator`: SQL statement validation for read-only enforcement

pub mod query;
pub mod schema;
pub mod sql_validator;
pub mod transaction;
pub mod write;

pub use query::{QueryInput, QueryOutput, QueryToolHandler};
pub use schema::{
    DescribeTableInput, DescribeTableOutput, ListTablesInput, ListTablesOutput, SchemaToolHandler,
};
pub use transaction::{
    BeginTransactionInput, BeginTransactionOutput, CommitInput, CommitOutput, RollbackInput,
    RollbackOutput, TransactionToolHandler,
};
pub use write::{ExecuteInput, ExecuteOutput, WriteToolHandler};
