//! Database abstraction layer.
//!
//! This module provides database access functionality:
//! - Connection pool management
//! - Query execution
//! - Schema introspection
//! - Type mappings
//! - Database dispatch macros for reducing code duplication
//! - Transaction registry for stateful transaction management

pub mod executor;
#[macro_use]
pub mod macros;
pub mod params;
pub mod pool;
pub mod schema;
pub mod transaction_registry;
pub mod types;

pub use executor::QueryExecutor;
pub use macros::DatabaseType;
pub use pool::{ConnectionManager, ConnectionSummary, DbPool};
pub use schema::{DatabaseInfoRow, SchemaInspector};
pub use transaction_registry::TransactionRegistry;
