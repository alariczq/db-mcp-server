//! Database dispatch macros for reducing code duplication.
//!
//! This module provides declarative macros that generate database-specific
//! implementations while maintaining linear readability. The macros expand
//! at compile time with zero runtime overhead.

/// Database backend type for dispatch operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseType {
    MySql,
    Postgres,
    SQLite,
}

/// Macro for generating database dispatch match arms.
///
/// This macro generates match arms for `DbPool` variants, reducing the need
/// to manually write repetitive match statements.
///
/// # Example
///
/// ```ignore
/// impl_db_dispatch!(pool, {
///     MySql(p) => do_mysql(p),
///     Postgres(p) => do_postgres(p),
///     SQLite(p) => do_sqlite(p),
/// });
/// ```
#[macro_export]
macro_rules! impl_db_dispatch {
    ($pool:expr, { $($variant:ident($p:ident) => $body:expr),+ $(,)? }) => {
        match $pool {
            $(
                $crate::db::pool::DbPool::$variant($p) => $body,
            )+
        }
    };
}

pub use impl_db_dispatch;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_type_debug() {
        assert_eq!(format!("{:?}", DatabaseType::MySql), "MySql");
        assert_eq!(format!("{:?}", DatabaseType::Postgres), "Postgres");
        assert_eq!(format!("{:?}", DatabaseType::SQLite), "SQLite");
    }

    #[test]
    fn test_database_type_equality() {
        assert_eq!(DatabaseType::MySql, DatabaseType::MySql);
        assert_ne!(DatabaseType::MySql, DatabaseType::Postgres);
    }
}
