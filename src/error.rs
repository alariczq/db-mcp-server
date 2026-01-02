//! Error types for the DB MCP Server.
//!
//! This module defines all error types using `thiserror` for ergonomic error handling.
//! Each error variant provides actionable messages to help AI assistants understand
//! and recover from error conditions.

use thiserror::Error;

#[derive(Error, Debug)]
pub enum DbError {
    #[error("Connection failed: {message}")]
    Connection { message: String, suggestion: String },

    #[error("Query failed: {message}")]
    Query {
        message: String,
        /// e.g., "42P01" for undefined table
        sql_state: Option<String>,
        suggestion: String,
    },

    #[error("Permission denied: {operation} - {reason}")]
    Permission { operation: String, reason: String },

    #[error("Schema error: {message} (object: {object})")]
    Schema { message: String, object: String },

    #[error("Transaction error: {message} (transaction: {transaction_id})")]
    Transaction {
        message: String,
        transaction_id: String,
    },

    #[error("Timeout: {operation} exceeded {elapsed_secs}s")]
    Timeout {
        operation: String,
        elapsed_secs: u32,
    },

    #[error("Connection not found: {connection_id}")]
    ConnectionNotFound { connection_id: String },

    #[error("Invalid input: {message}")]
    InvalidInput { message: String },

    #[error("Internal error: {message}")]
    Internal { message: String },
}

impl DbError {
    /// Create a connection error with a helpful suggestion.
    pub fn connection(message: impl Into<String>, suggestion: impl Into<String>) -> Self {
        Self::Connection {
            message: message.into(),
            suggestion: suggestion.into(),
        }
    }

    /// Create a query error with optional SQL state.
    pub fn query(
        message: impl Into<String>,
        sql_state: Option<String>,
        suggestion: impl Into<String>,
    ) -> Self {
        Self::Query {
            message: message.into(),
            sql_state,
            suggestion: suggestion.into(),
        }
    }

    /// Create a permission error.
    pub fn permission(operation: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::Permission {
            operation: operation.into(),
            reason: reason.into(),
        }
    }

    /// Create a schema error.
    pub fn schema(message: impl Into<String>, object: impl Into<String>) -> Self {
        Self::Schema {
            message: message.into(),
            object: object.into(),
        }
    }

    /// Create a transaction error.
    pub fn transaction(message: impl Into<String>, transaction_id: impl Into<String>) -> Self {
        Self::Transaction {
            message: message.into(),
            transaction_id: transaction_id.into(),
        }
    }

    /// Create a timeout error.
    pub fn timeout(operation: impl Into<String>, elapsed_secs: u32) -> Self {
        Self::Timeout {
            operation: operation.into(),
            elapsed_secs,
        }
    }

    /// Create a connection not found error.
    pub fn connection_not_found(connection_id: impl Into<String>) -> Self {
        Self::ConnectionNotFound {
            connection_id: connection_id.into(),
        }
    }

    /// Create an invalid input error.
    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::InvalidInput {
            message: message.into(),
        }
    }

    /// Create an internal error.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }

    /// Get the suggestion for this error, if available.
    pub fn suggestion(&self) -> Option<&str> {
        match self {
            Self::Connection { suggestion, .. } => Some(suggestion),
            Self::Query { suggestion, .. } => Some(suggestion),
            _ => None,
        }
    }

    /// Check if this error is retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Connection { .. } | Self::Timeout { .. })
    }
}

/// Convert sqlx errors to DbError.
impl From<sqlx::Error> for DbError {
    fn from(err: sqlx::Error) -> Self {
        match err {
            sqlx::Error::Configuration(msg) => DbError::connection(
                msg.to_string(),
                "Check the connection string format and credentials",
            ),
            sqlx::Error::Database(db_err) => {
                let code = db_err.code().map(|c| c.to_string());
                DbError::query(
                    db_err.message(),
                    code,
                    "Check the SQL syntax and referenced objects",
                )
            }
            sqlx::Error::RowNotFound => DbError::query(
                "No rows returned",
                None,
                "Verify the query conditions match existing data",
            ),
            sqlx::Error::PoolTimedOut => DbError::timeout("connection pool acquire", 30),
            sqlx::Error::PoolClosed => {
                DbError::connection("Connection pool is closed", "Reconnect to the database")
            }
            sqlx::Error::Io(io_err) => DbError::connection(
                format!("I/O error: {}", io_err),
                "Check network connectivity and database server status",
            ),
            sqlx::Error::Tls(tls_err) => DbError::connection(
                format!("TLS error: {}", tls_err),
                "Verify TLS configuration and certificates",
            ),
            sqlx::Error::Protocol(msg) => DbError::connection(
                format!("Protocol error: {}", msg),
                "Check database server compatibility",
            ),
            sqlx::Error::TypeNotFound { type_name } => DbError::schema(
                format!("Type not found: {}", type_name),
                type_name.to_string(),
            ),
            sqlx::Error::ColumnNotFound(col) => {
                DbError::schema(format!("Column not found: {}", col), col.to_string())
            }
            sqlx::Error::ColumnIndexOutOfBounds { index, len } => DbError::internal(format!(
                "Column index {} out of bounds (len: {})",
                index, len
            )),
            sqlx::Error::ColumnDecode { index, source } => {
                DbError::internal(format!("Failed to decode column {}: {}", index, source))
            }
            sqlx::Error::Decode(source) => DbError::internal(format!("Decode error: {}", source)),
            sqlx::Error::AnyDriverError(err) => DbError::connection(
                format!("Driver error: {}", err),
                "Check database driver configuration",
            ),
            sqlx::Error::Migrate(_) => DbError::internal("Migration error"),
            sqlx::Error::WorkerCrashed => DbError::internal("Database worker crashed"),
            _ => DbError::internal(format!("Unknown database error: {}", err)),
        }
    }
}

/// Result type alias for database operations.
pub type DbResult<T> = Result<T, DbError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = DbError::connection("Failed to connect", "Check credentials");
        assert!(err.to_string().contains("Connection failed"));
    }

    #[test]
    fn test_error_suggestion() {
        let err = DbError::query(
            "Syntax error",
            Some("42601".to_string()),
            "Check SQL syntax",
        );
        assert_eq!(err.suggestion(), Some("Check SQL syntax"));
    }

    #[test]
    fn test_error_retryable() {
        assert!(DbError::timeout("query", 30).is_retryable());
        assert!(DbError::connection("err", "sugg").is_retryable());
        assert!(!DbError::permission("write", "read-only").is_retryable());
    }
}
