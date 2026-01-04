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

    #[error("Database error: {message}")]
    Database {
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

    #[error(
        "Dangerous operation blocked: {operation}. {reason}. To proceed, set 'dangerous_operation_allowed' to true."
    )]
    DangerousOperationBlocked { operation: String, reason: String },

    #[error("Database '{database}' not found: {hint}")]
    DatabaseNotFound { database: String, hint: String },

    #[error(
        "Database parameter required for server-level connection '{connection_id}'. Specify the 'database' parameter to target a specific database."
    )]
    DatabaseRequired { connection_id: String },
}

impl DbError {
    /// Create a connection error with a helpful suggestion.
    pub fn connection(message: impl Into<String>, suggestion: impl Into<String>) -> Self {
        Self::Connection {
            message: message.into(),
            suggestion: suggestion.into(),
        }
    }

    /// Create a database error with optional SQL state.
    pub fn database(
        message: impl Into<String>,
        sql_state: Option<String>,
        suggestion: impl Into<String>,
    ) -> Self {
        Self::Database {
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

    /// Create a dangerous operation blocked error.
    pub fn dangerous_operation_blocked(
        operation: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self::DangerousOperationBlocked {
            operation: operation.into(),
            reason: reason.into(),
        }
    }

    /// Create a database not found error.
    pub fn database_not_found(database: impl Into<String>, hint: impl Into<String>) -> Self {
        Self::DatabaseNotFound {
            database: database.into(),
            hint: hint.into(),
        }
    }

    /// Create a database required error.
    pub fn database_required(connection_id: impl Into<String>) -> Self {
        Self::DatabaseRequired {
            connection_id: connection_id.into(),
        }
    }

    /// Get the suggestion for this error, if available.
    pub fn suggestion(&self) -> Option<&str> {
        match self {
            Self::Connection { suggestion, .. } => Some(suggestion),
            Self::Database { suggestion, .. } => Some(suggestion),
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
                DbError::database(
                    db_err.message(),
                    code,
                    "Check the SQL syntax and referenced objects",
                )
            }
            sqlx::Error::RowNotFound => DbError::database(
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

/// Build suggestion data as JSON value.
fn suggestion_data(suggestion: Option<&str>) -> Option<serde_json::Value> {
    suggestion.map(|s| serde_json::json!({ "suggestion": s }))
}

/// Convert DbError to MCP ErrorData for semantic error categorization.
/// Includes the suggestion field in the `data` object when available.
impl From<DbError> for rmcp::ErrorData {
    fn from(err: DbError) -> Self {
        match &err {
            // InvalidInput, Permission, DangerousOperationBlocked, Schema -> invalid_params
            DbError::InvalidInput { .. } => {
                rmcp::ErrorData::invalid_params(err.to_string(), suggestion_data(err.suggestion()))
            }
            DbError::Permission { .. } => {
                rmcp::ErrorData::invalid_params(err.to_string(), suggestion_data(err.suggestion()))
            }
            DbError::DangerousOperationBlocked { .. } => {
                rmcp::ErrorData::invalid_params(err.to_string(), suggestion_data(err.suggestion()))
            }
            DbError::Schema { .. } => {
                rmcp::ErrorData::invalid_params(err.to_string(), suggestion_data(err.suggestion()))
            }

            // ConnectionNotFound, Transaction, DatabaseNotFound -> resource_not_found
            DbError::ConnectionNotFound { .. } => rmcp::ErrorData::resource_not_found(
                err.to_string(),
                suggestion_data(err.suggestion()),
            ),
            DbError::Transaction { .. } => rmcp::ErrorData::resource_not_found(
                err.to_string(),
                suggestion_data(err.suggestion()),
            ),
            DbError::DatabaseNotFound { hint, .. } => {
                rmcp::ErrorData::resource_not_found(err.to_string(), suggestion_data(Some(hint)))
            }

            // DatabaseRequired -> invalid_params
            DbError::DatabaseRequired { .. } => rmcp::ErrorData::invalid_params(
                err.to_string(),
                suggestion_data(Some(
                    "Specify the 'database' parameter to target a specific database",
                )),
            ),

            // Connection, Timeout -> internal_error (with implicit retryable flag)
            DbError::Connection { suggestion, .. } => {
                rmcp::ErrorData::internal_error(err.to_string(), suggestion_data(Some(suggestion)))
            }
            DbError::Timeout { .. } => rmcp::ErrorData::internal_error(
                err.to_string(),
                suggestion_data(Some(
                    "Consider increasing the timeout or optimizing the operation",
                )),
            ),

            // Database errors -> invalid_params with sql_state in message
            DbError::Database {
                message,
                sql_state,
                suggestion,
            } => {
                let msg = match sql_state {
                    Some(code) => format!("{} (SQLSTATE: {})", message, code),
                    None => message.clone(),
                };
                rmcp::ErrorData::invalid_params(msg, suggestion_data(Some(suggestion)))
            }

            // Internal -> internal_error
            DbError::Internal { .. } => {
                rmcp::ErrorData::internal_error(err.to_string(), suggestion_data(err.suggestion()))
            }
        }
    }
}

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
        let err = DbError::database(
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

    // Tests for From<DbError> for rmcp::ErrorData

    #[test]
    fn test_invalid_input_maps_to_invalid_params() {
        let err = DbError::invalid_input("bad input");
        let mcp_err: rmcp::ErrorData = err.into();
        // invalid_params uses -32602
        assert_eq!(mcp_err.code.0, -32602);
    }

    #[test]
    fn test_permission_maps_to_invalid_params() {
        let err = DbError::permission("write", "read-only");
        let mcp_err: rmcp::ErrorData = err.into();
        assert_eq!(mcp_err.code.0, -32602);
    }

    #[test]
    fn test_dangerous_operation_maps_to_invalid_params() {
        let err = DbError::dangerous_operation_blocked("DROP", "destructive");
        let mcp_err: rmcp::ErrorData = err.into();
        assert_eq!(mcp_err.code.0, -32602);
    }

    #[test]
    fn test_schema_maps_to_invalid_params() {
        let err = DbError::schema("Table not found", "users");
        let mcp_err: rmcp::ErrorData = err.into();
        assert_eq!(mcp_err.code.0, -32602);
    }

    #[test]
    fn test_connection_not_found_maps_to_resource_not_found() {
        let err = DbError::connection_not_found("conn1");
        let mcp_err: rmcp::ErrorData = err.into();
        // resource_not_found uses -32002 in rmcp
        assert_eq!(mcp_err.code.0, -32002);
    }

    #[test]
    fn test_transaction_maps_to_resource_not_found() {
        let err = DbError::transaction("not found", "tx_123");
        let mcp_err: rmcp::ErrorData = err.into();
        assert_eq!(mcp_err.code.0, -32002);
    }

    #[test]
    fn test_connection_maps_to_internal_error() {
        let err = DbError::connection("failed", "try again");
        let mcp_err: rmcp::ErrorData = err.into();
        // internal_error uses -32603
        assert_eq!(mcp_err.code.0, -32603);
    }

    #[test]
    fn test_timeout_maps_to_internal_error() {
        let err = DbError::timeout("query", 30);
        let mcp_err: rmcp::ErrorData = err.into();
        assert_eq!(mcp_err.code.0, -32603);
    }

    #[test]
    fn test_database_error_includes_sql_state() {
        let err = DbError::database("syntax error", Some("42601".to_string()), "check syntax");
        let mcp_err: rmcp::ErrorData = err.into();
        assert!(mcp_err.message.contains("42601"));
    }

    #[test]
    fn test_internal_maps_to_internal_error() {
        let err = DbError::internal("unknown error");
        let mcp_err: rmcp::ErrorData = err.into();
        assert_eq!(mcp_err.code.0, -32603);
    }

    #[test]
    fn test_connection_error_includes_suggestion_in_data() {
        let err = DbError::connection("failed", "try reconnecting");
        let mcp_err: rmcp::ErrorData = err.into();
        assert!(mcp_err.data.is_some());
        let data = mcp_err.data.unwrap();
        assert_eq!(data["suggestion"], "try reconnecting");
    }

    #[test]
    fn test_database_error_includes_suggestion_in_data() {
        let err = DbError::database("syntax error", Some("42601".to_string()), "check syntax");
        let mcp_err: rmcp::ErrorData = err.into();
        assert!(mcp_err.data.is_some());
        let data = mcp_err.data.unwrap();
        assert_eq!(data["suggestion"], "check syntax");
    }

    #[test]
    fn test_database_not_found_includes_hint_in_data() {
        let err = DbError::database_not_found("mydb", "verify database exists");
        let mcp_err: rmcp::ErrorData = err.into();
        assert!(mcp_err.data.is_some());
        let data = mcp_err.data.unwrap();
        assert_eq!(data["suggestion"], "verify database exists");
    }
}
