//! Connection-related data models.
//!
//! This module defines types for database connection configuration and state.

use crate::config::PoolOptions;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Supported database types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum DatabaseType {
    PostgreSQL,
    /// Includes MariaDB
    MySQL,
    SQLite,
}

impl DatabaseType {
    /// Parse database type from a connection string.
    pub fn from_connection_string(connection_string: &str) -> Option<Self> {
        let lower = connection_string.to_lowercase();
        if lower.starts_with("postgres://") || lower.starts_with("postgresql://") {
            Some(Self::PostgreSQL)
        } else if lower.starts_with("mysql://") || lower.starts_with("mariadb://") {
            Some(Self::MySQL)
        } else if lower.starts_with("sqlite://") || lower.starts_with("sqlite:") {
            Some(Self::SQLite)
        } else {
            None
        }
    }

    /// Get the display name for this database type.
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::PostgreSQL => "PostgreSQL",
            Self::MySQL => "MySQL",
            Self::SQLite => "SQLite",
        }
    }

    /// Get the default port for this database type.
    pub fn default_port(&self) -> Option<u16> {
        match self {
            Self::PostgreSQL => Some(5432),
            Self::MySQL => Some(3306),
            Self::SQLite => None,
        }
    }
}

impl std::fmt::Display for DatabaseType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display_name())
    }
}

/// Configuration for a database connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionConfig {
    pub id: String,
    pub db_type: DatabaseType,
    /// Contains sensitive data - never log
    #[serde(skip_serializing)]
    pub connection_string: String,
    /// Default: false for safety
    #[serde(default)]
    pub writable: bool,
    /// True if connection is at server level (no specific database in URL)
    #[serde(default)]
    pub server_level: bool,
    /// Database name extracted from connection URL. None for server-level connections.
    pub database: Option<String>,
    /// Connection pool configuration options.
    #[serde(default)]
    pub pool_options: PoolOptions,
}

impl ConnectionConfig {
    /// Create a new connection configuration.
    pub fn new(
        id: impl Into<String>,
        connection_string: impl Into<String>,
        writable: bool,
        server_level: bool,
        database: Option<String>,
        pool_options: PoolOptions,
    ) -> Result<Self, ConnectionConfigError> {
        let id = id.into();
        let connection_string = connection_string.into();

        // Validate ID
        if id.is_empty() {
            return Err(ConnectionConfigError::EmptyId);
        }
        if !id
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
        {
            return Err(ConnectionConfigError::InvalidId(id));
        }

        // Detect database type
        let db_type = DatabaseType::from_connection_string(&connection_string)
            .ok_or_else(|| ConnectionConfigError::UnknownDatabaseType(connection_string.clone()))?;

        Ok(Self {
            id,
            db_type,
            connection_string,
            writable,
            server_level,
            database,
            pool_options,
        })
    }

    /// Get a display-safe version of the connection string (credentials masked).
    pub fn masked_connection_string(&self) -> String {
        // Simple masking: replace password in URL
        if let Some(at_pos) = self.connection_string.find('@') {
            if let Some(colon_pos) = self.connection_string[..at_pos].rfind(':') {
                let prefix = &self.connection_string[..colon_pos + 1];
                let suffix = &self.connection_string[at_pos..];
                return format!("{}****{}", prefix, suffix);
            }
        }
        self.connection_string.clone()
    }
}

/// Errors that can occur when creating a connection configuration.
#[derive(Debug, thiserror::Error)]
pub enum ConnectionConfigError {
    /// Connection ID is empty
    #[error("Connection ID cannot be empty")]
    EmptyId,

    /// Connection ID contains invalid characters
    #[error("Connection ID contains invalid characters: {0}")]
    InvalidId(String),

    /// Could not determine database type from connection string
    #[error("Unknown database type in connection string: {0}")]
    UnknownDatabaseType(String),
}

/// Information about an active connection, returned after successful connection.
#[derive(Debug, Clone, Serialize)]
pub struct ConnectionInfo {
    pub connection_id: String,
    pub database_type: DatabaseType,
    pub server_version: Option<String>,
    pub writable: bool,
    /// True if connection is at server level (no specific database in URL)
    pub server_level: bool,
    /// Database name extracted from connection URL. None for server-level connections.
    pub database: Option<String>,
}

/// Transaction state for active transactions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionState {
    Active,
    Committed,
    RolledBack,
    /// Automatically rolled back due to timeout
    TimedOut,
}

impl TransactionState {
    /// Check if the transaction is still active.
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Active)
    }

    /// Check if the transaction has ended (committed, rolled back, or timed out).
    pub fn is_ended(&self) -> bool {
        !self.is_active()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TransactionInfo {
    pub id: String,
    pub connection_id: String,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub timeout_secs: u32,
    pub state: TransactionState,
}

impl TransactionInfo {
    /// Check if the transaction has timed out.
    pub fn is_timed_out(&self) -> bool {
        if self.state != TransactionState::Active {
            return false;
        }
        let elapsed = chrono::Utc::now()
            .signed_duration_since(self.started_at)
            .num_seconds();
        elapsed >= self.timeout_secs as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_type_from_connection_string() {
        assert_eq!(
            DatabaseType::from_connection_string("postgres://localhost/db"),
            Some(DatabaseType::PostgreSQL)
        );
        assert_eq!(
            DatabaseType::from_connection_string("postgresql://localhost/db"),
            Some(DatabaseType::PostgreSQL)
        );
        assert_eq!(
            DatabaseType::from_connection_string("mysql://localhost/db"),
            Some(DatabaseType::MySQL)
        );
        assert_eq!(
            DatabaseType::from_connection_string("sqlite:test.db"),
            Some(DatabaseType::SQLite)
        );
        assert_eq!(
            DatabaseType::from_connection_string("sqlite://path/to/db"),
            Some(DatabaseType::SQLite)
        );
        assert_eq!(
            DatabaseType::from_connection_string("unknown://localhost"),
            None
        );
    }

    #[test]
    fn test_connection_config_new() {
        let config = ConnectionConfig::new(
            "test-conn",
            "postgres://user:pass@localhost:5432/db",
            true,
            false,
            Some("db".to_string()),
            PoolOptions::default(),
        )
        .unwrap();

        assert_eq!(config.id, "test-conn");
        assert_eq!(config.db_type, DatabaseType::PostgreSQL);
        assert!(config.writable);
        assert!(!config.server_level);
        assert_eq!(config.database, Some("db".to_string()));
    }

    #[test]
    fn test_connection_config_masked_string() {
        let config = ConnectionConfig::new(
            "test",
            "postgres://user:secret@localhost:5432/db",
            true,
            false,
            Some("db".to_string()),
            PoolOptions::default(),
        )
        .unwrap();

        let masked = config.masked_connection_string();
        assert!(!masked.contains("secret"));
        assert!(masked.contains("****"));
    }

    #[test]
    fn test_connection_config_empty_id() {
        let result = ConnectionConfig::new(
            "",
            "postgres://localhost/db",
            true,
            false,
            None,
            PoolOptions::default(),
        );
        assert!(matches!(result, Err(ConnectionConfigError::EmptyId)));
    }

    #[test]
    fn test_connection_config_invalid_id() {
        let result = ConnectionConfig::new(
            "test conn",
            "postgres://localhost/db",
            true,
            false,
            None,
            PoolOptions::default(),
        );
        assert!(matches!(result, Err(ConnectionConfigError::InvalidId(_))));
    }

    #[test]
    fn test_transaction_state() {
        assert!(TransactionState::Active.is_active());
        assert!(!TransactionState::Committed.is_active());
        assert!(TransactionState::Committed.is_ended());
    }
}
