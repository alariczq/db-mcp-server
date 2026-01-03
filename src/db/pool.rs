//! Connection pool management.
//!
//! This module provides connection pooling functionality using database-specific
//! pools (MySqlPool, PgPool, SqlitePool) to ensure full type support.

use crate::error::{DbError, DbResult};
use crate::models::{ConnectionConfig, ConnectionInfo, DatabaseType};
use sqlx::{
    MySqlPool, PgPool, SqlitePool, mysql::MySqlConnectOptions, mysql::MySqlPoolOptions,
    postgres::PgPoolOptions, sqlite::SqlitePoolOptions,
};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Connection information returned by list_connections (no secrets exposed).
#[derive(Debug, Clone, serde::Serialize, schemars::JsonSchema)]
pub struct ConnectionSummary {
    /// Connection identifier. Use this value in connection_id parameter for all tool calls.
    pub id: String,
    /// Database type: "postgresql", "mysql", or "sqlite"
    pub db_type: DatabaseType,
    /// If true, connection allows write operations. If false, only read operations allowed.
    pub writable: bool,
    /// If true, connection is at server level (no database in URL). Requires schema parameter for list_tables/describe_table.
    pub server_level: bool,
    /// Database name from connection URL. Only present when a specific database is targeted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
}

/// Database-specific connection pool (avoids AnyPool limitations).
#[derive(Debug, Clone)]
pub enum DbPool {
    MySql(MySqlPool),
    Postgres(PgPool),
    SQLite(SqlitePool),
}

impl DbPool {
    /// Close the connection pool.
    pub async fn close(&self) {
        match self {
            DbPool::MySql(pool) => pool.close().await,
            DbPool::Postgres(pool) => pool.close().await,
            DbPool::SQLite(pool) => pool.close().await,
        }
    }

    /// Get the database type for this pool.
    pub fn db_type(&self) -> DatabaseType {
        match self {
            DbPool::MySql(_) => DatabaseType::MySQL,
            DbPool::Postgres(_) => DatabaseType::PostgreSQL,
            DbPool::SQLite(_) => DatabaseType::SQLite,
        }
    }
}

#[derive(Debug)]
struct PoolEntry {
    pool: DbPool,
    config: ConnectionConfig,
    #[allow(dead_code)]
    server_version: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ConnectionManager {
    pools: Arc<RwLock<HashMap<String, PoolEntry>>>,
}

impl ConnectionManager {
    /// Create a new connection manager.
    pub fn new() -> Self {
        Self {
            pools: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Connect to a database and register the pool.
    pub async fn connect(&self, config: ConnectionConfig) -> DbResult<ConnectionInfo> {
        let connection_id = config.id.clone();
        let db_type = config.db_type;

        {
            let pools = self.pools.read().await;
            if pools.contains_key(&connection_id) {
                return Err(DbError::connection(
                    format!("Connection '{}' already exists", connection_id),
                    "Disconnect first or use a different connection ID",
                ));
            }
        }

        info!(
            connection_id = %connection_id,
            db_type = %db_type,
            "Connecting to database"
        );

        let pool = self.create_pool(&config).await?;
        let server_version = self.get_server_version(&pool).await;

        let entry = PoolEntry {
            pool,
            config: config.clone(),
            server_version: server_version.clone(),
        };

        {
            let mut pools = self.pools.write().await;
            pools.insert(connection_id.clone(), entry);
        }

        info!(
            connection_id = %connection_id,
            server_version = ?server_version,
            "Connected successfully"
        );

        Ok(ConnectionInfo {
            connection_id,
            database_type: db_type,
            server_version,
            writable: config.writable,
            server_level: config.server_level,
            database: config.database,
        })
    }

    /// Get a connection pool by ID.
    pub async fn get_pool(&self, connection_id: &str) -> DbResult<DbPool> {
        let pools = self.pools.read().await;
        match pools.get(connection_id) {
            Some(entry) => Ok(entry.pool.clone()),
            None => Err(DbError::connection_not_found(connection_id)),
        }
    }

    /// Get the configuration for a connection.
    pub async fn get_config(&self, connection_id: &str) -> DbResult<ConnectionConfig> {
        let pools = self.pools.read().await;
        match pools.get(connection_id) {
            Some(entry) => Ok(entry.config.clone()),
            None => Err(DbError::connection_not_found(connection_id)),
        }
    }

    /// Check if a connection allows write operations.
    pub async fn is_writable(&self, connection_id: &str) -> DbResult<bool> {
        let pools = self.pools.read().await;
        match pools.get(connection_id) {
            Some(entry) => Ok(entry.config.writable),
            None => Err(DbError::connection_not_found(connection_id)),
        }
    }

    /// List all active connection IDs.
    pub async fn list_connections(&self) -> Vec<String> {
        let pools = self.pools.read().await;
        pools.keys().cloned().collect()
    }

    /// Check if a connection exists.
    pub async fn exists(&self, connection_id: &str) -> bool {
        let pools = self.pools.read().await;
        pools.contains_key(connection_id)
    }

    /// List all active connections with details.
    pub async fn list_connections_detail(&self) -> Vec<ConnectionSummary> {
        let pools = self.pools.read().await;
        pools
            .values()
            .map(|entry| ConnectionSummary {
                id: entry.config.id.clone(),
                db_type: entry.config.db_type,
                writable: entry.config.writable,
                server_level: entry.config.server_level,
                database: entry.config.database.clone(),
            })
            .collect()
    }

    /// Get the number of active connections.
    pub async fn connection_count(&self) -> usize {
        let pools = self.pools.read().await;
        pools.len()
    }

    /// Close all connections and clear the pool.
    pub async fn close_all(&self) {
        let mut pools = self.pools.write().await;
        for (id, entry) in pools.drain() {
            info!(connection_id = %id, "Closing connection");
            entry.pool.close().await;
        }
        info!("All connections closed");
    }

    /// Create a connection pool for the given configuration.
    async fn create_pool(&self, config: &ConnectionConfig) -> DbResult<DbPool> {
        let pool_opts = &config.pool_options;
        let is_sqlite = config.db_type == DatabaseType::SQLite;
        let acquire_timeout = Duration::from_secs(pool_opts.acquire_timeout_or_default());
        let idle_timeout = Some(Duration::from_secs(pool_opts.idle_timeout_or_default()));

        match config.db_type {
            DatabaseType::MySQL => {
                let options = MySqlConnectOptions::from_str(&config.connection_string)
                    .map_err(|e| {
                        DbError::connection(
                            format!("Invalid MySQL connection string: {}", e),
                            "Check the connection URL format: mysql://user:pass@host:port/database",
                        )
                    })?
                    .charset("utf8mb4");

                let pool = MySqlPoolOptions::new()
                    .min_connections(pool_opts.min_connections_or_default())
                    .max_connections(pool_opts.max_connections_or_default(is_sqlite))
                    .acquire_timeout(acquire_timeout)
                    .idle_timeout(idle_timeout)
                    .test_before_acquire(pool_opts.test_before_acquire_or_default())
                    .connect_with(options)
                    .await
                    .map_err(|e| {
                        DbError::connection(
                            format!("Failed to connect: {}", e),
                            self.connection_suggestion(config.db_type, &e),
                        )
                    })?;
                Ok(DbPool::MySql(pool))
            }
            DatabaseType::PostgreSQL => {
                let pool = PgPoolOptions::new()
                    .min_connections(pool_opts.min_connections_or_default())
                    .max_connections(pool_opts.max_connections_or_default(is_sqlite))
                    .acquire_timeout(acquire_timeout)
                    .idle_timeout(idle_timeout)
                    .test_before_acquire(pool_opts.test_before_acquire_or_default())
                    .connect(&config.connection_string)
                    .await
                    .map_err(|e| {
                        DbError::connection(
                            format!("Failed to connect: {}", e),
                            self.connection_suggestion(config.db_type, &e),
                        )
                    })?;
                Ok(DbPool::Postgres(pool))
            }
            DatabaseType::SQLite => {
                let pool = SqlitePoolOptions::new()
                    .min_connections(pool_opts.min_connections_or_default())
                    .max_connections(pool_opts.max_connections_or_default(is_sqlite))
                    .acquire_timeout(acquire_timeout)
                    .idle_timeout(idle_timeout)
                    .test_before_acquire(pool_opts.test_before_acquire_or_default())
                    .connect(&config.connection_string)
                    .await
                    .map_err(|e| {
                        DbError::connection(
                            format!("Failed to connect: {}", e),
                            self.connection_suggestion(config.db_type, &e),
                        )
                    })?;
                Ok(DbPool::SQLite(pool))
            }
        }
    }

    /// Get the server version from the connected database.
    async fn get_server_version(&self, pool: &DbPool) -> Option<String> {
        match pool {
            DbPool::MySql(pool) => {
                match sqlx::query_scalar::<_, String>("SELECT version()")
                    .fetch_one(pool)
                    .await
                {
                    Ok(version) => {
                        debug!(version = %version, "Got server version");
                        Some(version)
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to get server version");
                        None
                    }
                }
            }
            DbPool::Postgres(pool) => {
                match sqlx::query_scalar::<_, String>("SELECT version()")
                    .fetch_one(pool)
                    .await
                {
                    Ok(version) => {
                        debug!(version = %version, "Got server version");
                        Some(version)
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to get server version");
                        None
                    }
                }
            }
            DbPool::SQLite(pool) => {
                match sqlx::query_scalar::<_, String>("SELECT sqlite_version()")
                    .fetch_one(pool)
                    .await
                {
                    Ok(version) => {
                        debug!(version = %version, "Got server version");
                        Some(version)
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to get server version");
                        None
                    }
                }
            }
        }
    }

    /// Generate a helpful suggestion for connection errors.
    fn connection_suggestion(&self, db_type: DatabaseType, error: &sqlx::Error) -> String {
        let error_str = error.to_string().to_lowercase();

        if error_str.contains("connection refused") {
            return format!(
                "Check that the {} server is running and accessible",
                db_type
            );
        }

        if error_str.contains("authentication") || error_str.contains("password") {
            return "Verify the username and password in the connection string".to_string();
        }

        if error_str.contains("does not exist") || error_str.contains("unknown database") {
            return "Check that the database name exists".to_string();
        }

        if error_str.contains("tls") || error_str.contains("ssl") {
            return "Check TLS/SSL configuration or try disabling it".to_string();
        }

        match db_type {
            DatabaseType::PostgreSQL => {
                "Verify the connection string format: postgres://user:pass@host:5432/db".to_string()
            }
            DatabaseType::MySQL => {
                "Verify the connection string format: mysql://user:pass@host:3306/db".to_string()
            }
            DatabaseType::SQLite => {
                "Verify the file path exists and is accessible: sqlite:path/to/db.sqlite"
                    .to_string()
            }
        }
    }
}

impl Default for ConnectionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_connection_manager_creation() {
        let manager = ConnectionManager::new();
        assert_eq!(manager.connection_count().await, 0);
    }

    #[tokio::test]
    async fn test_connection_not_found() {
        let manager = ConnectionManager::new();
        let result = manager.get_pool("nonexistent").await;
        assert!(matches!(result, Err(DbError::ConnectionNotFound { .. })));
    }

    #[tokio::test]
    async fn test_list_connections_empty() {
        let manager = ConnectionManager::new();
        let connections = manager.list_connections().await;
        assert!(connections.is_empty());
    }
}
