//! Connection pool management.
//!
//! This module provides connection pooling functionality using database-specific
//! pools (MySqlPool, PgPool, SqlitePool) to ensure full type support.

use crate::db::database_pool::{DatabasePoolConfig, DatabasePoolManager, DatabaseTarget};
use crate::error::{DbError, DbResult};
use crate::models::{ConnectionConfig, ConnectionInfo, DatabaseType};
use sqlx::{
    MySqlPool, PgPool, SqlitePool, mysql::MySqlConnectOptions, mysql::MySqlPoolOptions,
    postgres::PgPoolOptions, sqlite::SqliteConnectOptions, sqlite::SqlitePoolOptions,
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

/// Connection pool type - either a direct database connection or a server-level manager.
enum ConnectionPool {
    /// Direct connection to a specific database.
    Database {
        pool: DbPool,
        server_version: Option<String>,
        /// Override manager for database-specific pools when using database parameter.
        /// Lazily created on first database override request.
        override_manager: Option<Arc<DatabasePoolManager>>,
    },
    /// Server-level connection with lazy per-database pool creation.
    ServerLevel(Arc<DatabasePoolManager>),
}

impl ConnectionPool {
    /// Close all pools including any override managers.
    async fn close(&self) {
        match self {
            ConnectionPool::Database {
                pool,
                override_manager,
                ..
            } => {
                pool.close().await;
                if let Some(manager) = override_manager {
                    manager.close_all().await;
                }
            }
            ConnectionPool::ServerLevel(manager) => manager.close_all().await,
        }
    }
}

impl std::fmt::Debug for ConnectionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionPool::Database {
                pool,
                server_version,
                override_manager,
            } => f
                .debug_struct("Database")
                .field("pool", pool)
                .field("server_version", server_version)
                .field("has_override_manager", &override_manager.is_some())
                .finish(),
            ConnectionPool::ServerLevel(_) => f
                .debug_struct("ServerLevel")
                .field("manager", &"DatabasePoolManager")
                .finish(),
        }
    }
}

#[derive(Debug)]
struct PoolEntry {
    connection: ConnectionPool,
    config: ConnectionConfig,
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

        // Early check for existing connection
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
            server_level = %config.server_level,
            "Connecting to database"
        );

        let (connection, server_version) = if config.server_level {
            // Server-level connection: create DatabasePoolManager
            let db_pool_config = Self::create_database_pool_config(&config);
            let manager = DatabasePoolManager::new(db_pool_config);
            info!(
                connection_id = %connection_id,
                "Created database pool manager for server-level connection"
            );
            (ConnectionPool::ServerLevel(manager), None)
        } else {
            // Regular connection: create pool directly
            let pool = self.create_pool(&config).await?;
            let server_version = self.get_server_version(&pool).await;
            (
                ConnectionPool::Database {
                    pool,
                    server_version: server_version.clone(),
                    override_manager: None,
                },
                server_version,
            )
        };

        // Re-check after async work to prevent TOCTOU race
        // If duplicate detected, return the connection so we can close it outside the lock
        let maybe_connection_to_close: Option<ConnectionPool> = {
            let mut pools = self.pools.write().await;
            if pools.contains_key(&connection_id) {
                Some(connection)
            } else {
                let entry = PoolEntry {
                    connection,
                    config: config.clone(),
                };
                pools.insert(connection_id.clone(), entry);
                None
            }
        }; // Lock released here

        if let Some(conn_to_close) = maybe_connection_to_close {
            // Close the pool we just created outside of lock
            conn_to_close.close().await;
            return Err(DbError::connection(
                format!("Connection '{}' already exists", connection_id),
                "Concurrent connection attempt detected. Try again with a different ID.",
            ));
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
    /// For server-level connections, this returns an error - use get_pool_for_database instead.
    pub async fn get_pool(&self, connection_id: &str) -> DbResult<DbPool> {
        let pools = self.pools.read().await;
        match pools.get(connection_id) {
            Some(entry) => match &entry.connection {
                ConnectionPool::Database { pool, .. } => Ok(pool.clone()),
                ConnectionPool::ServerLevel(_) => Err(DbError::database_required(connection_id)),
            },
            None => Err(DbError::connection_not_found(connection_id)),
        }
    }

    /// Get a connection pool for a specific database.
    /// For server-level connections, uses the database pool manager.
    /// For regular connections, if database matches or is None, returns the default pool.
    /// If database differs, creates a database pool for the override.
    pub async fn get_pool_for_database(
        &self,
        connection_id: &str,
        database: Option<&str>,
    ) -> DbResult<DbPool> {
        // Determine what we need while holding the lock briefly
        enum PoolSource {
            Direct(DbPool),
            Manager(Arc<DatabasePoolManager>, DatabaseTarget),
            NeedOverride(String), // database name to override
            Error(DbError),
        }

        let source = {
            let pools = self.pools.read().await;
            let entry = match pools.get(connection_id) {
                Some(e) => e,
                None => return Err(DbError::connection_not_found(connection_id)),
            };

            match &entry.connection {
                ConnectionPool::ServerLevel(manager) => {
                    match DatabaseTarget::from_option(database) {
                        Ok(target) => PoolSource::Manager(Arc::clone(manager), target),
                        Err(e) => PoolSource::Error(e),
                    }
                }
                ConnectionPool::Database { pool, .. } => match database {
                    None => PoolSource::Direct(pool.clone()),
                    Some(db) if entry.config.database.as_deref() == Some(db) => {
                        PoolSource::Direct(pool.clone())
                    }
                    Some("") => PoolSource::Error(DbError::invalid_input(
                        "Database name cannot be empty. Omit the database parameter to use the default database.",
                    )),
                    Some(db) => PoolSource::NeedOverride(db.to_string()),
                },
            }
        }; // Read lock released here

        match source {
            PoolSource::Direct(pool) => Ok(pool),
            PoolSource::Error(e) => Err(e),
            PoolSource::Manager(manager, target) => {
                // Await outside of lock
                manager.get_or_create_pool(&target).await
            }
            PoolSource::NeedOverride(db) => {
                // Get or create override manager with brief write lock
                let manager = {
                    let mut pools = self.pools.write().await;
                    let entry = pools
                        .get_mut(connection_id)
                        .ok_or_else(|| DbError::connection_not_found(connection_id))?;

                    match &mut entry.connection {
                        ConnectionPool::ServerLevel(m) => Arc::clone(m),
                        ConnectionPool::Database {
                            override_manager, ..
                        } => {
                            let manager = override_manager.get_or_insert_with(|| {
                                let db_pool_config =
                                    Self::create_database_pool_config(&entry.config);
                                DatabasePoolManager::new(db_pool_config)
                            });
                            Arc::clone(manager)
                        }
                    }
                }; // Write lock released here

                let target = DatabaseTarget::Database(db);
                manager.get_or_create_pool(&target).await
            }
        }
    }

    /// Release a database pool after use (decrements active count).
    ///
    /// Call this after completing an operation that acquired a pool via get_pool_for_database.
    pub async fn release_pool_for_database(&self, connection_id: &str, database: Option<&str>) {
        // Clone manager under brief lock, then release outside lock
        let release_info: Option<(Arc<DatabasePoolManager>, DatabaseTarget)> = {
            let pools = self.pools.read().await;
            let Some(entry) = pools.get(connection_id) else {
                return;
            };

            match &entry.connection {
                ConnectionPool::ServerLevel(manager) => DatabaseTarget::from_option(database)
                    .ok()
                    .map(|target| (Arc::clone(manager), target)),
                ConnectionPool::Database {
                    override_manager: Some(manager),
                    ..
                } => database.map(|db| {
                    (
                        Arc::clone(manager),
                        DatabaseTarget::Database(db.to_string()),
                    )
                }),
                ConnectionPool::Database { .. } => None,
            }
        }; // Lock released here

        if let Some((manager, target)) = release_info {
            manager.release_pool(&target).await;
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
            entry.connection.close().await;
        }
        info!("All connections closed");
    }

    fn create_database_pool_config(config: &ConnectionConfig) -> DatabasePoolConfig {
        DatabasePoolConfig {
            base_connection_string: config.connection_string.clone(),
            db_type: config.db_type,
            pool_options: config.pool_options.clone(),
            idle_timeout: Duration::from_secs(
                config.pool_options.database_pool_idle_timeout_or_default(),
            ),
            cleanup_interval: Duration::from_secs(
                config
                    .pool_options
                    .database_pool_cleanup_interval_or_default(),
            ),
        }
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
                let mut options = SqliteConnectOptions::from_str(&config.connection_string)
                    .map_err(|e| {
                        DbError::connection(
                            format!("Invalid SQLite connection string: {}", e),
                            "Check the connection URL format: sqlite:path/to/db.sqlite",
                        )
                    })?;

                if config.writable {
                    options = options.create_if_missing(true).read_only(false);
                } else {
                    options = options.read_only(true);
                }

                let pool = SqlitePoolOptions::new()
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

/// RAII guard for database pool usage.
///
/// Automatically releases the pool (decrements active count) when dropped.
/// This ensures proper cleanup even if a panic occurs during pool usage.
///
/// # Usage
///
/// ```ignore
/// let guard = connection_manager
///     .get_pool_for_database_guarded(&connection_id, database)
///     .await?;
///
/// // Use the pool
/// let result = executor.execute_query(guard.pool(), &request).await;
///
/// // Explicit release (preferred) or rely on Drop
/// guard.release().await;
/// ```
///
/// # Runtime Shutdown Behavior
///
/// The `Drop` implementation spawns a tokio task to handle async cleanup.
/// If the tokio runtime is shutting down when `Drop` is called, the spawned
/// task may not execute. This is acceptable because:
/// - During shutdown, pool cleanup is handled by the runtime anyway
/// - The active count becomes irrelevant once the manager is dropped
///
/// For critical cleanup paths, always use `release().await` explicitly.
pub struct PoolGuard {
    pool: DbPool,
    connection_manager: Arc<ConnectionManager>,
    connection_id: String,
    database: Option<String>,
    released: bool,
}

impl std::fmt::Debug for PoolGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolGuard")
            .field("pool", &self.pool)
            .field("connection_id", &self.connection_id)
            .field("database", &self.database)
            .field("released", &self.released)
            .finish_non_exhaustive()
    }
}

impl PoolGuard {
    /// Create a new pool guard.
    fn new(
        pool: DbPool,
        connection_manager: Arc<ConnectionManager>,
        connection_id: String,
        database: Option<String>,
    ) -> Self {
        Self {
            pool,
            connection_manager,
            connection_id,
            database,
            released: false,
        }
    }

    /// Get a reference to the underlying pool.
    pub fn pool(&self) -> &DbPool {
        &self.pool
    }

    /// Explicitly release the pool (preferred over relying on Drop).
    ///
    /// This is more efficient than the Drop implementation as it doesn't
    /// need to spawn a task.
    pub async fn release(mut self) {
        self.released = true;
        self.connection_manager
            .release_pool_for_database(&self.connection_id, self.database.as_deref())
            .await;
    }
}

impl Drop for PoolGuard {
    fn drop(&mut self) {
        if self.released {
            return;
        }

        // Spawn a task to handle async release - this is for panic safety
        let connection_manager = Arc::clone(&self.connection_manager);
        let connection_id = self.connection_id.clone();
        let database = self.database.clone();

        tokio::spawn(async move {
            connection_manager
                .release_pool_for_database(&connection_id, database.as_deref())
                .await;
            warn!(
                connection_id = %connection_id,
                database = ?database,
                "Pool released via Drop - consider using explicit release()"
            );
        });
    }
}

impl ConnectionManager {
    /// Get a guarded connection pool for a specific database.
    ///
    /// Returns a `PoolGuard` that automatically releases the pool when dropped.
    /// This provides panic safety - the active count will be decremented even
    /// if an error or panic occurs during pool usage.
    ///
    /// For best performance, call `guard.release().await` explicitly when done
    /// rather than relying on the Drop implementation.
    pub async fn get_pool_for_database_guarded(
        self: &Arc<Self>,
        connection_id: &str,
        database: Option<&str>,
    ) -> DbResult<PoolGuard> {
        let pool = self.get_pool_for_database(connection_id, database).await?;
        Ok(PoolGuard::new(
            pool,
            Arc::clone(self),
            connection_id.to_string(),
            database.map(String::from),
        ))
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
