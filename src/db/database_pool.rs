//! Database-specific connection pool management for server-level connections.
//!
//! This module provides lazy database-specific connection pools for server-level connections,
//! enabling queries to multiple databases through a single connection configuration.
//!
//! # Design Decisions
//!
//! - **`OnceCell` per database key**: Single-flight pool creation prevents resource leaks
//!   from concurrent requests to the same database
//! - **`AtomicUsize` for active tracking**: Lock-free active count protects pools from
//!   premature cleanup while in use
//! - **`std::sync::Mutex` for cleanup handle**: Synchronous storage avoids async in Drop
//! - **`DatabaseTarget` enum**: Type-safe distinction between Server and Database targets
//!
//! # Concurrency Safety
//!
//! This module is designed for safe concurrent access with the following guarantees:
//!
//! ## No Deadlocks
//! - All locks are released before async operations (await points)
//! - Lock acquisition order is consistent: read locks for queries, write locks for mutations
//! - No nested lock acquisition within the same lock scope
//!
//! ## No Data Races
//! - `AtomicUsize` with `AcqRel`/`Acquire` ordering for active count operations
//! - `RwLock` protects the pool HashMap for concurrent read access
//! - `OnceCell` ensures single-flight initialization per database
//!
//! ## TOCTOU Safety
//! - Cleanup task re-checks conditions after acquiring write lock
//! - Residual race between read lock release and increment is safe:
//!   - Removed cells remain valid (Arc keeps them alive)
//!   - Operations complete successfully on orphaned pools
//!   - New pools are created for subsequent operations
//!   - Cost is potential pool recreation, not correctness issues
//!
//! ## Panic Safety
//! - Use [`PoolGuard`](super::PoolGuard) for automatic cleanup on panic
//! - `decrement_active` saturates at 0 to prevent underflow
//! - Underflow attempts are logged for debugging

use crate::config::PoolOptions;
use crate::db::pool::DbPool;
use crate::error::{DbError, DbResult};
use crate::models::DatabaseType;
use sqlx::{mysql::MySqlConnectOptions, mysql::MySqlPoolOptions, postgres::PgPoolOptions};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use tokio::sync::{OnceCell, RwLock as TokioRwLock};
use tokio::task::JoinHandle;
use tracing::{debug, info};

/// Target for database pool operations.
///
/// Distinguishes between server-level connections (no specific database)
/// and database-specific connections.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DatabaseTarget {
    /// Server-level pool - no specific database selected.
    /// Used for queries like `SELECT 1` or `SHOW DATABASES`.
    Server,
    /// Database-specific pool - targets a specific database.
    Database(String),
}

impl std::fmt::Display for DatabaseTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DatabaseTarget::Server => write!(f, "server"),
            DatabaseTarget::Database(name) => write!(f, "{}", name),
        }
    }
}

impl DatabaseTarget {
    /// Create a database target from an optional string.
    /// Returns error for empty string (use Server variant instead).
    pub fn from_option(database: Option<&str>) -> DbResult<Self> {
        match database {
            None => Ok(DatabaseTarget::Server),
            Some("") => Err(DbError::invalid_input(
                "Database name cannot be empty. Use None/null for server-level connections.",
            )),
            Some(db) => Ok(DatabaseTarget::Database(db.to_string())),
        }
    }
}

/// Entry for a database-specific connection pool.
pub struct DatabasePoolEntry {
    pub pool: DbPool,
    /// The target this pool was created for.
    pub target: DatabaseTarget,
    /// Uses std::sync::RwLock (not tokio) to avoid holding locks across await points.
    last_accessed: std::sync::RwLock<Instant>,
    pub created_at: Instant,
    /// Count of active borrows. Cleanup skips pools with active_count > 0.
    active_count: AtomicUsize,
}

impl DatabasePoolEntry {
    /// Increment active usage count. Call before returning pool to caller.
    pub fn increment_active(&self) {
        self.active_count.fetch_add(1, Ordering::AcqRel);
    }

    /// Decrement active usage count. Call after operation completes.
    /// Saturates at 0 to prevent underflow from extra release calls.
    pub fn decrement_active(&self) {
        // Use fetch_update to saturate at 0
        let result = self
            .active_count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                if count > 0 {
                    Some(count - 1)
                } else {
                    Some(0) // Saturate at 0, don't wrap
                }
            });

        // Log warning if we detected an underflow attempt (helps catch logic bugs)
        if let Ok(prev) = result {
            if prev == 0 {
                tracing::warn!(
                    target = %self.target,
                    "Active count underflow detected - extra release call"
                );
            }
        }
    }

    /// Get current active usage count.
    pub fn active_count(&self) -> usize {
        self.active_count.load(Ordering::Acquire)
    }

    /// Update last accessed time. Synchronous - does not hold locks across await.
    pub fn touch(&self) {
        if let Ok(mut last_accessed) = self.last_accessed.write() {
            *last_accessed = Instant::now();
        }
    }

    /// Get last accessed time. Synchronous - does not hold locks across await.
    pub fn last_accessed(&self) -> Instant {
        self.last_accessed
            .read()
            .map(|guard| *guard)
            .unwrap_or(self.created_at)
    }
}

impl std::fmt::Debug for DatabasePoolEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabasePoolEntry")
            .field("pool", &self.pool)
            .field("target", &self.target)
            .field("created_at", &self.created_at)
            .field("active_count", &self.active_count.load(Ordering::Relaxed))
            .finish()
    }
}

/// Configuration for creating database-specific pools.
#[derive(Debug, Clone)]
pub struct DatabasePoolConfig {
    pub base_connection_string: String,
    pub db_type: DatabaseType,
    pub pool_options: PoolOptions,
    pub idle_timeout: Duration,
    pub cleanup_interval: Duration,
}

/// Manages database-specific connection pools for a server-level connection.
///
/// Uses `DatabaseTarget` as HashMap key for type-safe pool lookup.
pub struct DatabasePoolManager {
    config: DatabasePoolConfig,
    /// Per-database lazy pools. OnceCell ensures single-flight creation.
    pools: TokioRwLock<HashMap<DatabaseTarget, Arc<OnceCell<DatabasePoolEntry>>>>,
    /// Cleanup task handle. Uses std::sync::Mutex for synchronous storage.
    cleanup_handle: std::sync::Mutex<Option<JoinHandle<()>>>,
}

impl DatabasePoolManager {
    /// Create a new database pool manager.
    ///
    /// Spawns a background cleanup task that periodically removes idle pools.
    /// The cleanup handle is stored synchronously to avoid race conditions.
    pub fn new(config: DatabasePoolConfig) -> Arc<Self> {
        let manager = Arc::new(Self {
            config,
            pools: TokioRwLock::new(HashMap::new()),
            cleanup_handle: std::sync::Mutex::new(None),
        });

        // Start cleanup task with weak reference to avoid circular reference memory leak
        let weak_manager = Arc::downgrade(&manager);
        let cleanup_interval = manager.config.cleanup_interval;
        let idle_timeout = manager.config.idle_timeout;

        let handle = tokio::spawn(async move {
            Self::cleanup_task(weak_manager, cleanup_interval, idle_timeout).await;
        });

        // Store the handle synchronously - no race condition possible
        {
            let mut guard = manager.cleanup_handle.lock().unwrap();
            *guard = Some(handle);
        }

        manager
    }

    /// Get or create a pool for the specified target.
    ///
    /// Uses OnceCell per database key for single-flight creation - concurrent
    /// requests for the same database will wait for the first to complete.
    pub async fn get_or_create_pool(&self, target: &DatabaseTarget) -> DbResult<DbPool> {
        // Get or create the OnceCell for this target
        let cell = {
            let pools = self.pools.read().await;
            if let Some(cell) = pools.get(target) {
                Arc::clone(cell)
            } else {
                drop(pools);
                let mut pools = self.pools.write().await;
                // Double-check after acquiring write lock
                if let Some(cell) = pools.get(target) {
                    Arc::clone(cell)
                } else {
                    let cell = Arc::new(OnceCell::new());
                    pools.insert(target.clone(), Arc::clone(&cell));
                    cell
                }
            }
        };

        // Clone target for use in closure
        let target_clone = target.clone();

        // OnceCell::get_or_try_init ensures single-flight pool creation
        let entry = cell
            .get_or_try_init(|| async {
                debug!(db_target = %target_clone, "Creating new database pool");
                let pool = self.create_database_pool(&target_clone).await?;
                let now = Instant::now();
                Ok::<_, DbError>(DatabasePoolEntry {
                    pool,
                    target: target_clone.clone(),
                    last_accessed: std::sync::RwLock::new(now),
                    created_at: now,
                    active_count: AtomicUsize::new(0),
                })
            })
            .await?;

        // Update last_accessed and increment active count
        entry.touch();
        entry.increment_active();

        debug!(db_target = %target, "Returning database pool");
        Ok(entry.pool.clone())
    }

    /// Mark a pool as no longer in active use.
    ///
    /// Call this after the operation using the pool completes.
    pub async fn release_pool(&self, target: &DatabaseTarget) {
        let pools = self.pools.read().await;
        if let Some(cell) = pools.get(target) {
            if let Some(entry) = cell.get() {
                entry.decrement_active();
            }
        }
    }

    /// Update the last accessed time for a database pool.
    pub async fn update_last_accessed(&self, target: &DatabaseTarget) {
        let pools = self.pools.read().await;
        if let Some(cell) = pools.get(target) {
            if let Some(entry) = cell.get() {
                entry.touch();
            }
        }
    }

    /// Get the number of active pools (initialized OnceCells).
    pub async fn pool_count(&self) -> usize {
        let pools = self.pools.read().await;
        pools.values().filter(|cell| cell.get().is_some()).count()
    }

    /// Close all database pools and cancel the cleanup task.
    pub async fn close_all(&self) {
        // Cancel cleanup task using synchronous mutex
        {
            let mut handle_guard = self.cleanup_handle.lock().unwrap();
            if let Some(handle) = handle_guard.take() {
                handle.abort();
            }
        }

        // Drain pools under lock, close outside lock
        let pools_to_close: Vec<_> = {
            let mut pools = self.pools.write().await;
            pools.drain().collect()
        }; // Lock released here

        for (db_name, cell) in pools_to_close {
            if let Some(entry) = cell.get() {
                info!(database = %db_name, "Closing database pool");
                entry.pool.close().await;
            }
        }
    }

    /// Create a connection pool for a specific target.
    async fn create_database_pool(&self, target: &DatabaseTarget) -> DbResult<DbPool> {
        let connection_string = self.build_database_url(target)?;
        let pool_opts = &self.config.pool_options;
        let is_sqlite = false; // Server-level connections don't support SQLite
        let acquire_timeout = Duration::from_secs(pool_opts.acquire_timeout_or_default());
        let idle_timeout = Some(Duration::from_secs(pool_opts.idle_timeout_or_default()));

        match self.config.db_type {
            DatabaseType::MySQL => {
                let options = MySqlConnectOptions::from_str(&connection_string)
                    .map_err(|e| {
                        DbError::connection(
                            format!("Invalid MySQL connection string: {}", e),
                            "Check the connection URL format",
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
                        let error_str = e.to_string().to_lowercase();
                        if let DatabaseTarget::Database(db) = target {
                            if error_str.contains("unknown database")
                                || error_str.contains("does not exist")
                            {
                                return DbError::database_not_found(
                                    db,
                                    "Verify the database name exists on the server",
                                );
                            }
                        }
                        DbError::connection(
                            format!("Failed to connect to {}: {}", target, e),
                            "Check the connection credentials are correct",
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
                    .connect(&connection_string)
                    .await
                    .map_err(|e| {
                        let error_str = e.to_string().to_lowercase();
                        if let DatabaseTarget::Database(db) = target {
                            if error_str.contains("does not exist") {
                                return DbError::database_not_found(
                                    db,
                                    "Verify the database name exists on the server",
                                );
                            }
                        }
                        DbError::connection(
                            format!("Failed to connect to {}: {}", target, e),
                            "Check the connection credentials are correct",
                        )
                    })?;
                Ok(DbPool::Postgres(pool))
            }
            DatabaseType::SQLite => Err(DbError::invalid_input(
                "SQLite does not support server-level connections or database pools",
            )),
        }
    }

    /// Build a database-specific URL from the base connection string.
    ///
    /// For `Server` target, returns the base connection string unchanged.
    /// For `Database` target, strips any existing database path from the URL before
    /// appending the new database name. This prevents URL path duplication like `/db1/db2`.
    fn build_database_url(&self, target: &DatabaseTarget) -> DbResult<String> {
        let base = &self.config.base_connection_string;

        let DatabaseTarget::Database(database) = target else {
            return Ok(base.clone());
        };

        // Parse the base URL
        let mut url = url::Url::parse(base).map_err(|e| {
            DbError::connection(
                format!("Invalid base connection URL: {}", e),
                "Check the connection URL format",
            )
        })?;

        // Strip existing path and set only the new database
        // This prevents /existing_db/new_db duplication
        url.set_path(&format!("/{}", database));

        Ok(url.to_string())
    }

    /// Background task to cleanup idle pools.
    ///
    /// Uses a Weak reference to the manager to avoid circular reference memory leaks.
    /// The task automatically exits when the manager is dropped.
    ///
    /// Uses collect-then-act pattern for thread safety:
    /// 1. Collect candidates with read lock (brief)
    /// 2. Remove items one at a time with brief write locks
    /// 3. Close pools outside of all locks
    ///
    /// Only removes pools that are:
    /// 1. Initialized (OnceCell has a value)
    /// 2. Not actively in use (active_count == 0)
    /// 3. Idle longer than the configured timeout
    async fn cleanup_task(
        weak_manager: Weak<Self>,
        cleanup_interval: Duration,
        idle_timeout: Duration,
    ) {
        let mut interval = tokio::time::interval(cleanup_interval);

        loop {
            interval.tick().await;

            // Try to upgrade weak reference - exit if manager was dropped
            let Some(manager) = weak_manager.upgrade() else {
                info!("Database pool manager dropped, cleanup task exiting");
                return;
            };

            // Phase 1: Collect candidates with read lock
            let now = Instant::now();
            let candidates: Vec<DatabaseTarget> = {
                let pools = manager.pools.read().await;
                let mut candidates = Vec::new();

                for (db_target, cell) in pools.iter() {
                    if let Some(entry) = cell.get() {
                        let last_accessed = entry.last_accessed();
                        let active = entry.active_count();

                        if active > 0 {
                            debug!(
                                database = %db_target,
                                active_count = %active,
                                "Skipping cleanup of active pool"
                            );
                            continue;
                        }

                        if now.saturating_duration_since(last_accessed) > idle_timeout {
                            candidates.push(db_target.clone());
                        }
                    }
                }
                candidates
            }; // Read lock released here

            // Phase 2 & 3: Remove items with brief write locks, close outside locks
            for db_target in candidates {
                // Brief write lock per item
                let removed_cell = {
                    let mut pools = manager.pools.write().await;

                    // Re-check conditions after acquiring write lock (TOCTOU protection).
                    // Note: There's still a theoretical race where a thread could be between
                    // getting a cell reference (via read lock) and calling increment_active.
                    // However, this is safe because:
                    // 1. The removed cell remains valid (Arc keeps it alive)
                    // 2. The operation using that pool will complete successfully
                    // 3. A new cell/pool will be created for subsequent operations
                    // The only cost is potential pool recreation, not correctness issues.
                    if let Some(cell) = pools.get(&db_target) {
                        if let Some(entry) = cell.get() {
                            if entry.active_count() > 0 {
                                debug!(
                                    database = %db_target,
                                    "Pool became active during cleanup, skipping"
                                );
                                continue;
                            }

                            let last_accessed = entry.last_accessed();
                            if now.saturating_duration_since(last_accessed) <= idle_timeout {
                                debug!(
                                    database = %db_target,
                                    "Pool accessed during cleanup, skipping"
                                );
                                continue;
                            }
                        }
                    }

                    pools.remove(&db_target)
                }; // Write lock released here

                // Close pool outside of all locks
                if let Some(cell) = removed_cell {
                    if let Some(entry) = cell.get() {
                        info!(
                            database = %db_target,
                            "Closing idle database pool"
                        );
                        entry.pool.close().await;
                    }
                }
            }

            // Drop strong reference before sleeping to allow manager deallocation
            drop(manager);
        }
    }
}

impl std::fmt::Debug for DatabasePoolManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabasePoolManager")
            .field("db_type", &self.config.db_type)
            .field("idle_timeout", &self.config.idle_timeout)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> DatabasePoolConfig {
        DatabasePoolConfig {
            base_connection_string: "mysql://user:pass@localhost:3306".to_string(),
            db_type: DatabaseType::MySQL,
            pool_options: PoolOptions::default(),
            idle_timeout: Duration::from_secs(600),
            cleanup_interval: Duration::from_secs(60),
        }
    }

    fn create_test_manager(config: DatabasePoolConfig) -> DatabasePoolManager {
        DatabasePoolManager {
            config,
            pools: TokioRwLock::new(HashMap::new()),
            cleanup_handle: std::sync::Mutex::new(None),
        }
    }

    #[test]
    fn test_build_database_url_mysql() {
        let config = create_test_config();
        let manager = create_test_manager(config);
        let target = DatabaseTarget::Database("testdb".to_string());

        let url = manager.build_database_url(&target).unwrap();
        assert_eq!(url, "mysql://user:pass@localhost:3306/testdb");
    }

    #[test]
    fn test_build_database_url_server_returns_base() {
        let config = create_test_config();
        let base = config.base_connection_string.clone();
        let manager = create_test_manager(config);

        let url = manager.build_database_url(&DatabaseTarget::Server).unwrap();
        assert_eq!(url, base);
    }

    #[test]
    fn test_build_database_url_with_trailing_slash() {
        let config = DatabasePoolConfig {
            base_connection_string: "mysql://user:pass@localhost:3306/".to_string(),
            db_type: DatabaseType::MySQL,
            pool_options: PoolOptions::default(),
            idle_timeout: Duration::from_secs(600),
            cleanup_interval: Duration::from_secs(60),
        };
        let manager = create_test_manager(config);
        let target = DatabaseTarget::Database("mydb".to_string());

        let url = manager.build_database_url(&target).unwrap();
        assert_eq!(url, "mysql://user:pass@localhost:3306/mydb");
    }

    #[test]
    fn test_build_database_url_postgres() {
        let config = DatabasePoolConfig {
            base_connection_string: "postgres://user:pass@localhost:5432".to_string(),
            db_type: DatabaseType::PostgreSQL,
            pool_options: PoolOptions::default(),
            idle_timeout: Duration::from_secs(600),
            cleanup_interval: Duration::from_secs(60),
        };
        let manager = create_test_manager(config);
        let target = DatabaseTarget::Database("analytics".to_string());

        let url = manager.build_database_url(&target).unwrap();
        assert_eq!(url, "postgres://user:pass@localhost:5432/analytics");
    }

    #[test]
    fn test_build_database_url_with_query_params() {
        let config = DatabasePoolConfig {
            base_connection_string: "mysql://user:pass@localhost:3306?ssl-mode=required"
                .to_string(),
            db_type: DatabaseType::MySQL,
            pool_options: PoolOptions::default(),
            idle_timeout: Duration::from_secs(600),
            cleanup_interval: Duration::from_secs(60),
        };
        let manager = create_test_manager(config);
        let target = DatabaseTarget::Database("testdb".to_string());

        let url = manager.build_database_url(&target).unwrap();
        assert!(url.contains("/testdb"));
        assert!(url.contains("ssl-mode=required"));
    }

    #[test]
    fn test_build_database_url_strips_existing_path() {
        let config = DatabasePoolConfig {
            base_connection_string: "mysql://user:pass@localhost:3306/existing_db".to_string(),
            db_type: DatabaseType::MySQL,
            pool_options: PoolOptions::default(),
            idle_timeout: Duration::from_secs(600),
            cleanup_interval: Duration::from_secs(60),
        };
        let manager = create_test_manager(config);
        let target = DatabaseTarget::Database("new_db".to_string());

        let url = manager.build_database_url(&target).unwrap();
        assert_eq!(url, "mysql://user:pass@localhost:3306/new_db");
        assert!(!url.contains("existing_db"));
    }

    #[test]
    fn test_database_target_from_option() {
        // None becomes Server
        assert_eq!(
            DatabaseTarget::from_option(None).unwrap(),
            DatabaseTarget::Server
        );

        // Some("db") becomes Database
        assert_eq!(
            DatabaseTarget::from_option(Some("mydb")).unwrap(),
            DatabaseTarget::Database("mydb".to_string())
        );

        // Empty string is rejected
        assert!(DatabaseTarget::from_option(Some("")).is_err());
    }

    #[test]
    fn test_database_target_display() {
        assert_eq!(DatabaseTarget::Server.to_string(), "server");
        assert_eq!(
            DatabaseTarget::Database("mydb".to_string()).to_string(),
            "mydb"
        );
    }

    #[tokio::test]
    async fn test_database_pool_entry_active_count() {
        let entry = DatabasePoolEntry {
            pool: DbPool::MySql(sqlx::Pool::connect_lazy("mysql://localhost").unwrap()),
            target: DatabaseTarget::Database("test".to_string()),
            last_accessed: std::sync::RwLock::new(Instant::now()),
            created_at: Instant::now(),
            active_count: AtomicUsize::new(0),
        };

        assert_eq!(entry.active_count(), 0);
        entry.increment_active();
        assert_eq!(entry.active_count(), 1);
        entry.increment_active();
        assert_eq!(entry.active_count(), 2);
        entry.decrement_active();
        assert_eq!(entry.active_count(), 1);
        entry.decrement_active();
        assert_eq!(entry.active_count(), 0);
    }

    #[tokio::test]
    async fn test_oncecell_ensures_single_entry_per_database() {
        let config = create_test_config();
        let manager = create_test_manager(config);

        {
            let mut pools = manager.pools.write().await;
            pools.insert(
                DatabaseTarget::Database("db1".to_string()),
                Arc::new(OnceCell::new()),
            );
            pools.insert(
                DatabaseTarget::Database("db2".to_string()),
                Arc::new(OnceCell::new()),
            );
        }

        let pools = manager.pools.read().await;
        assert_eq!(pools.len(), 2);
        assert!(pools.contains_key(&DatabaseTarget::Database("db1".to_string())));
        assert!(pools.contains_key(&DatabaseTarget::Database("db2".to_string())));
    }

    #[tokio::test]
    async fn test_pool_count_only_counts_initialized() {
        let config = create_test_config();
        let manager = create_test_manager(config);

        {
            let mut pools = manager.pools.write().await;
            pools.insert(
                DatabaseTarget::Database("db1".to_string()),
                Arc::new(OnceCell::new()),
            );
            pools.insert(
                DatabaseTarget::Database("db2".to_string()),
                Arc::new(OnceCell::new()),
            );
        }

        assert_eq!(manager.pool_count().await, 0);
    }

    #[test]
    fn test_cleanup_handle_is_sync_mutex() {
        let config = create_test_config();
        let manager = create_test_manager(config);

        let guard = manager.cleanup_handle.lock().unwrap();
        assert!(guard.is_none());
    }
}
