//! Transaction registry for managing database transactions across MCP tool calls.
//!
//! This module provides stateful transaction management, enabling transactions
//! to persist across multiple tool invocations. Each transaction holds a dedicated
//! database connection until committed or rolled back.

use crate::db::params::{bind_mysql_param, bind_postgres_param, bind_sqlite_param};
use crate::error::{DbError, DbResult};
use crate::models::{DatabaseType, QueryParam, QueryResult};
use chrono::{DateTime, Utc};
use sqlx::{MySql, MySqlPool, PgPool, Postgres, Sqlite, SqlitePool, Transaction};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, info, warn};

/// Default transaction timeout in seconds.
pub const DEFAULT_TRANSACTION_TIMEOUT_SECS: u32 = 60;

/// Maximum transaction timeout in seconds.
pub const MAX_TRANSACTION_TIMEOUT_SECS: u32 = 300;

/// Cleanup interval for expired transactions.
const CLEANUP_INTERVAL_SECS: u64 = 5;

/// Database-specific transaction wrapper.
pub enum DbTransaction {
    MySql(Transaction<'static, MySql>),
    Postgres(Transaction<'static, Postgres>),
    SQLite(Transaction<'static, Sqlite>),
}

impl DbTransaction {
    /// Get the database type for this transaction.
    pub fn db_type(&self) -> DatabaseType {
        match self {
            DbTransaction::MySql(_) => DatabaseType::MySQL,
            DbTransaction::Postgres(_) => DatabaseType::PostgreSQL,
            DbTransaction::SQLite(_) => DatabaseType::SQLite,
        }
    }

    /// Commit the transaction.
    pub async fn commit(self) -> DbResult<()> {
        match self {
            DbTransaction::MySql(tx) => tx.commit().await.map_err(DbError::from),
            DbTransaction::Postgres(tx) => tx.commit().await.map_err(DbError::from),
            DbTransaction::SQLite(tx) => tx.commit().await.map_err(DbError::from),
        }
    }

    /// Rollback the transaction.
    pub async fn rollback(self) -> DbResult<()> {
        match self {
            DbTransaction::MySql(tx) => tx.rollback().await.map_err(DbError::from),
            DbTransaction::Postgres(tx) => tx.rollback().await.map_err(DbError::from),
            DbTransaction::SQLite(tx) => tx.rollback().await.map_err(DbError::from),
        }
    }
}

/// Per-transaction entry holding the actual transaction and metadata.
struct TxEntry {
    /// The actual database transaction (None after commit/rollback initiated)
    transaction: Option<DbTransaction>,
    /// Connection this transaction belongs to
    connection_id: String,
    /// When the transaction was created
    created_at: Instant,
    /// Configured timeout for this transaction
    timeout_secs: u32,
}

impl TxEntry {
    /// Check if the transaction has expired.
    fn is_expired(&self) -> bool {
        self.created_at.elapsed().as_secs() > self.timeout_secs as u64
    }
}

/// Metadata about an active transaction (for listing without consuming).
#[derive(Debug, Clone)]
pub struct TransactionMetadata {
    pub transaction_id: String,
    pub connection_id: String,
    pub started_at: DateTime<Utc>,
    pub duration_secs: u64,
    pub timeout_secs: u32,
}

#[derive(Clone)]
pub struct TransactionRegistry {
    /// Map of transaction IDs to per-transaction mutex entries.
    /// Uses Arc<Mutex<TxEntry>> to enable per-transaction locking instead of global locking.
    transactions: Arc<RwLock<HashMap<String, Arc<Mutex<TxEntry>>>>>,
    /// Default timeout for new transactions (from config)
    default_timeout_secs: u32,
    // Used to convert Instant to DateTime<Utc>
    system_start_instant: Instant,
    system_start_datetime: DateTime<Utc>,
}

impl TransactionRegistry {
    /// Create a new transaction registry with default timeout.
    pub fn new() -> Self {
        Self {
            transactions: Arc::new(RwLock::new(HashMap::new())),
            default_timeout_secs: DEFAULT_TRANSACTION_TIMEOUT_SECS,
            system_start_instant: Instant::now(),
            system_start_datetime: Utc::now(),
        }
    }

    /// Create a new transaction registry with custom default timeout.
    pub fn with_defaults(default_timeout_secs: u32) -> Self {
        Self {
            transactions: Arc::new(RwLock::new(HashMap::new())),
            default_timeout_secs: default_timeout_secs.min(MAX_TRANSACTION_TIMEOUT_SECS),
            system_start_instant: Instant::now(),
            system_start_datetime: Utc::now(),
        }
    }

    /// List all active transactions with their metadata.
    pub async fn list_all(&self) -> Vec<TransactionMetadata> {
        let txs = self.transactions.read().await;
        let mut result = Vec::with_capacity(txs.len());

        for (id, entry_arc) in txs.iter() {
            let entry = entry_arc.lock().await;
            let duration = entry.created_at.elapsed();
            let duration_secs = duration.as_secs();
            let offset_from_start = entry.created_at.duration_since(self.system_start_instant);
            let started_at = self.system_start_datetime + offset_from_start;

            result.push(TransactionMetadata {
                transaction_id: id.clone(),
                connection_id: entry.connection_id.clone(),
                started_at,
                duration_secs,
                timeout_secs: entry.timeout_secs,
            });
        }

        result
    }

    /// Start a background task to clean up expired transactions.
    pub fn start_cleanup_task(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(CLEANUP_INTERVAL_SECS));
            loop {
                interval.tick().await;
                self.cleanup_expired().await;
            }
        });
    }

    /// Begin a new transaction on a MySQL pool.
    pub async fn begin_mysql(
        &self,
        pool: &MySqlPool,
        connection_id: String,
        timeout_secs: Option<u32>,
    ) -> DbResult<String> {
        let timeout_secs = timeout_secs
            .map(|t| t.min(MAX_TRANSACTION_TIMEOUT_SECS))
            .unwrap_or(self.default_timeout_secs);

        let tx = pool.begin().await.map_err(DbError::from)?;
        let transaction_id = generate_transaction_id();

        let entry = TxEntry {
            transaction: Some(DbTransaction::MySql(tx)),
            connection_id: connection_id.clone(),
            created_at: Instant::now(),
            timeout_secs,
        };

        {
            let mut txs = self.transactions.write().await;
            txs.insert(transaction_id.clone(), Arc::new(Mutex::new(entry)));
        }

        info!(
            transaction_id = %transaction_id,
            connection_id = %connection_id,
            timeout_secs = timeout_secs,
            "Transaction started (MySQL)"
        );

        Ok(transaction_id)
    }

    /// Begin a new transaction on a PostgreSQL pool.
    pub async fn begin_postgres(
        &self,
        pool: &PgPool,
        connection_id: String,
        timeout_secs: Option<u32>,
    ) -> DbResult<String> {
        let timeout_secs = timeout_secs
            .map(|t| t.min(MAX_TRANSACTION_TIMEOUT_SECS))
            .unwrap_or(self.default_timeout_secs);

        let tx = pool.begin().await.map_err(DbError::from)?;
        let transaction_id = generate_transaction_id();

        let entry = TxEntry {
            transaction: Some(DbTransaction::Postgres(tx)),
            connection_id: connection_id.clone(),
            created_at: Instant::now(),
            timeout_secs,
        };

        {
            let mut txs = self.transactions.write().await;
            txs.insert(transaction_id.clone(), Arc::new(Mutex::new(entry)));
        }

        info!(
            transaction_id = %transaction_id,
            connection_id = %connection_id,
            timeout_secs = timeout_secs,
            "Transaction started (PostgreSQL)"
        );

        Ok(transaction_id)
    }

    /// Begin a new transaction on a SQLite pool.
    pub async fn begin_sqlite(
        &self,
        pool: &SqlitePool,
        connection_id: String,
        timeout_secs: Option<u32>,
    ) -> DbResult<String> {
        let timeout_secs = timeout_secs
            .map(|t| t.min(MAX_TRANSACTION_TIMEOUT_SECS))
            .unwrap_or(self.default_timeout_secs);

        let tx = pool.begin().await.map_err(DbError::from)?;
        let transaction_id = generate_transaction_id();

        let entry = TxEntry {
            transaction: Some(DbTransaction::SQLite(tx)),
            connection_id: connection_id.clone(),
            created_at: Instant::now(),
            timeout_secs,
        };

        {
            let mut txs = self.transactions.write().await;
            txs.insert(transaction_id.clone(), Arc::new(Mutex::new(entry)));
        }

        info!(
            transaction_id = %transaction_id,
            connection_id = %connection_id,
            timeout_secs = timeout_secs,
            "Transaction started (SQLite)"
        );

        Ok(transaction_id)
    }

    /// Get transaction info without taking ownership.
    /// Uses two-phase locking: short map lock to get Arc, then lock entry.
    pub async fn get_info(&self, transaction_id: &str) -> DbResult<(String, u32, bool)> {
        // Phase 1: Short-lived map lock to get Arc reference
        let entry_arc = {
            let txs = self.transactions.read().await;
            txs.get(transaction_id).cloned()
        }
        .ok_or_else(|| DbError::transaction("Transaction not found", transaction_id))?;

        // Phase 2: Lock individual transaction (map lock already released)
        let entry = entry_arc.lock().await;
        let expired = entry.is_expired();
        Ok((entry.connection_id.clone(), entry.timeout_secs, expired))
    }

    /// Check if a transaction exists and is valid (not expired).
    /// Uses two-phase locking: short map lock to get Arc, then lock entry.
    pub async fn is_valid(&self, transaction_id: &str, connection_id: &str) -> DbResult<()> {
        // Phase 1: Short-lived map lock to get Arc reference
        let entry_arc = {
            let txs = self.transactions.read().await;
            txs.get(transaction_id).cloned()
        }
        .ok_or_else(|| DbError::transaction("Transaction not found", transaction_id))?;

        // Phase 2: Lock individual transaction (map lock already released)
        let entry = entry_arc.lock().await;
        Self::validate_entry(&entry, connection_id, transaction_id)
    }

    fn validate_entry(entry: &TxEntry, connection_id: &str, transaction_id: &str) -> DbResult<()> {
        if entry.connection_id != connection_id {
            return Err(DbError::transaction(
                "Transaction belongs to a different connection",
                transaction_id,
            ));
        }
        if entry.is_expired() {
            return Err(DbError::transaction(
                "Transaction has expired",
                transaction_id,
            ));
        }
        if entry.transaction.is_none() {
            return Err(DbError::transaction(
                "Transaction is no longer active",
                transaction_id,
            ));
        }
        Ok(())
    }

    /// Execute a write operation within a transaction.
    /// Uses two-phase locking: short map lock to get Arc, then lock entry.
    pub async fn execute_in_transaction(
        &self,
        transaction_id: &str,
        connection_id: &str,
        sql: &str,
        params: &[QueryParam],
    ) -> DbResult<u64> {
        // Phase 1: Short-lived map lock to get Arc reference
        let entry_arc = {
            let txs = self.transactions.read().await;
            txs.get(transaction_id).cloned()
        }
        .ok_or_else(|| DbError::transaction("Transaction not found", transaction_id))?;

        // Phase 2: Lock individual transaction (map lock already released)
        let mut entry = entry_arc.lock().await;
        Self::validate_entry(&entry, connection_id, transaction_id)?;

        let tx = entry.transaction.as_mut().ok_or_else(|| {
            DbError::transaction("Transaction is no longer active", transaction_id)
        })?;

        let rows_affected = match tx {
            DbTransaction::MySql(tx) => {
                let mut query = sqlx::query(sql);
                for param in params {
                    query = bind_mysql_param(query, param);
                }
                query
                    .execute(&mut **tx)
                    .await
                    .map_err(DbError::from)?
                    .rows_affected()
            }
            DbTransaction::Postgres(tx) => {
                let mut query = sqlx::query(sql);
                for param in params {
                    query = bind_postgres_param(query, param);
                }
                query
                    .execute(&mut **tx)
                    .await
                    .map_err(DbError::from)?
                    .rows_affected()
            }
            DbTransaction::SQLite(tx) => {
                let mut query = sqlx::query(sql);
                for param in params {
                    query = bind_sqlite_param(query, param);
                }
                query
                    .execute(&mut **tx)
                    .await
                    .map_err(DbError::from)?
                    .rows_affected()
            }
        };

        debug!(
            transaction_id = %transaction_id,
            sql = %sql,
            rows_affected = rows_affected,
            "Executed in transaction"
        );

        Ok(rows_affected)
    }

    /// Execute a query within a transaction.
    /// Uses two-phase locking: short map lock to get Arc, then lock entry.
    /// Uses streaming with take(limit+1) for memory efficiency.
    pub async fn query_in_transaction(
        &self,
        transaction_id: &str,
        connection_id: &str,
        sql: &str,
        params: &[QueryParam],
        limit: u32,
        decode_binary: bool,
    ) -> DbResult<QueryResult> {
        use crate::db::types::RowToJson;
        use futures_util::StreamExt;
        use std::time::Instant;

        let start = Instant::now();

        // Phase 1: Short-lived map lock to get Arc reference
        let entry_arc = {
            let txs = self.transactions.read().await;
            txs.get(transaction_id).cloned()
        }
        .ok_or_else(|| DbError::transaction("Transaction not found", transaction_id))?;

        // Phase 2: Lock individual transaction (map lock already released)
        let mut entry = entry_arc.lock().await;
        Self::validate_entry(&entry, connection_id, transaction_id)?;

        let tx = entry.transaction.as_mut().ok_or_else(|| {
            DbError::transaction("Transaction is no longer active", transaction_id)
        })?;

        let fetch_limit = limit as usize + 1;

        let (columns, rows) = match tx {
            DbTransaction::MySql(tx) => {
                let mut query = sqlx::query(sql);
                for param in params {
                    query = bind_mysql_param(query, param);
                }
                let stream = query.fetch(&mut **tx);
                let results: Vec<Result<sqlx::mysql::MySqlRow, sqlx::Error>> =
                    stream.take(fetch_limit).collect().await;

                let mut rows = Vec::with_capacity(results.len());
                for result in results {
                    rows.push(result.map_err(DbError::from)?);
                }

                let columns = if rows.is_empty() {
                    Vec::new()
                } else {
                    rows[0].get_column_names()
                };
                let json_rows: Vec<_> = rows
                    .iter()
                    .map(|r| r.to_json_map_with_options(decode_binary))
                    .collect();
                (columns, json_rows)
            }
            DbTransaction::Postgres(tx) => {
                let mut query = sqlx::query(sql);
                for param in params {
                    query = bind_postgres_param(query, param);
                }
                let stream = query.fetch(&mut **tx);
                let results: Vec<Result<sqlx::postgres::PgRow, sqlx::Error>> =
                    stream.take(fetch_limit).collect().await;

                let mut rows = Vec::with_capacity(results.len());
                for result in results {
                    rows.push(result.map_err(DbError::from)?);
                }

                let columns = if rows.is_empty() {
                    Vec::new()
                } else {
                    rows[0].get_column_names()
                };
                let json_rows: Vec<_> = rows
                    .iter()
                    .map(|r| r.to_json_map_with_options(decode_binary))
                    .collect();
                (columns, json_rows)
            }
            DbTransaction::SQLite(tx) => {
                let mut query = sqlx::query(sql);
                for param in params {
                    query = bind_sqlite_param(query, param);
                }
                let stream = query.fetch(&mut **tx);
                let results: Vec<Result<sqlx::sqlite::SqliteRow, sqlx::Error>> =
                    stream.take(fetch_limit).collect().await;

                let mut rows = Vec::with_capacity(results.len());
                for result in results {
                    rows.push(result.map_err(DbError::from)?);
                }

                let columns = if rows.is_empty() {
                    Vec::new()
                } else {
                    rows[0].get_column_names()
                };
                let json_rows: Vec<_> = rows
                    .iter()
                    .map(|r| r.to_json_map_with_options(decode_binary))
                    .collect();
                (columns, json_rows)
            }
        };

        let execution_time_ms = start.elapsed().as_millis() as u64;
        let has_more = rows.len() > limit as usize;
        let rows_to_return: Vec<_> = rows.into_iter().take(limit as usize).collect();

        debug!(
            transaction_id = %transaction_id,
            sql = %sql,
            row_count = rows_to_return.len(),
            has_more = has_more,
            "Queried in transaction"
        );

        Ok(QueryResult {
            columns,
            rows: rows_to_return,
            rows_affected: None,
            execution_time_ms,
            truncated: Some(has_more),
            has_more: Some(has_more),
        })
    }

    /// Commit a transaction.
    /// Uses two-phase locking: lock entry to validate and take transaction,
    /// then remove from map and commit outside all locks.
    pub async fn commit(&self, transaction_id: &str, connection_id: &str) -> DbResult<()> {
        // Phase 1: Get Arc reference with short map lock
        let entry_arc = {
            let txs = self.transactions.read().await;
            txs.get(transaction_id).cloned()
        }
        .ok_or_else(|| DbError::transaction("Transaction not found", transaction_id))?;

        // Phase 2: Lock entry, validate, and take the transaction
        let tx = {
            let mut entry = entry_arc.lock().await;

            if entry.connection_id != connection_id {
                return Err(DbError::transaction(
                    "Transaction belongs to a different connection",
                    transaction_id,
                ));
            }

            entry.transaction.take().ok_or_else(|| {
                DbError::transaction("Transaction is no longer active", transaction_id)
            })?
        };
        // Entry lock released here

        // Phase 3: Remove from map (entry is now empty)
        {
            let mut txs = self.transactions.write().await;
            txs.remove(transaction_id);
        }

        // Phase 4: Commit outside all locks
        tx.commit().await?;

        info!(
            transaction_id = %transaction_id,
            connection_id = %connection_id,
            "Transaction committed"
        );

        Ok(())
    }

    /// Rollback a transaction.
    /// Uses two-phase locking: lock entry to validate and take transaction,
    /// then remove from map and rollback outside all locks.
    pub async fn rollback(&self, transaction_id: &str, connection_id: &str) -> DbResult<()> {
        // Phase 1: Get Arc reference with short map lock
        let entry_arc = {
            let txs = self.transactions.read().await;
            txs.get(transaction_id).cloned()
        }
        .ok_or_else(|| DbError::transaction("Transaction not found", transaction_id))?;

        // Phase 2: Lock entry, validate, and take the transaction
        let tx = {
            let mut entry = entry_arc.lock().await;

            if entry.connection_id != connection_id {
                return Err(DbError::transaction(
                    "Transaction belongs to a different connection",
                    transaction_id,
                ));
            }

            entry.transaction.take().ok_or_else(|| {
                DbError::transaction("Transaction is no longer active", transaction_id)
            })?
        };
        // Entry lock released here

        // Phase 3: Remove from map (entry is now empty)
        {
            let mut txs = self.transactions.write().await;
            txs.remove(transaction_id);
        }

        // Phase 4: Rollback outside all locks
        tx.rollback().await?;

        info!(
            transaction_id = %transaction_id,
            connection_id = %connection_id,
            "Transaction rolled back"
        );

        Ok(())
    }

    /// Clean up expired transactions.
    /// Collects expired IDs, removes from map, then rolls back outside locks.
    async fn cleanup_expired(&self) {
        // Phase 1: Collect expired transaction info with read lock
        let expired_entries: Vec<(String, String, Arc<Mutex<TxEntry>>)> = {
            let txs = self.transactions.read().await;
            let mut expired = Vec::new();

            for (id, entry_arc) in txs.iter() {
                let entry = entry_arc.lock().await;
                if entry.is_expired() && entry.transaction.is_some() {
                    expired.push((id.clone(), entry.connection_id.clone(), entry_arc.clone()));
                }
            }

            expired
        };

        if expired_entries.is_empty() {
            return;
        }

        // Phase 2: For each expired entry, take the transaction and remove from map
        let mut transactions_to_rollback = Vec::new();

        for (id, connection_id, entry_arc) in expired_entries {
            // Lock entry and take transaction
            let tx = {
                let mut entry = entry_arc.lock().await;
                entry.transaction.take()
            };

            if let Some(tx) = tx {
                // Remove from map
                {
                    let mut txs = self.transactions.write().await;
                    txs.remove(&id);
                }
                transactions_to_rollback.push((id, connection_id, tx));
            }
        }

        // Phase 3: Rollback all expired transactions outside all locks
        for (id, connection_id, tx) in transactions_to_rollback {
            warn!(
                transaction_id = %id,
                connection_id = %connection_id,
                "Rolling back expired transaction"
            );
            let _ = tx.rollback().await;
        }
    }

    /// Get the number of active transactions.
    pub async fn count(&self) -> usize {
        let txs = self.transactions.read().await;
        txs.len()
    }
}

impl Default for TransactionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Generate a unique transaction ID.
fn generate_transaction_id() -> String {
    format!("tx_{}", uuid::Uuid::new_v4().simple())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_id_format() {
        let id = generate_transaction_id();
        assert!(id.starts_with("tx_"));
        assert_eq!(id.len(), 3 + 32); // "tx_" + 32 hex chars
    }

    #[tokio::test]
    async fn test_registry_creation() {
        let registry = TransactionRegistry::new();
        assert_eq!(registry.count().await, 0);
    }

    #[tokio::test]
    async fn test_transaction_not_found() {
        let registry = TransactionRegistry::new();
        let result = registry.is_valid("tx_nonexistent", "conn1").await;
        assert!(result.is_err());
    }

    #[test]
    fn test_timeout_constants() {
        assert!(DEFAULT_TRANSACTION_TIMEOUT_SECS <= MAX_TRANSACTION_TIMEOUT_SECS);
        assert!(MAX_TRANSACTION_TIMEOUT_SECS <= 300);
    }

    #[tokio::test]
    async fn test_list_all_empty_returns_empty_vec() {
        let registry = TransactionRegistry::new();
        let result = registry.list_all().await;
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_list_all_returns_transaction_metadata() {
        // We can't easily test with real transactions without a database,
        // but we can verify the registry starts empty
        let registry = TransactionRegistry::new();
        assert_eq!(registry.count().await, 0);
        let list = registry.list_all().await;
        assert!(list.is_empty());
    }

    #[test]
    fn test_transaction_metadata_fields() {
        // Test that TransactionMetadata has expected fields
        let metadata = TransactionMetadata {
            transaction_id: "tx_abc123".to_string(),
            connection_id: "conn1".to_string(),
            started_at: Utc::now(),
            duration_secs: 45,
            timeout_secs: 60,
        };
        assert_eq!(metadata.transaction_id, "tx_abc123");
        assert_eq!(metadata.connection_id, "conn1");
        assert_eq!(metadata.duration_secs, 45);
        assert_eq!(metadata.timeout_secs, 60);
    }

    #[tokio::test]
    async fn test_with_defaults_custom_timeout() {
        let registry = TransactionRegistry::with_defaults(120);
        assert_eq!(registry.default_timeout_secs, 120);
    }

    #[tokio::test]
    async fn test_with_defaults_capped_at_max() {
        let registry = TransactionRegistry::with_defaults(999);
        assert_eq!(registry.default_timeout_secs, MAX_TRANSACTION_TIMEOUT_SECS);
    }

    #[tokio::test]
    async fn test_concurrent_get_info_on_nonexistent_transactions() {
        let registry = Arc::new(TransactionRegistry::new());

        // Spawn multiple concurrent tasks trying to get info on non-existent transactions
        let mut handles = Vec::new();
        for i in 0..10 {
            let reg = registry.clone();
            let tx_id = format!("tx_nonexistent_{}", i);
            handles.push(tokio::spawn(async move { reg.get_info(&tx_id).await }));
        }

        // All should fail with "not found" error
        for handle in handles {
            let result = handle.await.expect("Task should not panic");
            assert!(result.is_err());
        }
    }

    #[tokio::test]
    async fn test_concurrent_is_valid_on_nonexistent_transactions() {
        let registry = Arc::new(TransactionRegistry::new());

        let mut handles = Vec::new();
        for i in 0..10 {
            let reg = registry.clone();
            let tx_id = format!("tx_nonexistent_{}", i);
            handles.push(tokio::spawn(
                async move { reg.is_valid(&tx_id, "conn1").await },
            ));
        }

        for handle in handles {
            let result = handle.await.expect("Task should not panic");
            assert!(result.is_err());
        }
    }

    #[tokio::test]
    async fn test_concurrent_list_all_is_safe() {
        let registry = Arc::new(TransactionRegistry::new());

        let mut handles = Vec::new();
        for _ in 0..10 {
            let reg = registry.clone();
            handles.push(tokio::spawn(async move { reg.list_all().await }));
        }

        for handle in handles {
            let result = handle.await.expect("Task should not panic");
            assert!(result.is_empty());
        }
    }

    #[tokio::test]
    async fn test_concurrent_count_is_safe() {
        let registry = Arc::new(TransactionRegistry::new());

        let mut handles = Vec::new();
        for _ in 0..10 {
            let reg = registry.clone();
            handles.push(tokio::spawn(async move { reg.count().await }));
        }

        for handle in handles {
            let count = handle.await.expect("Task should not panic");
            assert_eq!(count, 0);
        }
    }
}
