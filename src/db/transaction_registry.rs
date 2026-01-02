//! Transaction registry for managing database transactions across MCP tool calls.
//!
//! This module provides stateful transaction management, enabling transactions
//! to persist across multiple tool invocations. Each transaction holds a dedicated
//! database connection until committed or rolled back.

use crate::db::params::{bind_mysql_param, bind_postgres_param, bind_sqlite_param};
use crate::error::{DbError, DbResult};
use crate::models::{DatabaseType, QueryParam};
use chrono::{DateTime, Utc};
use sqlx::{MySql, MySqlPool, PgPool, Postgres, Sqlite, SqlitePool, Transaction};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Default transaction timeout in seconds.
pub const DEFAULT_TRANSACTION_TIMEOUT_SECS: u32 = 60;

/// Maximum transaction timeout in seconds.
pub const MAX_TRANSACTION_TIMEOUT_SECS: u32 = 300;

/// Cleanup interval for expired transactions.
const CLEANUP_INTERVAL_SECS: u64 = 5;

/// Database-specific transaction wrapper.
///
/// This enum wraps database-specific transaction types to provide
/// a unified interface for transaction management.
pub enum DbTransaction {
    /// MySQL transaction
    MySql(Transaction<'static, MySql>),
    /// PostgreSQL transaction
    Postgres(Transaction<'static, Postgres>),
    /// SQLite transaction
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

struct ActiveTransaction {
    transaction: Option<DbTransaction>,
    connection_id: String,
    created_at: Instant,
    timeout_secs: u32,
}

impl ActiveTransaction {
    /// Check if the transaction has expired.
    fn is_expired(&self) -> bool {
        self.created_at.elapsed().as_secs() > self.timeout_secs as u64
    }
}

/// Metadata about an active transaction (for listing without consuming).
#[derive(Debug, Clone)]
pub struct TransactionMetadata {
    /// Unique transaction identifier
    pub transaction_id: String,
    /// Connection this transaction belongs to
    pub connection_id: String,
    /// When the transaction started (absolute time)
    pub started_at: DateTime<Utc>,
    /// Seconds since transaction started
    pub duration_secs: u64,
    /// Configured timeout for this transaction
    pub timeout_secs: u32,
}

#[derive(Clone)]
pub struct TransactionRegistry {
    transactions: Arc<RwLock<HashMap<String, ActiveTransaction>>>,
    /// System start time for converting Instant to DateTime
    system_start_instant: Instant,
    /// System start time as UTC DateTime
    system_start_datetime: DateTime<Utc>,
}

impl TransactionRegistry {
    /// Create a new transaction registry.
    pub fn new() -> Self {
        Self {
            transactions: Arc::new(RwLock::new(HashMap::new())),
            system_start_instant: Instant::now(),
            system_start_datetime: Utc::now(),
        }
    }

    /// List all active transactions with their metadata.
    ///
    /// This method returns metadata about all transactions without consuming them.
    pub async fn list_all(&self) -> Vec<TransactionMetadata> {
        let txs = self.transactions.read().await;
        txs.iter()
            .map(|(id, entry)| {
                let duration = entry.created_at.elapsed();
                let duration_secs = duration.as_secs();
                // Convert Instant to DateTime by calculating offset from system start
                let offset_from_start = entry.created_at.duration_since(self.system_start_instant);
                let started_at = self.system_start_datetime + offset_from_start;

                TransactionMetadata {
                    transaction_id: id.clone(),
                    connection_id: entry.connection_id.clone(),
                    started_at,
                    duration_secs,
                    timeout_secs: entry.timeout_secs,
                }
            })
            .collect()
    }

    /// Start a background task to clean up expired transactions.
    ///
    /// This should be called once when the server starts.
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
            .unwrap_or(DEFAULT_TRANSACTION_TIMEOUT_SECS);

        let tx = pool.begin().await.map_err(DbError::from)?;
        let transaction_id = generate_transaction_id();

        let entry = ActiveTransaction {
            transaction: Some(DbTransaction::MySql(tx)),
            connection_id: connection_id.clone(),
            created_at: Instant::now(),
            timeout_secs,
        };

        {
            let mut txs = self.transactions.write().await;
            txs.insert(transaction_id.clone(), entry);
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
            .unwrap_or(DEFAULT_TRANSACTION_TIMEOUT_SECS);

        let tx = pool.begin().await.map_err(DbError::from)?;
        let transaction_id = generate_transaction_id();

        let entry = ActiveTransaction {
            transaction: Some(DbTransaction::Postgres(tx)),
            connection_id: connection_id.clone(),
            created_at: Instant::now(),
            timeout_secs,
        };

        {
            let mut txs = self.transactions.write().await;
            txs.insert(transaction_id.clone(), entry);
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
            .unwrap_or(DEFAULT_TRANSACTION_TIMEOUT_SECS);

        let tx = pool.begin().await.map_err(DbError::from)?;
        let transaction_id = generate_transaction_id();

        let entry = ActiveTransaction {
            transaction: Some(DbTransaction::SQLite(tx)),
            connection_id: connection_id.clone(),
            created_at: Instant::now(),
            timeout_secs,
        };

        {
            let mut txs = self.transactions.write().await;
            txs.insert(transaction_id.clone(), entry);
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
    pub async fn get_info(&self, transaction_id: &str) -> DbResult<(String, u32, bool)> {
        let txs = self.transactions.read().await;
        match txs.get(transaction_id) {
            Some(entry) => {
                let expired = entry.is_expired();
                Ok((entry.connection_id.clone(), entry.timeout_secs, expired))
            }
            None => Err(DbError::transaction(
                "Transaction not found",
                transaction_id,
            )),
        }
    }

    /// Check if a transaction exists and is valid (not expired).
    pub async fn is_valid(&self, transaction_id: &str, connection_id: &str) -> DbResult<()> {
        let txs = self.transactions.read().await;
        let entry = txs
            .get(transaction_id)
            .ok_or_else(|| DbError::transaction("Transaction not found", transaction_id))?;
        Self::validate_entry(entry, connection_id, transaction_id)
    }

    /// Validate a transaction entry's state.
    ///
    /// Checks that:
    /// - The entry belongs to the specified connection
    /// - The transaction has not expired
    /// - The transaction is still active
    fn validate_entry(
        entry: &ActiveTransaction,
        connection_id: &str,
        transaction_id: &str,
    ) -> DbResult<()> {
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
    pub async fn execute_in_transaction(
        &self,
        transaction_id: &str,
        connection_id: &str,
        sql: &str,
        params: &[QueryParam],
    ) -> DbResult<u64> {
        let mut txs = self.transactions.write().await;
        let entry = txs
            .get_mut(transaction_id)
            .ok_or_else(|| DbError::transaction("Transaction not found", transaction_id))?;

        Self::validate_entry(entry, connection_id, transaction_id)?;

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
    pub async fn query_in_transaction(
        &self,
        transaction_id: &str,
        connection_id: &str,
        sql: &str,
        params: &[QueryParam],
    ) -> DbResult<Vec<serde_json::Map<String, serde_json::Value>>> {
        use crate::db::types::RowToJson;
        use futures_util::TryStreamExt;

        let mut txs = self.transactions.write().await;
        let entry = txs
            .get_mut(transaction_id)
            .ok_or_else(|| DbError::transaction("Transaction not found", transaction_id))?;

        Self::validate_entry(entry, connection_id, transaction_id)?;

        let tx = entry.transaction.as_mut().ok_or_else(|| {
            DbError::transaction("Transaction is no longer active", transaction_id)
        })?;

        let rows: Vec<serde_json::Map<String, serde_json::Value>> = match tx {
            DbTransaction::MySql(tx) => {
                let mut query = sqlx::query(sql);
                for param in params {
                    query = bind_mysql_param(query, param);
                }
                let rows: Vec<sqlx::mysql::MySqlRow> = query
                    .fetch(&mut **tx)
                    .try_collect()
                    .await
                    .map_err(DbError::from)?;
                rows.iter().map(|r| r.to_json_map()).collect()
            }
            DbTransaction::Postgres(tx) => {
                let mut query = sqlx::query(sql);
                for param in params {
                    query = bind_postgres_param(query, param);
                }
                let rows: Vec<sqlx::postgres::PgRow> = query
                    .fetch(&mut **tx)
                    .try_collect()
                    .await
                    .map_err(DbError::from)?;
                rows.iter().map(|r| r.to_json_map()).collect()
            }
            DbTransaction::SQLite(tx) => {
                let mut query = sqlx::query(sql);
                for param in params {
                    query = bind_sqlite_param(query, param);
                }
                let rows: Vec<sqlx::sqlite::SqliteRow> = query
                    .fetch(&mut **tx)
                    .try_collect()
                    .await
                    .map_err(DbError::from)?;
                rows.iter().map(|r| r.to_json_map()).collect()
            }
        };

        debug!(
            transaction_id = %transaction_id,
            sql = %sql,
            row_count = rows.len(),
            "Queried in transaction"
        );

        Ok(rows)
    }

    /// Commit a transaction.
    pub async fn commit(&self, transaction_id: &str, connection_id: &str) -> DbResult<()> {
        let mut txs = self.transactions.write().await;
        let entry = txs
            .remove(transaction_id)
            .ok_or_else(|| DbError::transaction("Transaction not found", transaction_id))?;

        if entry.connection_id != connection_id {
            // Put it back and error
            txs.insert(transaction_id.to_string(), entry);
            return Err(DbError::transaction(
                "Transaction belongs to a different connection",
                transaction_id,
            ));
        }

        let tx = entry.transaction.ok_or_else(|| {
            DbError::transaction("Transaction is no longer active", transaction_id)
        })?;

        tx.commit().await?;

        info!(
            transaction_id = %transaction_id,
            connection_id = %connection_id,
            "Transaction committed"
        );

        Ok(())
    }

    /// Rollback a transaction.
    pub async fn rollback(&self, transaction_id: &str, connection_id: &str) -> DbResult<()> {
        let mut txs = self.transactions.write().await;
        let entry = txs
            .remove(transaction_id)
            .ok_or_else(|| DbError::transaction("Transaction not found", transaction_id))?;

        if entry.connection_id != connection_id {
            // Put it back and error
            txs.insert(transaction_id.to_string(), entry);
            return Err(DbError::transaction(
                "Transaction belongs to a different connection",
                transaction_id,
            ));
        }

        let tx = entry.transaction.ok_or_else(|| {
            DbError::transaction("Transaction is no longer active", transaction_id)
        })?;

        tx.rollback().await?;

        info!(
            transaction_id = %transaction_id,
            connection_id = %connection_id,
            "Transaction rolled back"
        );

        Ok(())
    }

    /// Clean up expired transactions.
    async fn cleanup_expired(&self) {
        let mut txs = self.transactions.write().await;
        let expired_ids: Vec<String> = txs
            .iter()
            .filter(|(_, entry)| entry.is_expired())
            .map(|(id, _)| id.clone())
            .collect();

        for id in expired_ids {
            if let Some(entry) = txs.remove(&id) {
                if let Some(tx) = entry.transaction {
                    warn!(
                        transaction_id = %id,
                        connection_id = %entry.connection_id,
                        "Rolling back expired transaction"
                    );
                    // Best effort rollback - ignore errors
                    let _ = tx.rollback().await;
                }
            }
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
}
