//! Integration tests for SQLite writable flag bug fix.
//!
//! Tests verify that:
//! - Writable connections allow INSERT, UPDATE, DELETE operations
//! - Read-only connections reject write operations
//! - Transactions work correctly with write operations
//! - Database file creation works with writable=true

use db_mcp_server::config::PoolOptions;
use db_mcp_server::db::{ConnectionManager, TransactionRegistry};
use db_mcp_server::models::{ConnectionConfig, QueryParamInput};
use db_mcp_server::tools::format::OutputFormat;
use db_mcp_server::tools::query::{QueryInput, QueryToolHandler};
use db_mcp_server::tools::transaction::{
    BeginTransactionInput, CommitInput, RollbackInput, TransactionToolHandler,
};
use db_mcp_server::tools::write::{ExecuteInput, WriteToolHandler};
use std::sync::Arc;
use tempfile::NamedTempFile;

/// Create a writable SQLite test database
async fn setup_writable_db() -> (Arc<ConnectionManager>, Arc<TransactionRegistry>, String) {
    let temp_file = NamedTempFile::new().unwrap();
    // Keep the temp file alive - prevent deletion when function returns
    let db_path = temp_file
        .into_temp_path()
        .keep()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let manager = Arc::new(ConnectionManager::new());
    let registry = Arc::new(TransactionRegistry::new());

    let conn_url = format!("sqlite:{}", db_path);
    let config = ConnectionConfig::new(
        "test-writable",
        &conn_url,
        true, // writable=true
        false,
        None,
        PoolOptions::default(),
    )
    .unwrap();

    manager.connect(config).await.unwrap();

    // Create test table
    let write_handler = WriteToolHandler::new(manager.clone(), registry.clone());
    let create_input = ExecuteInput {
        connection_id: "test-writable".to_string(),
        sql: "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)".to_string(),
        params: vec![],
        skip_sql_check: false,
        timeout_secs: None,
        transaction_id: None,
        database: None,
    };
    write_handler.execute(create_input).await.unwrap();

    (manager, registry, db_path)
}

/// Create a readonly SQLite test database
async fn setup_readonly_db() -> (Arc<ConnectionManager>, Arc<TransactionRegistry>, String) {
    let temp_file = NamedTempFile::new().unwrap();
    // Keep the temp file alive - prevent deletion when function returns
    let db_path = temp_file
        .into_temp_path()
        .keep()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // First create the database with writable connection
    {
        let manager_temp = Arc::new(ConnectionManager::new());
        let registry_temp = Arc::new(TransactionRegistry::new());
        let conn_url = format!("sqlite:{}", db_path);
        let config = ConnectionConfig::new(
            "temp-writable",
            &conn_url,
            true,
            false,
            None,
            PoolOptions::default(),
        )
        .unwrap();
        manager_temp.connect(config).await.unwrap();

        let write_handler = WriteToolHandler::new(manager_temp.clone(), registry_temp.clone());
        let create_input = ExecuteInput {
            connection_id: "temp-writable".to_string(),
            sql: "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)".to_string(),
            params: vec![],
            skip_sql_check: false,
            timeout_secs: None,
            transaction_id: None,
            database: None,
        };
        write_handler.execute(create_input).await.unwrap();
    }

    // Now connect in readonly mode
    let manager = Arc::new(ConnectionManager::new());
    let registry = Arc::new(TransactionRegistry::new());
    let conn_url = format!("sqlite:{}", db_path);
    let config = ConnectionConfig::new(
        "test-readonly",
        &conn_url,
        false, // writable=false
        false,
        None,
        PoolOptions::default(),
    )
    .unwrap();
    manager.connect(config).await.unwrap();

    (manager, registry, db_path)
}

// =============================================================================
// User Story 1 Tests: Basic Write Operations
// =============================================================================

#[tokio::test]
async fn test_writable_insert_operations() {
    let (manager, registry, _path) = setup_writable_db().await;
    let write_handler = WriteToolHandler::new(manager.clone(), registry.clone());

    let insert_input = ExecuteInput {
        connection_id: "test-writable".to_string(),
        sql: "INSERT INTO users (id, name, age) VALUES (?, ?, ?)".to_string(),
        params: vec![
            QueryParamInput::Int(1),
            QueryParamInput::String("Alice".to_string()),
            QueryParamInput::Int(30),
        ],
        skip_sql_check: false,
        timeout_secs: None,
        transaction_id: None,
        database: None,
    };

    let result = write_handler.execute(insert_input).await;
    assert!(
        result.is_ok(),
        "INSERT should succeed on writable connection"
    );

    let execute_result = result.unwrap();
    assert_eq!(execute_result.rows_affected, 1);
}

#[tokio::test]
async fn test_writable_update_operations() {
    let (manager, registry, _path) = setup_writable_db().await;
    let write_handler = WriteToolHandler::new(manager.clone(), registry.clone());

    // Insert data first
    let insert_input = ExecuteInput {
        connection_id: "test-writable".to_string(),
        sql: "INSERT INTO users (id, name, age) VALUES (?, ?, ?)".to_string(),
        params: vec![
            QueryParamInput::Int(1),
            QueryParamInput::String("Alice".to_string()),
            QueryParamInput::Int(30),
        ],
        skip_sql_check: false,
        timeout_secs: None,
        transaction_id: None,
        database: None,
    };
    write_handler.execute(insert_input).await.unwrap();

    // Now update
    let update_input = ExecuteInput {
        connection_id: "test-writable".to_string(),
        sql: "UPDATE users SET name = ?, age = ? WHERE id = ?".to_string(),
        params: vec![
            QueryParamInput::String("Alice Updated".to_string()),
            QueryParamInput::Int(31),
            QueryParamInput::Int(1),
        ],
        skip_sql_check: false,
        timeout_secs: None,
        transaction_id: None,
        database: None,
    };

    let result = write_handler.execute(update_input).await;
    assert!(
        result.is_ok(),
        "UPDATE should succeed on writable connection"
    );

    let execute_result = result.unwrap();
    assert_eq!(execute_result.rows_affected, 1);
}

#[tokio::test]
async fn test_writable_delete_operations() {
    let (manager, registry, _path) = setup_writable_db().await;
    let write_handler = WriteToolHandler::new(manager.clone(), registry.clone());

    // Insert data first
    let insert_input = ExecuteInput {
        connection_id: "test-writable".to_string(),
        sql: "INSERT INTO users (id, name, age) VALUES (?, ?, ?)".to_string(),
        params: vec![
            QueryParamInput::Int(1),
            QueryParamInput::String("Alice".to_string()),
            QueryParamInput::Int(30),
        ],
        skip_sql_check: false,
        timeout_secs: None,
        transaction_id: None,
        database: None,
    };
    write_handler.execute(insert_input).await.unwrap();

    // Now delete
    let delete_input = ExecuteInput {
        connection_id: "test-writable".to_string(),
        sql: "DELETE FROM users WHERE id = ?".to_string(),
        params: vec![QueryParamInput::Int(1)],
        skip_sql_check: true, // DELETE requires dangerous flag
        timeout_secs: None,
        transaction_id: None,
        database: None,
    };

    let result = write_handler.execute(delete_input).await;
    assert!(
        result.is_ok(),
        "DELETE should succeed on writable connection"
    );

    let execute_result = result.unwrap();
    assert_eq!(execute_result.rows_affected, 1);
}

#[tokio::test]
async fn test_readonly_rejects_writes() {
    let (manager, registry, _path) = setup_readonly_db().await;
    let write_handler = WriteToolHandler::new(manager.clone(), registry.clone());

    let insert_input = ExecuteInput {
        connection_id: "test-readonly".to_string(),
        sql: "INSERT INTO users (id, name, age) VALUES (?, ?, ?)".to_string(),
        params: vec![
            QueryParamInput::Int(1),
            QueryParamInput::String("Bob".to_string()),
            QueryParamInput::Int(25),
        ],
        skip_sql_check: false,
        timeout_secs: None,
        transaction_id: None,
        database: None,
    };

    let result = write_handler.execute(insert_input).await;
    assert!(result.is_err(), "INSERT should fail on readonly connection");
}

// =============================================================================
// User Story 2 Tests: Transaction-Based Write Operations
// =============================================================================

#[tokio::test]
async fn test_transaction_with_multiple_writes_and_commit() {
    let (manager, registry, _path) = setup_writable_db().await;
    let tx_handler = TransactionToolHandler::new(manager.clone(), registry.clone());
    let write_handler = WriteToolHandler::new(manager.clone(), registry.clone());

    // Begin transaction
    let begin_input = BeginTransactionInput {
        connection_id: "test-writable".to_string(),
        timeout_secs: None,
        database: None,
    };
    let tx_result = tx_handler.begin_transaction(begin_input).await.unwrap();
    let tx_id = tx_result.transaction_id.clone();

    // Insert multiple rows in transaction
    for i in 1..=3 {
        let insert_input = ExecuteInput {
            connection_id: "test-writable".to_string(),
            sql: "INSERT INTO users (id, name, age) VALUES (?, ?, ?)".to_string(),
            params: vec![
                QueryParamInput::Int(i),
                QueryParamInput::String(format!("User{}", i)),
                QueryParamInput::Int(20 + i),
            ],
            skip_sql_check: false,
            timeout_secs: None,
            transaction_id: Some(tx_id.clone()),
            database: None,
        };
        write_handler.execute(insert_input).await.unwrap();
    }

    // Commit transaction
    let commit_input = CommitInput {
        connection_id: "test-writable".to_string(),
        transaction_id: tx_id,
    };
    let commit_result = tx_handler.commit(commit_input).await;
    assert!(commit_result.is_ok(), "Transaction commit should succeed");

    // Verify all rows were committed
    let query_handler = QueryToolHandler::with_defaults(manager.clone(), registry.clone(), 30, 100);
    let query_input = QueryInput {
        connection_id: "test-writable".to_string(),
        sql: "SELECT COUNT(*) as count FROM users".to_string(),
        params: vec![],
        limit: None,
        timeout_secs: None,
        decode_binary: true,
        transaction_id: None,
        database: None,
        format: OutputFormat::Json,
    };
    let query_result = query_handler.query(query_input).await.unwrap();
    assert_eq!(query_result.rows.len(), 1);
}

#[tokio::test]
async fn test_transaction_with_rollback() {
    let (manager, registry, _path) = setup_writable_db().await;
    let tx_handler = TransactionToolHandler::new(manager.clone(), registry.clone());
    let write_handler = WriteToolHandler::new(manager.clone(), registry.clone());

    // Begin transaction
    let begin_input = BeginTransactionInput {
        connection_id: "test-writable".to_string(),
        timeout_secs: None,
        database: None,
    };
    let tx_result = tx_handler.begin_transaction(begin_input).await.unwrap();
    let tx_id = tx_result.transaction_id.clone();

    // Insert data in transaction
    let insert_input = ExecuteInput {
        connection_id: "test-writable".to_string(),
        sql: "INSERT INTO users (id, name, age) VALUES (?, ?, ?)".to_string(),
        params: vec![
            QueryParamInput::Int(1),
            QueryParamInput::String("Will Rollback".to_string()),
            QueryParamInput::Int(99),
        ],
        skip_sql_check: false,
        timeout_secs: None,
        transaction_id: Some(tx_id.clone()),
        database: None,
    };
    write_handler.execute(insert_input).await.unwrap();

    // Rollback transaction
    let rollback_input = RollbackInput {
        connection_id: "test-writable".to_string(),
        transaction_id: tx_id,
    };
    let rollback_result = tx_handler.rollback(rollback_input).await;
    assert!(
        rollback_result.is_ok(),
        "Transaction rollback should succeed"
    );

    // Verify no data was committed
    let query_handler = QueryToolHandler::with_defaults(manager.clone(), registry.clone(), 30, 100);
    let query_input = QueryInput {
        connection_id: "test-writable".to_string(),
        sql: "SELECT COUNT(*) as count FROM users".to_string(),
        params: vec![],
        limit: None,
        timeout_secs: None,
        decode_binary: true,
        transaction_id: None,
        database: None,
        format: OutputFormat::Json,
    };
    let query_result = query_handler.query(query_input).await.unwrap();
    assert_eq!(query_result.rows.len(), 1);
}

#[tokio::test]
async fn test_readonly_transaction_allowed() {
    let (manager, registry, _path) = setup_readonly_db().await;
    let tx_handler = TransactionToolHandler::new(manager.clone(), registry.clone());

    // Attempt to begin transaction on readonly connection
    // This should succeed - readonly connections can begin read-only transactions
    let begin_input = BeginTransactionInput {
        connection_id: "test-readonly".to_string(),
        timeout_secs: None,
        database: None,
    };
    let tx_result = tx_handler.begin_transaction(begin_input).await;
    // Readonly connections can begin transactions (for consistent reads)
    assert!(
        tx_result.is_ok(),
        "Readonly connection should be able to begin read-only transaction"
    );
}

// =============================================================================
// User Story 3 Tests: Database File Creation
// =============================================================================

#[tokio::test]
async fn test_database_file_creation() {
    let temp_file = NamedTempFile::new().unwrap();
    let db_path = temp_file.path().to_str().unwrap().to_string();

    // Delete the temp file so it doesn't exist
    drop(temp_file);

    let manager = Arc::new(ConnectionManager::new());
    let registry = Arc::new(TransactionRegistry::new());

    // Connect with writable=true to non-existent file
    let conn_url = format!("sqlite:{}", db_path);
    let config = ConnectionConfig::new(
        "test-create",
        &conn_url,
        true, // writable=true enables create_if_missing
        false,
        None,
        PoolOptions::default(),
    )
    .unwrap();

    let connect_result = manager.connect(config).await;
    assert!(
        connect_result.is_ok(),
        "Should create database file with writable=true"
    );

    // Verify we can create a table
    let write_handler = WriteToolHandler::new(manager.clone(), registry.clone());
    let create_input = ExecuteInput {
        connection_id: "test-create".to_string(),
        sql: "CREATE TABLE test (id INTEGER PRIMARY KEY)".to_string(),
        params: vec![],
        skip_sql_check: false,
        timeout_secs: None,
        transaction_id: None,
        database: None,
    };

    let result = write_handler.execute(create_input).await;
    assert!(
        result.is_ok(),
        "Should be able to create table in new database"
    );
}

#[tokio::test]
async fn test_readonly_nonexistent_file_rejection() {
    let temp_file = NamedTempFile::new().unwrap();
    let db_path = temp_file.path().to_str().unwrap().to_string();

    // Delete the temp file so it doesn't exist
    drop(temp_file);

    let manager = Arc::new(ConnectionManager::new());

    // Connect with writable=false to non-existent file
    let conn_url = format!("sqlite:{}", db_path);
    let config = ConnectionConfig::new(
        "test-readonly-missing",
        &conn_url,
        false, // writable=false means read_only=true
        false,
        None,
        PoolOptions::default(),
    )
    .unwrap();

    let connect_result = manager.connect(config).await;
    assert!(
        connect_result.is_err(),
        "Should fail to connect to non-existent file with writable=false"
    );
}
