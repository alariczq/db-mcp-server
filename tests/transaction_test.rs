//! Integration tests for transaction functionality.

use db_mcp_server::config::PoolOptions;
use db_mcp_server::db::{ConnectionManager, TransactionRegistry};
use db_mcp_server::models::ConnectionConfig;
use db_mcp_server::tools::OutputFormat;
use db_mcp_server::tools::query::{QueryInput, QueryToolHandler};
use db_mcp_server::tools::transaction::{
    BeginTransactionInput, CommitInput, RollbackInput, TransactionToolHandler,
};
use db_mcp_server::tools::write::{ExecuteInput, WriteToolHandler};
use std::sync::Arc;

/// Test that requires a running MySQL database.
/// Set TEST_MYSQL_URL environment variable to run this test.
/// Example: TEST_MYSQL_URL="mysql://root:root@localhost:3306/test_db?writable=true"
#[tokio::test]
async fn test_transaction_rollback() {
    let mysql_url = match std::env::var("TEST_MYSQL_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("Skipping test: TEST_MYSQL_URL not set");
            return;
        }
    };

    // Setup
    let manager = Arc::new(ConnectionManager::new());
    let config = ConnectionConfig::new(
        "test_mysql",
        &mysql_url,
        true,
        false,
        Some("test".to_string()),
        PoolOptions::default(),
    )
    .unwrap();
    manager.connect(config).await.unwrap();

    let registry = Arc::new(TransactionRegistry::new());

    let tx_handler = TransactionToolHandler::new(manager.clone(), registry.clone());
    let write_handler = WriteToolHandler::new(manager.clone(), registry.clone());
    let query_handler = QueryToolHandler::new(manager.clone());

    // Create test table if not exists
    let create_result = write_handler
        .execute(ExecuteInput {
            connection_id: "test_mysql".to_string(),
            sql: "CREATE TABLE IF NOT EXISTS tx_test (id INT PRIMARY KEY, name VARCHAR(100))"
                .to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            dangerous_operation_allowed: false,
            database: None,
        })
        .await;

    if let Err(e) = &create_result {
        eprintln!("Warning: Could not create table: {}", e);
    }

    // Clean up any existing test data
    let _ = write_handler
        .execute(ExecuteInput {
            connection_id: "test_mysql".to_string(),
            sql: "DELETE FROM tx_test WHERE id = 12345".to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            dangerous_operation_allowed: false,
            database: None,
        })
        .await;

    // Begin transaction
    let begin_result = tx_handler
        .begin_transaction(BeginTransactionInput {
            connection_id: "test_mysql".to_string(),
            timeout_secs: None,
            database: None,
        })
        .await
        .expect("Failed to begin transaction");

    let tx_id = begin_result.transaction_id.clone();
    println!("Transaction started: {}", tx_id);

    // Insert data within transaction
    let insert_result = write_handler
        .execute(ExecuteInput {
            connection_id: "test_mysql".to_string(),
            sql: "INSERT INTO tx_test (id, name) VALUES (12345, 'rollback_test')".to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: Some(tx_id.clone()),
            dangerous_operation_allowed: false,
            database: None,
        })
        .await
        .expect("Failed to insert in transaction");

    assert_eq!(insert_result.rows_affected, 1);
    println!("Inserted 1 row in transaction");

    // Rollback the transaction
    let rollback_result = tx_handler
        .rollback(RollbackInput {
            connection_id: "test_mysql".to_string(),
            transaction_id: tx_id.clone(),
        })
        .await
        .expect("Failed to rollback transaction");

    assert!(rollback_result.success);
    println!("Transaction rolled back");

    // Verify data was NOT persisted
    let query_result = query_handler
        .query(QueryInput {
            connection_id: "test_mysql".to_string(),
            sql: "SELECT * FROM tx_test WHERE id = 12345".to_string(),
            params: vec![],
            limit: None,
            timeout_secs: None,
            format: OutputFormat::Json,
            decode_binary: false,
            transaction_id: None,
            database: None,
        })
        .await
        .expect("Failed to query");

    assert_eq!(
        query_result.row_count, 0,
        "Data should NOT exist after rollback!"
    );
    println!("Verified: No data persisted after rollback - Transaction works correctly!");
}

#[tokio::test]
async fn test_transaction_commit() {
    let mysql_url = match std::env::var("TEST_MYSQL_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("Skipping test: TEST_MYSQL_URL not set");
            return;
        }
    };

    // Setup
    let manager = Arc::new(ConnectionManager::new());
    let config = ConnectionConfig::new(
        "test_mysql",
        &mysql_url,
        true,
        false,
        Some("test".to_string()),
        PoolOptions::default(),
    )
    .unwrap();
    manager.connect(config).await.unwrap();

    let registry = Arc::new(TransactionRegistry::new());

    let tx_handler = TransactionToolHandler::new(manager.clone(), registry.clone());
    let write_handler = WriteToolHandler::new(manager.clone(), registry.clone());
    let query_handler = QueryToolHandler::new(manager.clone());

    // Clean up any existing test data
    let _ = write_handler
        .execute(ExecuteInput {
            connection_id: "test_mysql".to_string(),
            sql: "DELETE FROM tx_test WHERE id = 99999".to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            dangerous_operation_allowed: false,
            database: None,
        })
        .await;

    // Begin transaction
    let begin_result = tx_handler
        .begin_transaction(BeginTransactionInput {
            connection_id: "test_mysql".to_string(),
            timeout_secs: None,
            database: None,
        })
        .await
        .expect("Failed to begin transaction");

    let tx_id = begin_result.transaction_id.clone();

    // Insert data within transaction
    write_handler
        .execute(ExecuteInput {
            connection_id: "test_mysql".to_string(),
            sql: "INSERT INTO tx_test (id, name) VALUES (99999, 'commit_test')".to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: Some(tx_id.clone()),
            dangerous_operation_allowed: false,
            database: None,
        })
        .await
        .expect("Failed to insert in transaction");

    // Commit the transaction
    tx_handler
        .commit(CommitInput {
            connection_id: "test_mysql".to_string(),
            transaction_id: tx_id.clone(),
        })
        .await
        .expect("Failed to commit transaction");

    // Verify data WAS persisted
    let query_result = query_handler
        .query(QueryInput {
            connection_id: "test_mysql".to_string(),
            sql: "SELECT * FROM tx_test WHERE id = 99999".to_string(),
            params: vec![],
            limit: None,
            timeout_secs: None,
            format: OutputFormat::Json,
            decode_binary: false,
            transaction_id: None,
            database: None,
        })
        .await
        .expect("Failed to query");

    assert_eq!(query_result.row_count, 1, "Data SHOULD exist after commit!");
    println!("Verified: Data persisted after commit - Transaction works correctly!");

    // Clean up
    let _ = write_handler
        .execute(ExecuteInput {
            connection_id: "test_mysql".to_string(),
            sql: "DELETE FROM tx_test WHERE id = 99999".to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            dangerous_operation_allowed: false,
            database: None,
        })
        .await;
}
