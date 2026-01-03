//! Integration tests for the list_transactions tool.

use db_mcp_server::config::PoolOptions;
use db_mcp_server::db::{ConnectionManager, TransactionRegistry};
use db_mcp_server::models::ConnectionConfig;
use db_mcp_server::tools::{
    BeginTransactionInput, ListTransactionsInput, ListTransactionsOutput, RollbackInput,
    TransactionInfo, TransactionToolHandler,
};
use std::sync::Arc;
use tempfile::NamedTempFile;

/// Create a test SQLite database and return the connection manager with it configured.
async fn create_test_connection_manager() -> (Arc<ConnectionManager>, String, NamedTempFile) {
    create_test_connection_manager_with_pool_size(5).await
}

/// Create a test SQLite database with custom max connections.
async fn create_test_connection_manager_with_pool_size(
    max_connections: u32,
) -> (Arc<ConnectionManager>, String, NamedTempFile) {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let db_path = temp_file.path().to_str().unwrap();
    let connection_id = "test_db";
    let url = format!("sqlite:{}?mode=rwc", db_path);

    let manager = Arc::new(ConnectionManager::new());
    let pool_options = PoolOptions {
        max_connections: Some(max_connections),
        ..Default::default()
    };
    let config = ConnectionConfig::new(
        connection_id,
        &url,
        true,  // writable
        false, // not server_level
        None,
        pool_options,
    )
    .expect("Failed to create config");
    manager
        .connect(config)
        .await
        .expect("Failed to connect to test database");

    (manager, connection_id.to_string(), temp_file)
}

#[tokio::test]
async fn test_list_transactions_empty_returns_no_transactions_message() {
    let (manager, _conn_id, _temp_file) = create_test_connection_manager().await;
    let registry = Arc::new(TransactionRegistry::new());
    let handler = TransactionToolHandler::new(manager, registry);

    let input = ListTransactionsInput {
        connection_id: None,
    };
    let result = handler.list_transactions(input).await;

    assert!(result.is_ok());
    let output = result.unwrap();
    assert_eq!(output.count, 0);
    assert!(output.transactions.is_empty());
    assert!(output.message.is_some());
    assert!(
        output
            .message
            .unwrap()
            .to_lowercase()
            .contains("no active transactions")
    );
}

#[tokio::test]
async fn test_list_transactions_shows_active_transaction() {
    let (manager, conn_id, _temp_file) = create_test_connection_manager().await;
    let registry = Arc::new(TransactionRegistry::new());
    let handler = TransactionToolHandler::new(manager, registry.clone());

    // Start a transaction
    let begin_input = BeginTransactionInput {
        connection_id: conn_id.clone(),
        timeout_secs: Some(60),
    };
    let begin_result = handler.begin_transaction(begin_input).await;
    assert!(begin_result.is_ok());
    let tx_id = begin_result.unwrap().transaction_id;

    // List transactions
    let list_input = ListTransactionsInput {
        connection_id: None,
    };
    let list_result = handler.list_transactions(list_input).await;

    assert!(list_result.is_ok());
    let output = list_result.unwrap();
    assert_eq!(output.count, 1);
    assert_eq!(output.transactions.len(), 1);

    let tx_info = &output.transactions[0];
    assert_eq!(tx_info.transaction_id, tx_id);
    assert_eq!(tx_info.connection_id, conn_id);
    assert!(tx_info.duration_secs < 5); // Should be very recent
    assert_eq!(tx_info.timeout_secs, 60);
    assert!(!tx_info.is_long_running);

    // Clean up - rollback the transaction
    let rollback_input = RollbackInput {
        connection_id: conn_id,
        transaction_id: tx_id,
    };
    let _ = handler.rollback(rollback_input).await;
}

#[tokio::test]
async fn test_list_transactions_multiple_transactions() {
    let (manager, conn_id, _temp_file) = create_test_connection_manager().await;
    let registry = Arc::new(TransactionRegistry::new());
    let handler = TransactionToolHandler::new(manager, registry.clone());

    // Start first transaction
    let begin_input1 = BeginTransactionInput {
        connection_id: conn_id.clone(),
        timeout_secs: Some(60),
    };
    let tx1 = handler
        .begin_transaction(begin_input1)
        .await
        .unwrap()
        .transaction_id;

    // Start second transaction
    let begin_input2 = BeginTransactionInput {
        connection_id: conn_id.clone(),
        timeout_secs: Some(120),
    };
    let tx2 = handler
        .begin_transaction(begin_input2)
        .await
        .unwrap()
        .transaction_id;

    // List transactions
    let list_input = ListTransactionsInput {
        connection_id: None,
    };
    let list_result = handler.list_transactions(list_input).await;

    assert!(list_result.is_ok());
    let output = list_result.unwrap();
    assert_eq!(output.count, 2);
    assert_eq!(output.transactions.len(), 2);

    // Verify both transactions are listed
    let tx_ids: Vec<&str> = output
        .transactions
        .iter()
        .map(|t| t.transaction_id.as_str())
        .collect();
    assert!(tx_ids.contains(&tx1.as_str()));
    assert!(tx_ids.contains(&tx2.as_str()));

    // Clean up
    let _ = handler
        .rollback(RollbackInput {
            connection_id: conn_id.clone(),
            transaction_id: tx1,
        })
        .await;
    let _ = handler
        .rollback(RollbackInput {
            connection_id: conn_id,
            transaction_id: tx2,
        })
        .await;
}

#[tokio::test]
async fn test_transaction_info_serialization() {
    let info = TransactionInfo {
        transaction_id: "tx_test123".to_string(),
        connection_id: "conn1".to_string(),
        started_at: "2026-01-02T10:30:00Z".to_string(),
        duration_secs: 45,
        timeout_secs: 60,
        is_long_running: false,
    };

    let json = serde_json::to_string(&info).unwrap();
    assert!(json.contains("\"transaction_id\":\"tx_test123\""));
    assert!(json.contains("\"is_long_running\":false"));
}

#[tokio::test]
async fn test_list_transactions_output_serialization() {
    let output = ListTransactionsOutput {
        transactions: vec![],
        count: 0,
        message: Some("No active transactions".to_string()),
    };

    let json = serde_json::to_string(&output).unwrap();
    assert!(json.contains("\"count\":0"));
    assert!(json.contains("\"transactions\":[]"));
    assert!(json.contains("No active transactions"));
}

// User Story 3 tests - filter by connection_id

#[tokio::test]
async fn test_list_transactions_filter_by_connection_id() {
    let (manager, conn_id, _temp_file) = create_test_connection_manager().await;
    let registry = Arc::new(TransactionRegistry::new());
    let handler = TransactionToolHandler::new(manager, registry.clone());

    // Start a transaction
    let begin_input = BeginTransactionInput {
        connection_id: conn_id.clone(),
        timeout_secs: Some(60),
    };
    let tx_id = handler
        .begin_transaction(begin_input)
        .await
        .unwrap()
        .transaction_id;

    // List with filter matching the connection
    let list_input = ListTransactionsInput {
        connection_id: Some(conn_id.clone()),
    };
    let list_result = handler.list_transactions(list_input).await;

    assert!(list_result.is_ok());
    let output = list_result.unwrap();
    assert_eq!(output.count, 1);
    assert_eq!(output.transactions[0].transaction_id, tx_id);

    // Clean up
    let _ = handler
        .rollback(RollbackInput {
            connection_id: conn_id,
            transaction_id: tx_id,
        })
        .await;
}

#[tokio::test]
async fn test_list_transactions_filter_by_nonexistent_connection_returns_error() {
    let (manager, _conn_id, _temp_file) = create_test_connection_manager().await;
    let registry = Arc::new(TransactionRegistry::new());
    let handler = TransactionToolHandler::new(manager, registry);

    let list_input = ListTransactionsInput {
        connection_id: Some("nonexistent_connection".to_string()),
    };
    let list_result = handler.list_transactions(list_input).await;

    assert!(list_result.is_err());
}

#[tokio::test]
async fn test_list_transactions_filter_returns_empty_for_valid_connection_no_transactions() {
    let (manager, conn_id, _temp_file) = create_test_connection_manager().await;
    let registry = Arc::new(TransactionRegistry::new());
    let handler = TransactionToolHandler::new(manager, registry);

    // List with filter for valid connection but no transactions
    let list_input = ListTransactionsInput {
        connection_id: Some(conn_id.clone()),
    };
    let list_result = handler.list_transactions(list_input).await;

    assert!(list_result.is_ok());
    let output = list_result.unwrap();
    assert_eq!(output.count, 0);
    assert!(output.transactions.is_empty());
    assert!(output.message.is_some());
}
