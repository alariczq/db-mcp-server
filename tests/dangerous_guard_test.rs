//! Integration tests for dangerous operation guard.
//!
//! These tests verify that the dangerous operation guard correctly blocks
//! dangerous operations by default and allows them when explicitly confirmed.

use db_mcp_server::config::PoolOptions;
use db_mcp_server::db::{ConnectionManager, TransactionRegistry};
use db_mcp_server::models::ConnectionConfig;
use db_mcp_server::tools::write::{ExecuteInput, WriteToolHandler};
use std::sync::Arc;

/// Helper to setup test environment
async fn setup_test_handler() -> (WriteToolHandler, String) {
    let manager = Arc::new(ConnectionManager::new());
    let config = ConnectionConfig::new(
        "test_sqlite",
        "sqlite::memory:",
        true,
        false,
        None,
        PoolOptions::default(),
    )
    .unwrap();
    manager.connect(config).await.unwrap();

    let registry = Arc::new(TransactionRegistry::new());
    let handler = WriteToolHandler::new(manager, registry);

    // Create test table
    handler
        .execute(ExecuteInput {
            connection_id: "test_sqlite".to_string(),
            sql: "CREATE TABLE test_users (id INTEGER PRIMARY KEY, name TEXT)".to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            skip_sql_check: false,
            database: None,
        })
        .await
        .expect("Failed to create test table");

    (handler, "test_sqlite".to_string())
}

// =========================================================================
// User Story 1: Block Dangerous Operations by Default
// =========================================================================

#[tokio::test]
async fn test_drop_table_blocked_by_default() {
    let (handler, conn_id) = setup_test_handler().await;

    let result = handler
        .execute(ExecuteInput {
            connection_id: conn_id,
            sql: "DROP TABLE test_users".to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            skip_sql_check: false,
            database: None,
        })
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("DROP TABLE"),
        "Error should mention DROP TABLE: {}",
        err
    );
    assert!(
        err.to_string().contains("skip_sql_check"),
        "Error should mention how to bypass: {}",
        err
    );
}

#[tokio::test]
async fn test_drop_database_blocked_by_default() {
    let (handler, conn_id) = setup_test_handler().await;

    let result = handler
        .execute(ExecuteInput {
            connection_id: conn_id,
            sql: "DROP DATABASE test_db".to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            skip_sql_check: false,
            database: None,
        })
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("DROP DATABASE"),
        "Error should mention DROP DATABASE: {}",
        err
    );
}

#[tokio::test]
async fn test_delete_without_where_blocked_by_default() {
    let (handler, conn_id) = setup_test_handler().await;

    let result = handler
        .execute(ExecuteInput {
            connection_id: conn_id,
            sql: "DELETE FROM test_users".to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            skip_sql_check: false,
            database: None,
        })
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("DELETE without WHERE"),
        "Error should mention DELETE without WHERE: {}",
        err
    );
}

#[tokio::test]
async fn test_update_without_where_blocked_by_default() {
    let (handler, conn_id) = setup_test_handler().await;

    let result = handler
        .execute(ExecuteInput {
            connection_id: conn_id,
            sql: "UPDATE test_users SET name = 'blocked'".to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            skip_sql_check: false,
            database: None,
        })
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("UPDATE without WHERE"),
        "Error should mention UPDATE without WHERE: {}",
        err
    );
}

#[tokio::test]
async fn test_truncate_blocked_by_default() {
    let (handler, conn_id) = setup_test_handler().await;

    let result = handler
        .execute(ExecuteInput {
            connection_id: conn_id,
            sql: "TRUNCATE TABLE test_users".to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            skip_sql_check: false,
            database: None,
        })
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("TRUNCATE"),
        "Error should mention TRUNCATE: {}",
        err
    );
}

// =========================================================================
// User Story 2: Execute with Explicit Confirmation
// =========================================================================

#[tokio::test]
async fn test_drop_table_allowed_with_confirmation() {
    let (handler, conn_id) = setup_test_handler().await;

    let result = handler
        .execute(ExecuteInput {
            connection_id: conn_id,
            sql: "DROP TABLE test_users".to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            skip_sql_check: true,
            database: None,
        })
        .await;

    assert!(
        result.is_ok(),
        "DROP TABLE should succeed with confirmation: {:?}",
        result
    );
}

#[tokio::test]
async fn test_delete_without_where_allowed_with_confirmation() {
    let (handler, conn_id) = setup_test_handler().await;

    // Insert some data first
    handler
        .execute(ExecuteInput {
            connection_id: conn_id.clone(),
            sql: "INSERT INTO test_users (id, name) VALUES (1, 'Alice')".to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            skip_sql_check: false,
            database: None,
        })
        .await
        .expect("Insert failed");

    let result = handler
        .execute(ExecuteInput {
            connection_id: conn_id,
            sql: "DELETE FROM test_users".to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            skip_sql_check: true,
            database: None,
        })
        .await;

    assert!(
        result.is_ok(),
        "DELETE without WHERE should succeed with confirmation: {:?}",
        result
    );
    assert_eq!(result.unwrap().rows_affected, 1);
}

#[tokio::test]
async fn test_update_without_where_allowed_with_confirmation() {
    let (handler, conn_id) = setup_test_handler().await;

    // Insert some data first
    handler
        .execute(ExecuteInput {
            connection_id: conn_id.clone(),
            sql: "INSERT INTO test_users (id, name) VALUES (1, 'Alice'), (2, 'Bob')".to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            skip_sql_check: false,
            database: None,
        })
        .await
        .expect("Insert failed");

    let result = handler
        .execute(ExecuteInput {
            connection_id: conn_id,
            sql: "UPDATE test_users SET name = 'Updated'".to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            skip_sql_check: true,
            database: None,
        })
        .await;

    assert!(
        result.is_ok(),
        "UPDATE without WHERE should succeed with confirmation: {:?}",
        result
    );
    assert_eq!(result.unwrap().rows_affected, 2);
}

// =========================================================================
// User Story 3: Safe Operations Work Normally
// =========================================================================

#[tokio::test]
async fn test_insert_works_without_confirmation() {
    let (handler, conn_id) = setup_test_handler().await;

    let result = handler
        .execute(ExecuteInput {
            connection_id: conn_id,
            sql: "INSERT INTO test_users (id, name) VALUES (1, 'Alice')".to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            skip_sql_check: false,
            database: None,
        })
        .await;

    assert!(
        result.is_ok(),
        "INSERT should work without confirmation: {:?}",
        result
    );
    assert_eq!(result.unwrap().rows_affected, 1);
}

#[tokio::test]
async fn test_delete_with_where_works_without_confirmation() {
    let (handler, conn_id) = setup_test_handler().await;

    // Insert some data first
    handler
        .execute(ExecuteInput {
            connection_id: conn_id.clone(),
            sql: "INSERT INTO test_users (id, name) VALUES (1, 'Alice')".to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            skip_sql_check: false,
            database: None,
        })
        .await
        .expect("Insert failed");

    let result = handler
        .execute(ExecuteInput {
            connection_id: conn_id,
            sql: "DELETE FROM test_users WHERE id = 1".to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            skip_sql_check: false,
            database: None,
        })
        .await;

    assert!(
        result.is_ok(),
        "DELETE with WHERE should work without confirmation: {:?}",
        result
    );
    assert_eq!(result.unwrap().rows_affected, 1);
}

#[tokio::test]
async fn test_update_with_where_works_without_confirmation() {
    let (handler, conn_id) = setup_test_handler().await;

    // Insert some data first
    handler
        .execute(ExecuteInput {
            connection_id: conn_id.clone(),
            sql: "INSERT INTO test_users (id, name) VALUES (1, 'Alice')".to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            skip_sql_check: false,
            database: None,
        })
        .await
        .expect("Insert failed");

    let result = handler
        .execute(ExecuteInput {
            connection_id: conn_id,
            sql: "UPDATE test_users SET name = 'Bob' WHERE id = 1".to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            skip_sql_check: false,
            database: None,
        })
        .await;

    assert!(
        result.is_ok(),
        "UPDATE with WHERE should work without confirmation: {:?}",
        result
    );
    assert_eq!(result.unwrap().rows_affected, 1);
}

#[tokio::test]
async fn test_create_table_works_without_confirmation() {
    let (handler, conn_id) = setup_test_handler().await;

    let result = handler
        .execute(ExecuteInput {
            connection_id: conn_id,
            sql: "CREATE TABLE another_table (id INTEGER PRIMARY KEY)".to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            skip_sql_check: false,
            database: None,
        })
        .await;

    assert!(
        result.is_ok(),
        "CREATE TABLE should work without confirmation: {:?}",
        result
    );
}

#[tokio::test]
async fn test_alter_table_works_without_confirmation() {
    let (handler, conn_id) = setup_test_handler().await;

    let result = handler
        .execute(ExecuteInput {
            connection_id: conn_id,
            sql: "ALTER TABLE test_users ADD COLUMN email TEXT".to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            skip_sql_check: false,
            database: None,
        })
        .await;

    assert!(
        result.is_ok(),
        "ALTER TABLE should work without confirmation: {:?}",
        result
    );
}

// =========================================================================
// User Story 4: Precise SQL Parsing (bypass prevention)
// =========================================================================

#[tokio::test]
async fn test_comment_before_dangerous_operation_still_blocked() {
    let (handler, conn_id) = setup_test_handler().await;

    let result = handler
        .execute(ExecuteInput {
            connection_id: conn_id,
            sql: "-- This is a comment\nDELETE FROM test_users".to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            skip_sql_check: false,
            database: None,
        })
        .await;

    assert!(
        result.is_err(),
        "DELETE without WHERE should be blocked even with comment"
    );
}

#[tokio::test]
async fn test_string_literal_with_dangerous_keyword_not_blocked() {
    let (handler, conn_id) = setup_test_handler().await;

    let result = handler
        .execute(ExecuteInput {
            connection_id: conn_id,
            sql: "INSERT INTO test_users (id, name) VALUES (1, 'DELETE FROM test_users')"
                .to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            skip_sql_check: false,
            database: None,
        })
        .await;

    assert!(
        result.is_ok(),
        "INSERT with dangerous keyword in string should not be blocked: {:?}",
        result
    );
}

#[tokio::test]
async fn test_multi_statement_with_dangerous_blocked() {
    let (handler, conn_id) = setup_test_handler().await;

    let result = handler
        .execute(ExecuteInput {
            connection_id: conn_id,
            sql: "INSERT INTO test_users (id, name) VALUES (1, 'Alice'); DELETE FROM test_users"
                .to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            skip_sql_check: false,
            database: None,
        })
        .await;

    assert!(
        result.is_err(),
        "Multi-statement with dangerous operation should be blocked"
    );
}

// =========================================================================
// ALTER TABLE DROP COLUMN protection
// =========================================================================

#[tokio::test]
async fn test_alter_table_drop_column_blocked_by_default() {
    let (handler, conn_id) = setup_test_handler().await;

    // First add a column to drop
    handler
        .execute(ExecuteInput {
            connection_id: conn_id.clone(),
            sql: "ALTER TABLE test_users ADD COLUMN email TEXT".to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            skip_sql_check: false,
            database: None,
        })
        .await
        .expect("ALTER TABLE ADD COLUMN should work");

    // Try to drop the column without confirmation
    let result = handler
        .execute(ExecuteInput {
            connection_id: conn_id,
            sql: "ALTER TABLE test_users DROP COLUMN email".to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            skip_sql_check: false,
            database: None,
        })
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("ALTER TABLE DROP COLUMN"),
        "Error should mention ALTER TABLE DROP COLUMN: {}",
        err
    );
}

#[tokio::test]
async fn test_alter_table_drop_column_allowed_with_confirmation() {
    let (handler, conn_id) = setup_test_handler().await;

    // First add a column to drop
    handler
        .execute(ExecuteInput {
            connection_id: conn_id.clone(),
            sql: "ALTER TABLE test_users ADD COLUMN email TEXT".to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            skip_sql_check: false,
            database: None,
        })
        .await
        .expect("ALTER TABLE ADD COLUMN should work");

    // Drop the column with confirmation
    let result = handler
        .execute(ExecuteInput {
            connection_id: conn_id,
            sql: "ALTER TABLE test_users DROP COLUMN email".to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            skip_sql_check: true,
            database: None,
        })
        .await;

    assert!(
        result.is_ok(),
        "ALTER TABLE DROP COLUMN should succeed with confirmation: {:?}",
        result
    );
}

// =========================================================================
// SELECT blocked in execute tool
// =========================================================================

#[tokio::test]
async fn test_select_blocked_in_execute() {
    let (handler, conn_id) = setup_test_handler().await;

    let result = handler
        .execute(ExecuteInput {
            connection_id: conn_id,
            sql: "SELECT * FROM test_users".to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            skip_sql_check: false,
            database: None,
        })
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("read-only"),
        "Error should mention read-only operation: {}",
        err
    );
    assert!(
        err.to_string().contains("query"),
        "Error should suggest using query tool: {}",
        err
    );
}

#[tokio::test]
async fn test_show_tables_blocked_in_execute() {
    let (handler, conn_id) = setup_test_handler().await;

    let result = handler
        .execute(ExecuteInput {
            connection_id: conn_id,
            sql: "SHOW TABLES".to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            skip_sql_check: false,
            database: None,
        })
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("read-only"),
        "Error should mention read-only operation: {}",
        err
    );
}
