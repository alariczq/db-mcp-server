//! Integration tests for UTF-8 encoding in MySQL.

use db_mcp_server::config::PoolOptions;
use db_mcp_server::db::ConnectionManager;
use db_mcp_server::db::TransactionRegistry;
use db_mcp_server::models::ConnectionConfig;
use db_mcp_server::tools::OutputFormat;
use db_mcp_server::tools::query::{QueryInput, QueryToolHandler};
use db_mcp_server::tools::write::{ExecuteInput, WriteToolHandler};
use std::sync::Arc;

/// Test that requires a running MySQL database.
/// Set TEST_MYSQL_URL environment variable to run this test.
#[tokio::test]
async fn test_mysql_utf8_chinese_characters() {
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
    let write_handler = WriteToolHandler::new(manager.clone(), registry);
    let query_handler = QueryToolHandler::new(manager.clone());

    // Create test table with Chinese characters
    let _ = write_handler
        .execute(ExecuteInput {
            connection_id: "test_mysql".to_string(),
            sql: "DROP TABLE IF EXISTS utf8_test".to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            dangerous_operation_allowed: true,
        })
        .await;

    write_handler
        .execute(ExecuteInput {
            connection_id: "test_mysql".to_string(),
            sql: r#"CREATE TABLE utf8_test (
                id INT PRIMARY KEY COMMENT '主键ID',
                name VARCHAR(100) COMMENT '用户名称',
                description TEXT COMMENT '详细描述'
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='中文测试表'"#
                .to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            dangerous_operation_allowed: false,
        })
        .await
        .expect("Failed to create table");

    // Insert Chinese data
    write_handler
        .execute(ExecuteInput {
            connection_id: "test_mysql".to_string(),
            sql: "INSERT INTO utf8_test (id, name, description) VALUES (1, '张三', '这是中文描述')"
                .to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            dangerous_operation_allowed: false,
        })
        .await
        .expect("Failed to insert Chinese data");

    // Query the data
    let result = query_handler
        .query(QueryInput {
            connection_id: "test_mysql".to_string(),
            sql: "SELECT * FROM utf8_test WHERE id = 1".to_string(),
            params: vec![],
            limit: None,
            timeout_secs: None,
            format: OutputFormat::Json,
            decode_binary: false,
            transaction_id: None,
        })
        .await
        .expect("Failed to query");

    assert_eq!(result.row_count, 1);

    // Verify Chinese characters are correctly retrieved
    let row = &result.rows[0];
    let name = row.get("name").and_then(|v| v.as_str()).unwrap();
    let description = row.get("description").and_then(|v| v.as_str()).unwrap();

    assert_eq!(
        name, "张三",
        "Chinese name should be '张三' but got '{}'",
        name
    );
    assert_eq!(
        description, "这是中文描述",
        "Chinese description should be '这是中文描述' but got '{}'",
        description
    );

    println!("UTF-8 test passed! Chinese characters correctly stored and retrieved:");
    println!("  name: {}", name);
    println!("  description: {}", description);

    // Clean up
    let _ = write_handler
        .execute(ExecuteInput {
            connection_id: "test_mysql".to_string(),
            sql: "DROP TABLE utf8_test".to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            dangerous_operation_allowed: true,
        })
        .await;
}

#[tokio::test]
async fn test_mysql_table_comment_utf8() {
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
    let write_handler = WriteToolHandler::new(manager.clone(), registry);
    let query_handler = QueryToolHandler::new(manager.clone());

    // Create test table with Chinese comment
    let _ = write_handler
        .execute(ExecuteInput {
            connection_id: "test_mysql".to_string(),
            sql: "DROP TABLE IF EXISTS comment_utf8_test".to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            dangerous_operation_allowed: true,
        })
        .await;

    write_handler
        .execute(ExecuteInput {
            connection_id: "test_mysql".to_string(),
            sql: r#"CREATE TABLE comment_utf8_test (
                id INT PRIMARY KEY COMMENT '主键ID'
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户操作日志表'"#
                .to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            dangerous_operation_allowed: false,
        })
        .await
        .expect("Failed to create table");

    // Query table comment from information_schema
    let result = query_handler
        .query(QueryInput {
            connection_id: "test_mysql".to_string(),
            sql: "SELECT TABLE_COMMENT FROM information_schema.TABLES WHERE TABLE_NAME = 'comment_utf8_test'".to_string(),
            params: vec![],
            limit: None,
            timeout_secs: None,
            format: OutputFormat::Json,
            decode_binary: false,
            transaction_id: None,
        })
        .await
        .expect("Failed to query");

    assert_eq!(result.row_count, 1);
    let comment = result.rows[0]
        .get("TABLE_COMMENT")
        .and_then(|v| v.as_str())
        .unwrap();

    assert_eq!(
        comment, "用户操作日志表",
        "Table comment should be '用户操作日志表' but got '{}'",
        comment
    );
    println!("Table comment UTF-8 test passed! Comment: {}", comment);

    // Query column comment
    let result = query_handler
        .query(QueryInput {
            connection_id: "test_mysql".to_string(),
            sql: "SELECT COLUMN_COMMENT FROM information_schema.COLUMNS WHERE TABLE_NAME = 'comment_utf8_test' AND COLUMN_NAME = 'id'".to_string(),
            params: vec![],
            limit: None,
            timeout_secs: None,
            format: OutputFormat::Json,
            decode_binary: false,
            transaction_id: None,
        })
        .await
        .expect("Failed to query");

    assert_eq!(result.row_count, 1);
    let column_comment = result.rows[0]
        .get("COLUMN_COMMENT")
        .and_then(|v| v.as_str())
        .unwrap();

    assert_eq!(
        column_comment, "主键ID",
        "Column comment should be '主键ID' but got '{}'",
        column_comment
    );
    println!(
        "Column comment UTF-8 test passed! Comment: {}",
        column_comment
    );

    // Clean up
    let _ = write_handler
        .execute(ExecuteInput {
            connection_id: "test_mysql".to_string(),
            sql: "DROP TABLE comment_utf8_test".to_string(),
            params: vec![],
            timeout_secs: None,
            transaction_id: None,
            dangerous_operation_allowed: true,
        })
        .await;
}
