//! Integration tests for the explain tool.

use db_mcp_server::db::{ConnectionManager, TransactionRegistry};
use db_mcp_server::models::ConnectionConfig;
use db_mcp_server::tools::{ExplainInput, ExplainOutput, ExplainToolHandler};
use std::sync::Arc;
use tempfile::NamedTempFile;

/// Create a test SQLite database with a sample table.
async fn create_test_connection_manager() -> (Arc<ConnectionManager>, String, NamedTempFile) {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let db_path = temp_file.path().to_str().unwrap();
    let connection_id = "test_db";
    let url = format!("sqlite:{}?mode=rwc", db_path);

    let manager = Arc::new(ConnectionManager::new());
    let config = ConnectionConfig::new(
        connection_id,
        &url,
        true,  // writable
        false, // not server_level
        None,
    )
    .expect("Failed to create config");
    manager
        .connect(config)
        .await
        .expect("Failed to connect to test database");

    // Create a sample table for testing
    let pool = manager.get_pool(connection_id).await.unwrap();
    if let db_mcp_server::db::DbPool::SQLite(p) = pool {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)",
        )
        .execute(&p)
        .await
        .expect("Failed to create test table");
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)")
            .execute(&p)
            .await
            .expect("Failed to create index");
    }

    (manager, connection_id.to_string(), temp_file)
}

#[tokio::test]
async fn test_explain_simple_select() {
    let (manager, conn_id, _temp_file) = create_test_connection_manager().await;
    let registry = Arc::new(TransactionRegistry::new());
    let handler = ExplainToolHandler::new(manager, registry);

    let input = ExplainInput {
        connection_id: conn_id,
        sql: "SELECT * FROM users".to_string(),
        params: vec![],
        transaction_id: None,
        timeout_secs: None,
        format: Default::default(),
    };

    let result = handler.explain(input).await;
    assert!(result.is_ok());

    let output = result.unwrap();
    assert_eq!(output.sql, "SELECT * FROM users");
    assert!(!output.plan.is_empty());
    // SQLite EXPLAIN QUERY PLAN returns rows with 'detail' column
    assert!(output.plan[0].contains_key("detail") || output.plan[0].contains_key("id"));
}

#[tokio::test]
async fn test_explain_select_with_where() {
    let (manager, conn_id, _temp_file) = create_test_connection_manager().await;
    let registry = Arc::new(TransactionRegistry::new());
    let handler = ExplainToolHandler::new(manager, registry);

    let input = ExplainInput {
        connection_id: conn_id,
        sql: "SELECT * FROM users WHERE email = 'test@example.com'".to_string(),
        params: vec![],
        transaction_id: None,
        timeout_secs: None,
        format: Default::default(),
    };

    let result = handler.explain(input).await;
    assert!(result.is_ok());

    let output = result.unwrap();
    assert!(!output.plan.is_empty());
    // Check that the plan contains some output
    let plan_str = serde_json::to_string(&output.plan).unwrap();
    assert!(!plan_str.is_empty());
}

#[tokio::test]
async fn test_explain_with_parameters() {
    let (manager, conn_id, _temp_file) = create_test_connection_manager().await;
    let registry = Arc::new(TransactionRegistry::new());
    let handler = ExplainToolHandler::new(manager, registry);

    let input = ExplainInput {
        connection_id: conn_id,
        sql: "SELECT * FROM users WHERE id = ?".to_string(),
        params: vec![db_mcp_server::tools::QueryParamInput::Int(1)],
        transaction_id: None,
        timeout_secs: None,
        format: Default::default(),
    };

    let result = handler.explain(input).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_explain_insert_statement() {
    let (manager, conn_id, _temp_file) = create_test_connection_manager().await;
    let registry = Arc::new(TransactionRegistry::new());
    let handler = ExplainToolHandler::new(manager, registry);

    // SQLite uses plain EXPLAIN for non-SELECT statements (returns opcodes)
    let input = ExplainInput {
        connection_id: conn_id,
        sql: "INSERT INTO users (name, email) VALUES ('test', 'test@test.com')".to_string(),
        params: vec![],
        transaction_id: None,
        timeout_secs: None,
        format: Default::default(),
    };

    let result = handler.explain(input).await;
    assert!(result.is_ok());

    let output = result.unwrap();
    // For non-SELECT, SQLite returns opcode-level EXPLAIN (different format)
    assert!(!output.plan.is_empty());
}

#[tokio::test]
async fn test_explain_invalid_sql() {
    let (manager, conn_id, _temp_file) = create_test_connection_manager().await;
    let registry = Arc::new(TransactionRegistry::new());
    let handler = ExplainToolHandler::new(manager, registry);

    let input = ExplainInput {
        connection_id: conn_id,
        sql: "SELCT * FORM nonexistent".to_string(), // Intentionally misspelled
        params: vec![],
        transaction_id: None,
        timeout_secs: None,
        format: Default::default(),
    };

    let result = handler.explain(input).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_explain_empty_sql_returns_error() {
    let (manager, conn_id, _temp_file) = create_test_connection_manager().await;
    let registry = Arc::new(TransactionRegistry::new());
    let handler = ExplainToolHandler::new(manager, registry);

    let input = ExplainInput {
        connection_id: conn_id,
        sql: "   ".to_string(),
        params: vec![],
        transaction_id: None,
        timeout_secs: None,
        format: Default::default(),
    };

    let result = handler.explain(input).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_explain_invalid_connection_returns_error() {
    let (manager, _conn_id, _temp_file) = create_test_connection_manager().await;
    let registry = Arc::new(TransactionRegistry::new());
    let handler = ExplainToolHandler::new(manager, registry);

    let input = ExplainInput {
        connection_id: "nonexistent_connection".to_string(),
        sql: "SELECT 1".to_string(),
        params: vec![],
        transaction_id: None,
        timeout_secs: None,
        format: Default::default(),
    };

    let result = handler.explain(input).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_explain_output_has_execution_time() {
    let (manager, conn_id, _temp_file) = create_test_connection_manager().await;
    let registry = Arc::new(TransactionRegistry::new());
    let handler = ExplainToolHandler::new(manager, registry);

    let input = ExplainInput {
        connection_id: conn_id,
        sql: "SELECT * FROM users".to_string(),
        params: vec![],
        transaction_id: None,
        timeout_secs: None,
        format: Default::default(),
    };

    let result = handler.explain(input).await;
    assert!(result.is_ok());

    let output = result.unwrap();
    // Execution time should be recorded (likely <100ms for simple query)
    assert!(output.execution_time_ms < 1000);
}

#[tokio::test]
async fn test_explain_output_serialization() {
    let output = ExplainOutput {
        plan: vec![{
            let mut map = serde_json::Map::new();
            map.insert("id".to_string(), serde_json::json!(0));
            map.insert("detail".to_string(), serde_json::json!("SCAN users"));
            map
        }],
        sql: "SELECT * FROM users".to_string(),
        formatted: None,
        execution_time_ms: 5,
    };

    let json = serde_json::to_string(&output).unwrap();
    assert!(json.contains("\"sql\":\"SELECT * FROM users\""));
    assert!(json.contains("\"execution_time_ms\":5"));
    assert!(json.contains("SCAN users"));
}

#[tokio::test]
async fn test_explain_with_table_format() {
    let (manager, conn_id, _temp_file) = create_test_connection_manager().await;
    let registry = Arc::new(TransactionRegistry::new());
    let handler = ExplainToolHandler::new(manager, registry);

    let input = ExplainInput {
        connection_id: conn_id,
        sql: "SELECT * FROM users".to_string(),
        params: vec![],
        transaction_id: None,
        timeout_secs: None,
        format: db_mcp_server::tools::format::OutputFormat::Table,
    };

    let result = handler.explain(input).await;
    assert!(result.is_ok());

    let output = result.unwrap();
    assert!(output.plan.is_empty()); // plan should be empty when formatted
    assert!(output.formatted.is_some());
    let formatted = output.formatted.unwrap();
    // Table format should have separator lines
    assert!(formatted.contains("+"));
    assert!(formatted.contains("|"));
}

#[tokio::test]
async fn test_explain_with_markdown_format() {
    let (manager, conn_id, _temp_file) = create_test_connection_manager().await;
    let registry = Arc::new(TransactionRegistry::new());
    let handler = ExplainToolHandler::new(manager, registry);

    let input = ExplainInput {
        connection_id: conn_id,
        sql: "SELECT * FROM users".to_string(),
        params: vec![],
        transaction_id: None,
        timeout_secs: None,
        format: db_mcp_server::tools::format::OutputFormat::Markdown,
    };

    let result = handler.explain(input).await;
    assert!(result.is_ok());

    let output = result.unwrap();
    assert!(output.plan.is_empty()); // plan should be empty when formatted
    assert!(output.formatted.is_some());
    let formatted = output.formatted.unwrap();
    // Markdown format should have header separator
    assert!(formatted.contains("|---"));
}
