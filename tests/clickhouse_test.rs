//! Integration tests for ClickHouse support.
//!
//! Tests verify that:
//! - Connection to ClickHouse works with default settings (no password)
//! - Query operations work correctly
//! - Write operations work correctly
//! - Schema introspection works
//! - List databases and tables functionality

use db_mcp_server::config::PoolOptions;
use db_mcp_server::db::{ConnectionManager, TransactionRegistry};
use db_mcp_server::models::{ConnectionConfig, QueryParamInput};
use db_mcp_server::tools::format::OutputFormat;
use db_mcp_server::tools::query::{QueryInput, QueryToolHandler};
use db_mcp_server::tools::schema::{
    DescribeTableInput, ListDatabasesInput, ListTablesInput, SchemaToolHandler,
};
use db_mcp_server::tools::write::{ExecuteInput, WriteToolHandler};
use std::sync::Arc;

/// Create a ClickHouse test connection
async fn setup_clickhouse() -> Option<(Arc<ConnectionManager>, Arc<TransactionRegistry>)> {
    let manager = Arc::new(ConnectionManager::new());
    let registry = Arc::new(TransactionRegistry::new());

    // Use default ClickHouse connection (localhost:8123, no password)
    let conn_url = "clickhouse://localhost:8123/default";
    let config = match ConnectionConfig::new(
        "test-clickhouse",
        conn_url,
        true,  // writable
        false,
        None,
        PoolOptions::default(),
    ) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to create ClickHouse config: {}", e);
            return None;
        }
    };

    match manager.connect(config).await {
        Ok(_) => Some((manager, registry)),
        Err(e) => {
            eprintln!("Failed to connect to ClickHouse: {}", e);
            None
        }
    }
}

/// Create a ClickHouse read-only test connection
async fn setup_clickhouse_readonly() -> Option<(Arc<ConnectionManager>, Arc<TransactionRegistry>)> {
    let manager = Arc::new(ConnectionManager::new());
    let registry = Arc::new(TransactionRegistry::new());

    // Use default ClickHouse connection (localhost:8123, no password)
    let conn_url = "clickhouse://localhost:8123/default";
    let config = match ConnectionConfig::new(
        "test-clickhouse-readonly",
        conn_url,
        false,  // not writable (read-only)
        false,
        None,
        PoolOptions::default(),
    ) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to create ClickHouse readonly config: {}", e);
            return None;
        }
    };

    match manager.connect(config).await {
        Ok(_) => Some((manager, registry)),
        Err(e) => {
            eprintln!("Failed to connect to ClickHouse readonly: {}", e);
            None
        }
    }
}

#[tokio::test]
async fn test_clickhouse_connection() {
    let setup = setup_clickhouse().await;
    if setup.is_none() {
        println!("ClickHouse not available, skipping test");
        return;
    }

    let (manager, _registry) = setup.unwrap();
    
    // Verify connection is established
    let pool = manager.get_pool("test-clickhouse").await;
    assert!(pool.is_ok(), "Should get connection pool");
}

#[tokio::test]
async fn test_clickhouse_query() {
    let setup = setup_clickhouse().await;
    if setup.is_none() {
        println!("ClickHouse not available, skipping test");
        return;
    }

    let (manager, registry) = setup.unwrap();
    let query_handler = QueryToolHandler::with_defaults(manager.clone(), registry.clone(), 30, 100);

    // Test simple SELECT query
    let query_input = QueryInput {
        connection_id: "test-clickhouse".to_string(),
        sql: "SELECT 1 as value, 'test' as name".to_string(),
        params: vec![],
        limit: None,
        timeout_secs: None,
        decode_binary: true,
        transaction_id: None,
        database: None,
        format: OutputFormat::Json,
    };

    let result = query_handler.query(query_input).await;
    if let Err(e) = &result {
        println!("Query error: {}", e);
    }
    assert!(result.is_ok(), "SELECT query should succeed");
    
    let query_result = result.unwrap();
    assert_eq!(query_result.rows.len(), 1);
}

#[tokio::test]
async fn test_clickhouse_query_with_params() {
    let setup = setup_clickhouse().await;
    if setup.is_none() {
        println!("ClickHouse not available, skipping test");
        return;
    }

    let (manager, registry) = setup.unwrap();
    let query_handler = QueryToolHandler::with_defaults(manager.clone(), registry.clone(), 30, 100);

    // Test query with parameters
    let query_input = QueryInput {
        connection_id: "test-clickhouse".to_string(),
        sql: "SELECT ? as num, ? as text".to_string(),
        params: vec![
            QueryParamInput::Int(42),
            QueryParamInput::String("hello".to_string()),
        ],
        limit: None,
        timeout_secs: None,
        decode_binary: true,
        transaction_id: None,
        database: None,
        format: OutputFormat::Json,
    };

    let result = query_handler.query(query_input).await;
    assert!(result.is_ok(), "Query with params should succeed");
    
    let query_result = result.unwrap();
    assert_eq!(query_result.rows.len(), 1);
}

#[tokio::test]
async fn test_clickhouse_create_table() {
    let setup = setup_clickhouse().await;
    if setup.is_none() {
        println!("ClickHouse not available, skipping test");
        return;
    }

    let (manager, registry) = setup.unwrap();
    let write_handler = WriteToolHandler::new(manager.clone(), registry.clone());

    // Create test table
    let create_input = ExecuteInput {
        connection_id: "test-clickhouse".to_string(),
        sql: "CREATE TABLE IF NOT EXISTS test_table (id UInt64, name String, value Float64) ENGINE = MergeTree PRIMARY KEY id".to_string(),
        params: vec![],
        skip_sql_check: false,
        timeout_secs: None,
        transaction_id: None,
        database: None,
    };

    let result = write_handler.execute(create_input).await;
    assert!(result.is_ok(), "CREATE TABLE should succeed");
}

#[tokio::test]
async fn test_clickhouse_insert_and_query() {
    let setup = setup_clickhouse().await;
    if setup.is_none() {
        println!("ClickHouse not available, skipping test");
        return;
    }
    let (manager, registry) = setup.unwrap();

    let query_handler = QueryToolHandler::with_defaults(manager.clone(), registry.clone(), 30, 100);
    let write_handler = WriteToolHandler::with_defaults(manager, registry, 30);

    // Cleanup any existing test table
    let drop_input = ExecuteInput {
        connection_id: "test-clickhouse".to_string(),
        sql: "DROP TABLE IF EXISTS test_insert".to_string(),
        params: vec![],
        skip_sql_check: true,
        timeout_secs: None,
        transaction_id: None,
        database: None,
    };
    write_handler.execute(drop_input).await.unwrap();

    // Create table
    let create_input = ExecuteInput {
        connection_id: "test-clickhouse".to_string(),
        sql: "CREATE TABLE IF NOT EXISTS test_insert (id Int32, name String) ENGINE = Memory".to_string(),
        params: vec![],
        skip_sql_check: false,
        timeout_secs: None,
        transaction_id: None,
        database: None,
    };
    write_handler.execute(create_input).await.unwrap();

    // Insert data
    let insert_input = ExecuteInput {
        connection_id: "test-clickhouse".to_string(),
        sql: "INSERT INTO test_insert (id, name) VALUES (?, ?)".to_string(),
        params: vec![
            QueryParamInput::Int(1),
            QueryParamInput::String("test_data".to_string()),
        ],
        skip_sql_check: false,
        timeout_secs: None,
        transaction_id: None,
        database: None,
    };

    let result = write_handler.execute(insert_input).await;
    assert!(result.is_ok(), "INSERT should succeed");

    // Query inserted data
    let query_input = QueryInput {
        connection_id: "test-clickhouse".to_string(),
        sql: "SELECT * FROM test_insert WHERE id = ?".to_string(),
        params: vec![QueryParamInput::Int(1)],
        limit: None,
        timeout_secs: None,
        decode_binary: true,
        transaction_id: None,
        database: None,
        format: OutputFormat::Json,
    };

    let result = query_handler.query(query_input).await;
    assert!(result.is_ok(), "SELECT after INSERT should succeed");
    
    let query_result = result.unwrap();
    assert_eq!(query_result.rows.len(), 1);
    
    // Cleanup: drop the test table
    let drop_input = ExecuteInput {
        connection_id: "test-clickhouse".to_string(),
        sql: "DROP TABLE IF EXISTS test_insert".to_string(),
        params: vec![],
        skip_sql_check: true,
        timeout_secs: None,
        transaction_id: None,
        database: None,
    };
    write_handler.execute(drop_input).await.unwrap();
}

#[tokio::test]
async fn test_clickhouse_list_databases() {
    let setup = setup_clickhouse().await;
    if setup.is_none() {
        println!("ClickHouse not available, skipping test");
        return;
    }

    let (manager, _registry) = setup.unwrap();
    let schema_handler = SchemaToolHandler::new(manager.clone());

    let list_input = ListDatabasesInput {
        connection_id: "test-clickhouse".to_string(),
    };

    let result = schema_handler.list_databases(list_input).await;
    if let Err(e) = &result {
        println!("List databases error: {}", e);
    }
    assert!(result.is_ok(), "List databases should succeed");
    
    let databases = result.unwrap();
    assert!(databases.databases.len() >= 1, "Should have at least one database");
}

#[tokio::test]
async fn test_clickhouse_list_tables() {
    let setup = setup_clickhouse().await;
    if setup.is_none() {
        println!("ClickHouse not available, skipping test");
        return;
    }

    let (manager, _registry) = setup.unwrap();
    let schema_handler = SchemaToolHandler::new(manager.clone());

    let list_input = ListTablesInput {
        connection_id: "test-clickhouse".to_string(),
        include_views: false,
        database: Some("system".to_string()),
    };

    let result = schema_handler.list_tables(list_input).await;
    assert!(result.is_ok(), "List tables should succeed");
    
    let tables = result.unwrap();
    assert!(tables.tables.len() > 0, "Should have tables in system database");
}

#[tokio::test]
async fn test_clickhouse_describe_table() {
    let setup = setup_clickhouse().await;
    if setup.is_none() {
        println!("ClickHouse not available, skipping test");
        return;
    }

    let (manager, _registry) = setup.unwrap();
    let schema_handler = SchemaToolHandler::new(manager.clone());

    let describe_input = DescribeTableInput {
        connection_id: "test-clickhouse".to_string(),
        table_name: "tables".to_string(),
        database: Some("system".to_string()),
    };

    let result = schema_handler.describe_table(describe_input).await;
    assert!(result.is_ok(), "Describe table should succeed");
    
    let table_schema = result.unwrap();
    assert!(table_schema.columns.len() > 0, "Table should have columns");
}

#[tokio::test]
async fn test_clickhouse_transaction_not_supported() {
    let setup = setup_clickhouse().await;
    if setup.is_none() {
        println!("ClickHouse not available, skipping test");
        return;
    }

    let (manager, registry) = setup.unwrap();
    let tx_handler = db_mcp_server::tools::transaction::TransactionToolHandler::new(manager.clone(), registry.clone());

    let begin_input = db_mcp_server::tools::transaction::BeginTransactionInput {
        connection_id: "test-clickhouse".to_string(),
        timeout_secs: None,
        database: None,
    };

    let result = tx_handler.begin_transaction(begin_input).await;
    // ClickHouse doesn't support traditional transactions
    assert!(result.is_err(), "ClickHouse should return error for transactions");
}

#[tokio::test]
async fn test_clickhouse_readonly_query_allowed() {
    let setup = setup_clickhouse_readonly().await;
    if setup.is_none() {
        println!("ClickHouse not available, skipping test");
        return;
    }

    let (manager, registry) = setup.unwrap();
    let query_handler = QueryToolHandler::with_defaults(manager.clone(), registry.clone(), 30, 100);

    // Read-only connection should allow SELECT queries
    let query_input = QueryInput {
        connection_id: "test-clickhouse-readonly".to_string(),
        sql: "SELECT 1 as value, 'test' as name".to_string(),
        params: vec![],
        limit: None,
        timeout_secs: None,
        decode_binary: true,
        transaction_id: None,
        database: None,
        format: OutputFormat::Json,
    };

    let result = query_handler.query(query_input).await;
    assert!(result.is_ok(), "Read-only connection should allow SELECT queries");

    let query_result = result.unwrap();
    assert_eq!(query_result.rows.len(), 1);
}

#[tokio::test]
async fn test_clickhouse_readonly_insert_blocked() {
    let setup = setup_clickhouse_readonly().await;
    if setup.is_none() {
        println!("ClickHouse not available, skipping test");
        return;
    }

    let (manager, registry) = setup.unwrap();
    let write_handler = WriteToolHandler::with_defaults(manager, registry, 30);

    // Read-only connection should block INSERT
    let insert_input = ExecuteInput {
        connection_id: "test-clickhouse-readonly".to_string(),
        sql: "INSERT INTO test_readonly (id, name) VALUES (1, 'test')".to_string(),
        params: vec![],
        skip_sql_check: false,
        timeout_secs: None,
        transaction_id: None,
        database: None,
    };

    let result = write_handler.execute(insert_input).await;
    assert!(result.is_err(), "Read-only connection should block INSERT");

    let err = result.unwrap_err();
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("read-only") || err_msg.contains("readonly") || err_msg.contains("writable"),
        "Error should indicate read-only restriction: {}",
        err_msg
    );
}

#[tokio::test]
async fn test_clickhouse_readonly_create_table_blocked() {
    let setup = setup_clickhouse_readonly().await;
    if setup.is_none() {
        println!("ClickHouse not available, skipping test");
        return;
    }

    let (manager, registry) = setup.unwrap();
    let write_handler = WriteToolHandler::with_defaults(manager, registry, 30);

    // Read-only connection should block CREATE TABLE
    let create_input = ExecuteInput {
        connection_id: "test-clickhouse-readonly".to_string(),
        sql: "CREATE TABLE test_readonly (id UInt64, name String) ENGINE = Memory".to_string(),
        params: vec![],
        skip_sql_check: false,
        timeout_secs: None,
        transaction_id: None,
        database: None,
    };

    let result = write_handler.execute(create_input).await;
    assert!(result.is_err(), "Read-only connection should block CREATE TABLE");

    let err = result.unwrap_err();
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("read-only") || err_msg.contains("readonly") || err_msg.contains("writable"),
        "Error should indicate read-only restriction: {}",
        err_msg
    );
}

#[tokio::test]
async fn test_clickhouse_readonly_schema_allowed() {
    let setup = setup_clickhouse_readonly().await;
    if setup.is_none() {
        println!("ClickHouse not available, skipping test");
        return;
    }

    let (manager, _registry) = setup.unwrap();
    let schema_handler = SchemaToolHandler::new(manager.clone());

    // Read-only connection should allow listing databases
    let list_input = ListDatabasesInput {
        connection_id: "test-clickhouse-readonly".to_string(),
    };

    let result = schema_handler.list_databases(list_input).await;
    assert!(result.is_ok(), "Read-only connection should allow listing databases");

    // Read-only connection should allow listing tables
    let list_tables_input = ListTablesInput {
        connection_id: "test-clickhouse-readonly".to_string(),
        include_views: false,
        database: Some("system".to_string()),
    };

    let result = schema_handler.list_tables(list_tables_input).await;
    assert!(result.is_ok(), "Read-only connection should allow listing tables");

    // Read-only connection should allow describing tables
    let describe_input = DescribeTableInput {
        connection_id: "test-clickhouse-readonly".to_string(),
        table_name: "tables".to_string(),
        database: Some("system".to_string()),
    };

    let result = schema_handler.describe_table(describe_input).await;
    assert!(result.is_ok(), "Read-only connection should allow describing tables");
}
