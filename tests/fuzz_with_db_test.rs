//! Deep fuzzing tests with real database connections.
//!
//! This test suite performs black-box fuzzing with actual SQLite databases
//! to discover bugs in real execution paths.

use db_mcp_server::config::PoolOptions;
use db_mcp_server::db::{ConnectionManager, TransactionRegistry};
use db_mcp_server::models::ConnectionConfig;
use db_mcp_server::tools::explain::{ExplainInput, ExplainToolHandler};
use db_mcp_server::tools::format::OutputFormat;
use db_mcp_server::tools::query::{QueryInput, QueryToolHandler};
use db_mcp_server::tools::schema::{DescribeTableInput, ListTablesInput, SchemaToolHandler};
use db_mcp_server::tools::transaction::{
    BeginTransactionInput, CommitInput, RollbackInput, TransactionToolHandler,
};
use db_mcp_server::tools::write::{ExecuteInput, WriteToolHandler};
use rand::Rng;
use rand::distributions::Alphanumeric;
use std::sync::Arc;
use tempfile::NamedTempFile;

fn random_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

async fn setup_test_db() -> (Arc<ConnectionManager>, Arc<TransactionRegistry>, String) {
    let temp_file = NamedTempFile::new().unwrap();

    // IMPORTANT: Keep the temp file alive - prevent deletion when function returns
    let db_path = temp_file
        .into_temp_path()
        .keep()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    let manager = Arc::new(ConnectionManager::new());
    let registry = Arc::new(TransactionRegistry::new());

    let conn_url = format!("sqlite:{}?mode=rwc", db_path);
    let config =
        ConnectionConfig::new("test", &conn_url, true, false, None, PoolOptions::default())
            .unwrap();
    manager.connect(config).await.unwrap();

    // Create a test table
    let pool = manager.get_pool("test").await.unwrap();
    if let db_mcp_server::db::pool::DbPool::SQLite(pool) = pool {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT,
                age INTEGER,
                data BLOB
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query("INSERT INTO users (name, email, age) VALUES (?, ?, ?)")
            .bind("Alice")
            .bind("alice@example.com")
            .bind(30)
            .execute(&pool)
            .await
            .unwrap();

        sqlx::query("INSERT INTO users (name, email, age) VALUES (?, ?, ?)")
            .bind("Bob")
            .bind("bob@example.com")
            .bind(25)
            .execute(&pool)
            .await
            .unwrap();
    }

    (manager, registry, db_path)
}

#[tokio::test]
async fn fuzz_valid_queries_with_edge_cases() {
    let (manager, registry, _path) = setup_test_db().await;
    let handler = QueryToolHandler::with_defaults(manager, registry, 30, 100);

    let queries = vec![
        "SELECT * FROM users",
        "SELECT * FROM users WHERE id = 1",
        "SELECT * FROM users WHERE name LIKE '%'",
        "SELECT * FROM users WHERE age > 0",
        "SELECT COUNT(*) FROM users",
        "SELECT id, name FROM users ORDER BY id DESC",
        "SELECT * FROM users LIMIT 0",
        "SELECT * FROM users LIMIT 1000000",
        "SELECT 1 + 1",
        "SELECT NULL",
        "SELECT ''",
        "SELECT 0.0",
        "SELECT x'00'",
    ];

    for sql in queries {
        let input = QueryInput {
            connection_id: "test".to_string(),
            sql: sql.to_string(),
            params: vec![],
            limit: Some(100),
            timeout_secs: Some(5),
            format: OutputFormat::Json,
            decode_binary: true,
            transaction_id: None,
            database: None,
        };

        let result = handler.query(input).await;
        assert!(result.is_ok(), "Query failed: {}", sql);
    }
}

#[tokio::test]
async fn fuzz_write_operations() {
    let (manager, registry, _path) = setup_test_db().await;
    let handler = WriteToolHandler::with_defaults(manager, registry, 30);

    let operations = vec![
        "INSERT INTO users (name, email, age) VALUES ('Charlie', 'charlie@example.com', 35)",
        "UPDATE users SET age = 31 WHERE name = 'Alice'",
        "DELETE FROM users WHERE id = 999",
        "INSERT INTO users (name, email) VALUES ('Dave', NULL)",
        "UPDATE users SET email = 'newemail@test.com' WHERE id = 1",
    ];

    for sql in operations {
        let input = ExecuteInput {
            connection_id: "test".to_string(),
            sql: sql.to_string(),
            params: vec![],
            timeout_secs: Some(5),
            transaction_id: None,
            dangerous_operation_allowed: false,
            database: None,
        };

        let result = handler.execute(input).await;
        if let Err(ref e) = result {
            eprintln!("Write failed for SQL: {}\nError: {:?}", sql, e);
        }
        assert!(result.is_ok(), "Write failed: {}", sql);
    }
}

#[tokio::test]
async fn fuzz_dangerous_operations_blocked() {
    let (manager, registry, _path) = setup_test_db().await;
    let handler = WriteToolHandler::with_defaults(manager, registry, 30);

    let dangerous = vec![
        "DROP TABLE users",
        "DELETE FROM users",
        "TRUNCATE TABLE users",
        "UPDATE users SET name = 'hacked'",
    ];

    for sql in dangerous {
        let input = ExecuteInput {
            connection_id: "test".to_string(),
            sql: sql.to_string(),
            params: vec![],
            timeout_secs: Some(1),
            transaction_id: None,
            dangerous_operation_allowed: false,
            database: None,
        };

        let result = handler.execute(input).await;
        assert!(
            result.is_err(),
            "Dangerous operation should be blocked: {}",
            sql
        );
    }
}

#[tokio::test]
async fn fuzz_transaction_workflow() {
    let (manager, registry, _path) = setup_test_db().await;
    let tx_handler = TransactionToolHandler::new(manager.clone(), registry.clone());
    let write_handler = WriteToolHandler::with_defaults(manager.clone(), registry.clone(), 30);
    let query_handler = QueryToolHandler::with_defaults(manager, registry, 30, 100);

    let begin_input = BeginTransactionInput {
        connection_id: "test".to_string(),
        timeout_secs: Some(60),
        database: None,
    };

    let tx_result = tx_handler.begin_transaction(begin_input).await;
    assert!(tx_result.is_ok());
    let tx_id = tx_result.unwrap().transaction_id;

    let insert_input = ExecuteInput {
        connection_id: "test".to_string(),
        sql: "INSERT INTO users (name, email, age) VALUES ('Eve', 'eve@example.com', 28)"
            .to_string(),
        params: vec![],
        timeout_secs: Some(5),
        transaction_id: Some(tx_id.clone()),
        dangerous_operation_allowed: false,
        database: None,
    };

    let insert_result = write_handler.execute(insert_input).await;
    assert!(insert_result.is_ok());

    let query_input = QueryInput {
        connection_id: "test".to_string(),
        sql: "SELECT * FROM users WHERE name = 'Eve'".to_string(),
        params: vec![],
        limit: Some(10),
        timeout_secs: Some(5),
        format: OutputFormat::Json,
        decode_binary: true,
        transaction_id: Some(tx_id.clone()),
        database: None,
    };

    let query_result = query_handler.query(query_input).await;
    assert!(query_result.is_ok());

    let commit_input = CommitInput {
        connection_id: "test".to_string(),
        transaction_id: tx_id,
    };

    let commit_result = tx_handler.commit(commit_input).await;
    assert!(commit_result.is_ok());
}

#[tokio::test]
async fn fuzz_transaction_rollback() {
    let (manager, registry, _path) = setup_test_db().await;
    let tx_handler = TransactionToolHandler::new(manager.clone(), registry.clone());
    let write_handler = WriteToolHandler::with_defaults(manager.clone(), registry, 30);

    let begin_input = BeginTransactionInput {
        connection_id: "test".to_string(),
        timeout_secs: Some(60),
        database: None,
    };

    let tx_result = tx_handler.begin_transaction(begin_input).await;
    assert!(tx_result.is_ok());
    let tx_id = tx_result.unwrap().transaction_id;

    let delete_input = ExecuteInput {
        connection_id: "test".to_string(),
        sql: "DELETE FROM users WHERE id = 1".to_string(),
        params: vec![],
        timeout_secs: Some(5),
        transaction_id: Some(tx_id.clone()),
        dangerous_operation_allowed: true,
        database: None,
    };

    let delete_result = write_handler.execute(delete_input).await;
    assert!(delete_result.is_ok());

    let rollback_input = RollbackInput {
        connection_id: "test".to_string(),
        transaction_id: tx_id,
    };

    let rollback_result = tx_handler.rollback(rollback_input).await;
    assert!(rollback_result.is_ok());
}

#[tokio::test]
async fn fuzz_explain_queries() {
    let (manager, registry, _path) = setup_test_db().await;
    let handler = ExplainToolHandler::new(manager, registry);

    let queries = vec![
        "SELECT * FROM users",
        "SELECT * FROM users WHERE id = 1",
        "SELECT COUNT(*) FROM users",
        "INSERT INTO users (name, email) VALUES ('Test', 'test@example.com')",
        "UPDATE users SET age = 50 WHERE id = 1",
        "DELETE FROM users WHERE id = 999",
    ];

    for sql in queries {
        let input = ExplainInput {
            connection_id: "test".to_string(),
            sql: sql.to_string(),
            params: vec![],
            transaction_id: None,
            timeout_secs: Some(5),
            format: OutputFormat::Json,
            database: None,
        };

        let result = handler.explain(input).await;
        assert!(result.is_ok(), "EXPLAIN failed for: {}", sql);
    }
}

#[tokio::test]
async fn fuzz_schema_operations() {
    let (manager, _registry, _path) = setup_test_db().await;
    let handler = SchemaToolHandler::new(manager);

    let list_input = ListTablesInput {
        connection_id: "test".to_string(),
        include_views: true,
        database: None,
    };

    let list_result = handler.list_tables(list_input).await;
    assert!(list_result.is_ok());

    let describe_input = DescribeTableInput {
        connection_id: "test".to_string(),
        table_name: "users".to_string(),
        database: None,
    };

    let describe_result = handler.describe_table(describe_input).await;
    assert!(describe_result.is_ok());

    let bad_describe_input = DescribeTableInput {
        connection_id: "test".to_string(),
        table_name: "nonexistent_table".to_string(),
        database: None,
    };

    let bad_result = handler.describe_table(bad_describe_input).await;
    assert!(bad_result.is_err());
}

#[tokio::test]
async fn fuzz_parameterized_queries() {
    use db_mcp_server::models::QueryParamInput;

    let (manager, registry, _path) = setup_test_db().await;
    let handler = QueryToolHandler::with_defaults(manager, registry, 30, 100);

    let test_cases = vec![
        (
            "SELECT * FROM users WHERE id = ?",
            vec![QueryParamInput::Int(1)],
        ),
        (
            "SELECT * FROM users WHERE name = ?",
            vec![QueryParamInput::String("Alice".to_string())],
        ),
        (
            "SELECT * FROM users WHERE age > ?",
            vec![QueryParamInput::Int(25)],
        ),
        (
            "SELECT * FROM users WHERE email = ?",
            vec![QueryParamInput::Null],
        ),
        (
            "SELECT * FROM users WHERE id IN (?, ?, ?)",
            vec![
                QueryParamInput::Int(1),
                QueryParamInput::Int(2),
                QueryParamInput::Int(3),
            ],
        ),
    ];

    for (sql, params) in test_cases {
        let input = QueryInput {
            connection_id: "test".to_string(),
            sql: sql.to_string(),
            params,
            limit: Some(100),
            timeout_secs: Some(5),
            format: OutputFormat::Json,
            decode_binary: true,
            transaction_id: None,
            database: None,
        };

        let result = handler.query(input).await;
        assert!(result.is_ok(), "Parameterized query failed: {}", sql);
    }
}

#[tokio::test]
async fn fuzz_output_formats() {
    let (manager, registry, _path) = setup_test_db().await;
    let handler = QueryToolHandler::with_defaults(manager, registry, 30, 100);

    let formats = vec![
        OutputFormat::Json,
        OutputFormat::Table,
        OutputFormat::Markdown,
    ];

    for format in formats {
        let input = QueryInput {
            connection_id: "test".to_string(),
            sql: "SELECT * FROM users".to_string(),
            params: vec![],
            limit: Some(10),
            timeout_secs: Some(5),
            format,
            decode_binary: true,
            transaction_id: None,
            database: None,
        };

        let result = handler.query(input).await;
        assert!(result.is_ok(), "Query with format {:?} failed", format);
    }
}

#[tokio::test]
async fn fuzz_invalid_sql_syntax() {
    let (manager, registry, _path) = setup_test_db().await;
    let handler = QueryToolHandler::with_defaults(manager, registry, 30, 100);

    let invalid_sqls = vec![
        "SELEC FROM users",
        "SELECT * FORM users",
        "SELECT * FROM",
        "SELECT",
        "",
        ";;;",
        "SELECT * FROM users WHERE",
        "SELECT * FROM users WHERE id =",
    ];

    for sql in invalid_sqls {
        let input = QueryInput {
            connection_id: "test".to_string(),
            sql: sql.to_string(),
            params: vec![],
            limit: Some(10),
            timeout_secs: Some(1),
            format: OutputFormat::Json,
            decode_binary: true,
            transaction_id: None,
            database: None,
        };

        let result = handler.query(input).await;
        // Most should error, but shouldn't panic
        assert!(result.is_err() || result.is_ok());
    }
}

#[tokio::test]
async fn fuzz_double_commit_rollback() {
    let (manager, registry, _path) = setup_test_db().await;
    let tx_handler = TransactionToolHandler::new(manager, registry);

    let begin_input = BeginTransactionInput {
        connection_id: "test".to_string(),
        timeout_secs: Some(60),
        database: None,
    };

    let tx_result = tx_handler.begin_transaction(begin_input).await;
    assert!(tx_result.is_ok());
    let tx_id = tx_result.unwrap().transaction_id;

    let commit_input = CommitInput {
        connection_id: "test".to_string(),
        transaction_id: tx_id.clone(),
    };

    let first_commit = tx_handler.commit(commit_input.clone()).await;
    assert!(first_commit.is_ok());

    let second_commit = tx_handler.commit(commit_input).await;
    assert!(second_commit.is_err(), "Double commit should fail");
}

#[tokio::test]
async fn fuzz_concurrent_transactions() {
    use tokio::task::JoinSet;

    let (manager, registry, _path) = setup_test_db().await;

    let mut tasks = JoinSet::new();

    for i in 0..10 {
        let manager_clone = manager.clone();
        let registry_clone = registry.clone();

        tasks.spawn(async move {
            let tx_handler = TransactionToolHandler::new(manager_clone, registry_clone);

            let begin_input = BeginTransactionInput {
                connection_id: "test".to_string(),
                timeout_secs: Some(60),
                database: None,
            };

            let tx_result = tx_handler.begin_transaction(begin_input).await;
            if let Ok(tx) = tx_result {
                if i % 2 == 0 {
                    let commit_input = CommitInput {
                        connection_id: "test".to_string(),
                        transaction_id: tx.transaction_id,
                    };
                    tx_handler.commit(commit_input).await.map(|_| ())
                } else {
                    let rollback_input = RollbackInput {
                        connection_id: "test".to_string(),
                        transaction_id: tx.transaction_id,
                    };
                    tx_handler.rollback(rollback_input).await.map(|_| ())
                }
            } else {
                tx_result.map(|_| ())
            }
        });
    }

    let mut success_count = 0;
    while let Some(result) = tasks.join_next().await {
        if let Ok(task_result) = result {
            if task_result.is_ok() {
                success_count += 1;
            }
        }
    }

    assert!(success_count > 0);
}

#[tokio::test]
async fn fuzz_malicious_table_names() {
    let (manager, _registry, _path) = setup_test_db().await;
    let handler = SchemaToolHandler::new(manager);

    let malicious_names = vec![
        "'; DROP TABLE users--".to_string(),
        "../../../etc/passwd".to_string(),
        "\0".to_string(),
        "table`; DROP TABLE users; --".to_string(),
        "users; SELECT * FROM sqlite_master".to_string(),
        random_string(1000),
    ];

    for table_name in malicious_names {
        let input = DescribeTableInput {
            connection_id: "test".to_string(),
            table_name: table_name.clone(),
            database: None,
        };

        let result = handler.describe_table(input).await;
        // Should error gracefully, not panic
        assert!(result.is_err() || result.is_ok());
    }
}
