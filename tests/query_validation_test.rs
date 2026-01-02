//! Integration tests for query validation.
//!
//! These tests verify that the query tool correctly rejects write operations
//! and allows read-only queries.

use db_mcp_server::error::DbError;
use db_mcp_server::tools::sql_validator::{
    SqlStatementType, get_statement_type, validate_readonly,
};

/// Test that INSERT is rejected with Permission error.
#[test]
fn test_query_rejects_insert() {
    let result = validate_readonly("INSERT INTO users (name) VALUES ('test')");
    assert!(result.is_err(), "INSERT should be rejected");

    let err = result.unwrap_err();
    assert!(
        matches!(err, DbError::Permission { .. }),
        "Should be Permission error, got: {:?}",
        err
    );
}

/// Test that UPDATE is rejected with Permission error.
#[test]
fn test_query_rejects_update() {
    let result = validate_readonly("UPDATE users SET name = 'changed' WHERE id = 1");
    assert!(result.is_err(), "UPDATE should be rejected");

    let err = result.unwrap_err();
    assert!(matches!(err, DbError::Permission { .. }));
}

/// Test that DELETE is rejected with Permission error.
#[test]
fn test_query_rejects_delete() {
    let result = validate_readonly("DELETE FROM users WHERE id = 1");
    assert!(result.is_err(), "DELETE should be rejected");

    let err = result.unwrap_err();
    assert!(matches!(err, DbError::Permission { .. }));
}

/// Test that CREATE TABLE is rejected with Permission error.
#[test]
fn test_query_rejects_create() {
    let result = validate_readonly("CREATE TABLE test (id INT PRIMARY KEY)");
    assert!(result.is_err(), "CREATE TABLE should be rejected");

    let err = result.unwrap_err();
    assert!(matches!(err, DbError::Permission { .. }));
}

/// Test that DROP TABLE is rejected with Permission error.
#[test]
fn test_query_rejects_drop() {
    let result = validate_readonly("DROP TABLE users");
    assert!(result.is_err(), "DROP TABLE should be rejected");

    let err = result.unwrap_err();
    assert!(matches!(err, DbError::Permission { .. }));
}

/// Test that SELECT is allowed.
#[test]
fn test_query_allows_select() {
    let result = validate_readonly("SELECT * FROM users WHERE id = 1");
    assert!(result.is_ok(), "SELECT should be allowed");
}

/// Test that SELECT with complex joins is allowed.
#[test]
fn test_query_allows_complex_select() {
    let sql = r#"
        SELECT u.name, o.total
        FROM users u
        JOIN orders o ON u.id = o.user_id
        WHERE o.created_at > '2024-01-01'
        ORDER BY o.total DESC
        LIMIT 10
    "#;
    let result = validate_readonly(sql);
    assert!(result.is_ok(), "Complex SELECT should be allowed");
}

/// Test that WITH (CTE) is allowed.
#[test]
fn test_query_allows_cte() {
    let sql = r#"
        WITH active_users AS (
            SELECT id, name FROM users WHERE active = true
        )
        SELECT * FROM active_users
    "#;
    let result = validate_readonly(sql);
    assert!(result.is_ok(), "CTE should be allowed");
}

/// Test that EXPLAIN SELECT is allowed.
#[test]
fn test_query_allows_explain_select() {
    let result = validate_readonly("EXPLAIN SELECT * FROM users");
    assert!(result.is_ok(), "EXPLAIN SELECT should be allowed");
}

/// Test that EXPLAIN INSERT is rejected.
#[test]
fn test_query_rejects_explain_insert() {
    let result = validate_readonly("EXPLAIN INSERT INTO users (name) VALUES ('test')");
    assert!(result.is_err(), "EXPLAIN INSERT should be rejected");
}

/// Test that SHOW TABLES is allowed.
#[test]
fn test_query_allows_show() {
    let result = validate_readonly("SHOW TABLES");
    assert!(result.is_ok(), "SHOW should be allowed");
}

/// Test that DESCRIBE is allowed.
#[test]
fn test_query_allows_describe() {
    let result = validate_readonly("DESCRIBE users");
    assert!(result.is_ok(), "DESCRIBE should be allowed");
}

/// Test that case-insensitive detection works.
#[test]
fn test_case_insensitive_detection() {
    // All variations should be detected as DmlWrite (using complete valid SQL)
    assert_eq!(
        get_statement_type("insert into users values (1)"),
        SqlStatementType::DmlWrite
    );
    assert_eq!(
        get_statement_type("INSERT INTO users VALUES (1)"),
        SqlStatementType::DmlWrite
    );
    assert_eq!(
        get_statement_type("Insert Into Users Values (1)"),
        SqlStatementType::DmlWrite
    );
    assert_eq!(
        get_statement_type("iNsErT iNtO uSeRs VaLuEs (1)"),
        SqlStatementType::DmlWrite
    );

    // All variations should be detected as Select
    assert_eq!(
        get_statement_type("select * from users"),
        SqlStatementType::Select
    );
    assert_eq!(
        get_statement_type("SELECT * FROM users"),
        SqlStatementType::Select
    );
    assert_eq!(
        get_statement_type("Select * From Users"),
        SqlStatementType::Select
    );
}

/// Test that SQL with leading comments is correctly parsed.
#[test]
fn test_sql_with_comments() {
    // Single-line comment before SELECT
    let result = validate_readonly("-- This is a comment\nSELECT * FROM users");
    assert!(
        result.is_ok(),
        "SELECT with single-line comment should be allowed"
    );

    // Multi-line comment before SELECT
    let result = validate_readonly("/* This is\na comment */ SELECT * FROM users");
    assert!(
        result.is_ok(),
        "SELECT with multi-line comment should be allowed"
    );

    // Comment before INSERT should still be rejected
    let result = validate_readonly("-- comment\nINSERT INTO users VALUES (1)");
    assert!(result.is_err(), "INSERT with comment should be rejected");
}

/// Test that BEGIN transaction is rejected.
#[test]
fn test_query_rejects_transaction() {
    assert!(
        validate_readonly("BEGIN").is_err(),
        "BEGIN should be rejected"
    );
    assert!(
        validate_readonly("COMMIT").is_err(),
        "COMMIT should be rejected"
    );
    assert!(
        validate_readonly("ROLLBACK").is_err(),
        "ROLLBACK should be rejected"
    );
}

/// Test that CALL is rejected.
#[test]
fn test_query_rejects_procedure_call() {
    assert!(
        validate_readonly("CALL my_procedure()").is_err(),
        "CALL should be rejected"
    );
}

/// Test error message contains helpful guidance.
#[test]
fn test_error_message_guidance() {
    let result = validate_readonly("INSERT INTO users VALUES (1)");
    let err = result.unwrap_err();
    let msg = err.to_string();

    assert!(
        msg.contains("execute") || msg.contains("write") || msg.contains("INSERT"),
        "Error message should provide guidance: {}",
        msg
    );
}
