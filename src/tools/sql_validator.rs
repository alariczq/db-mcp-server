//! SQL statement validation for read-only enforcement.
//!
//! This module provides validation logic to ensure that the `query` tool
//! only executes read-only SQL statements (SELECT, SHOW, DESCRIBE, etc.).
//! Write operations (INSERT, UPDATE, DELETE, DDL, etc.) are blocked with
//! actionable error messages guiding users to the appropriate tool.
//!
//! Uses [sqlparser](https://docs.rs/sqlparser/) for accurate SQL parsing,
//! ensuring that no write operations can bypass validation through formatting
//! tricks or SQL dialect variations.

use crate::error::{DbError, DbResult};
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Type of SQL statement detected by the validator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlStatementType {
    /// SELECT and other read-only queries (SELECT, SHOW, DESCRIBE, VALUES)
    Select,
    /// INSERT, UPDATE, DELETE, MERGE, REPLACE, UPSERT
    DmlWrite,
    /// CREATE, DROP, ALTER, TRUNCATE, RENAME
    Ddl,
    /// BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE, START
    Transaction,
    /// CALL, EXECUTE, EXEC (stored procedures)
    ProcedureCall,
    /// GRANT, REVOKE, SET, LOCK, UNLOCK, VACUUM
    Administrative,
    /// Unknown or unparseable statement
    Unknown,
}

/// Error messages for each statement type category.
mod error_messages {
    pub const DML_WRITE: &str =
        "Write operations not allowed in query. Use execute for INSERT/UPDATE/DELETE.";
    pub const DDL: &str =
        "DDL operations not allowed in query. Use execute for schema modifications.";
    pub const TRANSACTION: &str =
        "Transaction control not allowed in query. Use begin_transaction, commit, or rollback.";
    pub const PROCEDURE: &str =
        "Procedure calls not allowed in query. Use execute for stored procedures.";
    pub const ADMINISTRATIVE: &str = "Administrative operations not allowed in query.";
    pub const UNKNOWN: &str =
        "Unrecognized SQL statement. Only SELECT queries are allowed in query.";
    pub const PARSE_ERROR: &str = "Failed to parse SQL statement.";
}

/// Validate SQL for read-only execution in the query tool.
///
/// Returns `Ok(())` if the statement is allowed (SELECT, SHOW, DESCRIBE, etc.),
/// or `Err(DbError::Permission)` if the statement is a write operation.
///
/// Uses sqlparser for accurate AST-based validation, preventing any bypass
/// through formatting tricks or SQL dialect variations.
///
/// # Examples
///
/// ```
/// use db_mcp_server::tools::sql_validator::validate_readonly;
///
/// // SELECT is allowed
/// assert!(validate_readonly("SELECT * FROM users").is_ok());
///
/// // INSERT is blocked
/// assert!(validate_readonly("INSERT INTO users VALUES (1)").is_err());
/// ```
pub fn validate_readonly(sql: &str) -> DbResult<()> {
    let dialect = GenericDialect {};

    let statements = Parser::parse_sql(&dialect, sql).map_err(|e| {
        DbError::invalid_input(format!("{} Error: {}", error_messages::PARSE_ERROR, e))
    })?;

    if statements.is_empty() {
        return Err(DbError::invalid_input("Empty SQL statement"));
    }

    for stmt in statements {
        validate_statement(&stmt)?;
    }

    Ok(())
}

/// Validate a single parsed statement.
fn validate_statement(stmt: &Statement) -> DbResult<()> {
    let (stmt_type, operation_name) = classify_statement(stmt);

    match stmt_type {
        SqlStatementType::Select => Ok(()),
        SqlStatementType::DmlWrite => Err(DbError::permission(
            operation_name,
            error_messages::DML_WRITE,
        )),
        SqlStatementType::Ddl => Err(DbError::permission(operation_name, error_messages::DDL)),
        SqlStatementType::Transaction => Err(DbError::permission(
            operation_name,
            error_messages::TRANSACTION,
        )),
        SqlStatementType::ProcedureCall => Err(DbError::permission(
            operation_name,
            error_messages::PROCEDURE,
        )),
        SqlStatementType::Administrative => Err(DbError::permission(
            operation_name,
            error_messages::ADMINISTRATIVE,
        )),
        SqlStatementType::Unknown => {
            Err(DbError::permission(operation_name, error_messages::UNKNOWN))
        }
    }
}

/// Classify a parsed statement into a statement type.
fn classify_statement(stmt: &Statement) -> (SqlStatementType, &'static str) {
    match stmt {
        // =====================================================================
        // Read-only operations - ALLOWED
        // =====================================================================
        Statement::Query(_) => (SqlStatementType::Select, "SELECT"),
        Statement::ShowTables { .. } => (SqlStatementType::Select, "SHOW TABLES"),
        Statement::ShowColumns { .. } => (SqlStatementType::Select, "SHOW COLUMNS"),
        Statement::ShowDatabases { .. } => (SqlStatementType::Select, "SHOW DATABASES"),
        Statement::ShowSchemas { .. } => (SqlStatementType::Select, "SHOW SCHEMAS"),
        Statement::ShowCreate { .. } => (SqlStatementType::Select, "SHOW CREATE"),
        Statement::ShowFunctions { .. } => (SqlStatementType::Select, "SHOW FUNCTIONS"),
        Statement::ShowVariable { .. } => (SqlStatementType::Select, "SHOW VARIABLE"),
        Statement::ShowVariables { .. } => (SqlStatementType::Select, "SHOW VARIABLES"),
        Statement::ShowStatus { .. } => (SqlStatementType::Select, "SHOW STATUS"),
        Statement::ShowCollation { .. } => (SqlStatementType::Select, "SHOW COLLATION"),
        Statement::ExplainTable { .. } => (SqlStatementType::Select, "EXPLAIN TABLE"),

        // EXPLAIN needs special handling - check underlying statement
        Statement::Explain { statement, .. } => {
            let (inner_type, inner_name) = classify_statement(statement);
            if inner_type == SqlStatementType::Select {
                (SqlStatementType::Select, "EXPLAIN")
            } else {
                // EXPLAIN on write operation - block it
                (inner_type, inner_name)
            }
        }

        // =====================================================================
        // DML Write operations - BLOCKED
        // =====================================================================
        Statement::Insert(_) => (SqlStatementType::DmlWrite, "INSERT"),
        Statement::Update { .. } => (SqlStatementType::DmlWrite, "UPDATE"),
        Statement::Delete(_) => (SqlStatementType::DmlWrite, "DELETE"),
        Statement::Merge { .. } => (SqlStatementType::DmlWrite, "MERGE"),
        Statement::Copy { .. } => (SqlStatementType::DmlWrite, "COPY"),
        Statement::CopyIntoSnowflake { .. } => (SqlStatementType::DmlWrite, "COPY INTO"),

        // =====================================================================
        // DDL operations - BLOCKED
        // =====================================================================
        Statement::CreateTable { .. } => (SqlStatementType::Ddl, "CREATE TABLE"),
        Statement::CreateView { .. } => (SqlStatementType::Ddl, "CREATE VIEW"),
        Statement::CreateIndex(_) => (SqlStatementType::Ddl, "CREATE INDEX"),
        Statement::CreateSchema { .. } => (SqlStatementType::Ddl, "CREATE SCHEMA"),
        Statement::CreateDatabase { .. } => (SqlStatementType::Ddl, "CREATE DATABASE"),
        Statement::CreateSequence { .. } => (SqlStatementType::Ddl, "CREATE SEQUENCE"),
        Statement::CreateType { .. } => (SqlStatementType::Ddl, "CREATE TYPE"),
        Statement::CreateFunction { .. } => (SqlStatementType::Ddl, "CREATE FUNCTION"),
        Statement::CreateProcedure { .. } => (SqlStatementType::Ddl, "CREATE PROCEDURE"),
        Statement::CreateTrigger { .. } => (SqlStatementType::Ddl, "CREATE TRIGGER"),
        Statement::CreateRole { .. } => (SqlStatementType::Ddl, "CREATE ROLE"),
        Statement::CreateSecret { .. } => (SqlStatementType::Ddl, "CREATE SECRET"),
        Statement::CreateStage { .. } => (SqlStatementType::Ddl, "CREATE STAGE"),
        Statement::CreateVirtualTable { .. } => (SqlStatementType::Ddl, "CREATE VIRTUAL TABLE"),
        Statement::CreateExtension { .. } => (SqlStatementType::Ddl, "CREATE EXTENSION"),
        Statement::CreatePolicy { .. } => (SqlStatementType::Ddl, "CREATE POLICY"),
        Statement::CreateConnector { .. } => (SqlStatementType::Ddl, "CREATE CONNECTOR"),

        Statement::AlterTable { .. } => (SqlStatementType::Ddl, "ALTER TABLE"),
        Statement::AlterView { .. } => (SqlStatementType::Ddl, "ALTER VIEW"),
        Statement::AlterIndex { .. } => (SqlStatementType::Ddl, "ALTER INDEX"),
        Statement::AlterSchema { .. } => (SqlStatementType::Ddl, "ALTER SCHEMA"),
        Statement::AlterRole { .. } => (SqlStatementType::Ddl, "ALTER ROLE"),
        Statement::AlterSession { .. } => (SqlStatementType::Ddl, "ALTER SESSION"),
        Statement::AlterPolicy { .. } => (SqlStatementType::Ddl, "ALTER POLICY"),
        Statement::AlterType { .. } => (SqlStatementType::Ddl, "ALTER TYPE"),
        Statement::AlterConnector { .. } => (SqlStatementType::Ddl, "ALTER CONNECTOR"),

        Statement::Drop { .. } => (SqlStatementType::Ddl, "DROP"),
        Statement::DropFunction { .. } => (SqlStatementType::Ddl, "DROP FUNCTION"),
        Statement::DropProcedure { .. } => (SqlStatementType::Ddl, "DROP PROCEDURE"),
        Statement::DropTrigger { .. } => (SqlStatementType::Ddl, "DROP TRIGGER"),
        Statement::DropSecret { .. } => (SqlStatementType::Ddl, "DROP SECRET"),
        Statement::DropPolicy { .. } => (SqlStatementType::Ddl, "DROP POLICY"),
        Statement::DropConnector { .. } => (SqlStatementType::Ddl, "DROP CONNECTOR"),

        Statement::Truncate { .. } => (SqlStatementType::Ddl, "TRUNCATE"),
        Statement::Comment { .. } => (SqlStatementType::Ddl, "COMMENT"),

        // =====================================================================
        // Transaction control - BLOCKED (use transaction tools)
        // =====================================================================
        Statement::StartTransaction { .. } => (SqlStatementType::Transaction, "BEGIN"),
        Statement::Commit { .. } => (SqlStatementType::Transaction, "COMMIT"),
        Statement::Rollback { .. } => (SqlStatementType::Transaction, "ROLLBACK"),
        Statement::Savepoint { .. } => (SqlStatementType::Transaction, "SAVEPOINT"),
        Statement::ReleaseSavepoint { .. } => (SqlStatementType::Transaction, "RELEASE SAVEPOINT"),

        // =====================================================================
        // Procedure/Function calls - BLOCKED (cannot verify behavior)
        // =====================================================================
        Statement::Call { .. } => (SqlStatementType::ProcedureCall, "CALL"),
        Statement::Execute { .. } => (SqlStatementType::ProcedureCall, "EXECUTE"),
        Statement::Prepare { .. } => (SqlStatementType::ProcedureCall, "PREPARE"),
        Statement::Deallocate { .. } => (SqlStatementType::ProcedureCall, "DEALLOCATE"),

        // =====================================================================
        // Administrative operations - BLOCKED
        // =====================================================================
        Statement::Grant { .. } => (SqlStatementType::Administrative, "GRANT"),
        Statement::Revoke { .. } => (SqlStatementType::Administrative, "REVOKE"),
        Statement::Deny { .. } => (SqlStatementType::Administrative, "DENY"),
        Statement::Set(_) => (SqlStatementType::Administrative, "SET"),
        Statement::Use(_) => (SqlStatementType::Administrative, "USE"),
        Statement::Kill { .. } => (SqlStatementType::Administrative, "KILL"),
        Statement::Vacuum { .. } => (SqlStatementType::Administrative, "VACUUM"),
        Statement::Analyze { .. } => (SqlStatementType::Administrative, "ANALYZE"),
        Statement::Discard { .. } => (SqlStatementType::Administrative, "DISCARD"),
        Statement::LockTables { .. } => (SqlStatementType::Administrative, "LOCK"),
        Statement::UnlockTables => (SqlStatementType::Administrative, "UNLOCK"),
        Statement::Flush { .. } => (SqlStatementType::Administrative, "FLUSH"),
        Statement::Cache { .. } => (SqlStatementType::Administrative, "CACHE"),
        Statement::UNCache { .. } => (SqlStatementType::Administrative, "UNCACHE"),
        Statement::Pragma { .. } => (SqlStatementType::Administrative, "PRAGMA"),
        Statement::Load { .. } => (SqlStatementType::Administrative, "LOAD"),
        Statement::Unload { .. } => (SqlStatementType::Administrative, "UNLOAD"),
        Statement::Install { .. } => (SqlStatementType::Administrative, "INSTALL"),
        Statement::OptimizeTable { .. } => (SqlStatementType::Administrative, "OPTIMIZE"),
        Statement::AttachDatabase { .. } => (SqlStatementType::Administrative, "ATTACH"),
        Statement::AttachDuckDBDatabase { .. } => (SqlStatementType::Administrative, "ATTACH"),
        Statement::DetachDuckDBDatabase { .. } => (SqlStatementType::Administrative, "DETACH"),
        Statement::LISTEN { .. } => (SqlStatementType::Administrative, "LISTEN"),
        Statement::UNLISTEN { .. } => (SqlStatementType::Administrative, "UNLISTEN"),
        Statement::NOTIFY { .. } => (SqlStatementType::Administrative, "NOTIFY"),

        // =====================================================================
        // Unknown/other statements - BLOCKED (conservative approach)
        // =====================================================================
        _ => (SqlStatementType::Unknown, "Unknown"),
    }
}

/// Determine the statement type from SQL text.
///
/// This function parses the SQL and classifies the first statement.
/// For validation, use `validate_readonly()` instead.
pub fn get_statement_type(sql: &str) -> SqlStatementType {
    let dialect = GenericDialect {};

    match Parser::parse_sql(&dialect, sql) {
        Ok(statements) if !statements.is_empty() => {
            let (stmt_type, _) = classify_statement(&statements[0]);
            stmt_type
        }
        _ => SqlStatementType::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // Tests for get_statement_type - DML Write (US1)
    // =========================================================================

    #[test]
    fn test_statement_type_insert() {
        assert_eq!(
            get_statement_type("INSERT INTO users VALUES (1)"),
            SqlStatementType::DmlWrite
        );
    }

    #[test]
    fn test_statement_type_update() {
        assert_eq!(
            get_statement_type("UPDATE users SET name = 'test'"),
            SqlStatementType::DmlWrite
        );
    }

    #[test]
    fn test_statement_type_delete() {
        assert_eq!(
            get_statement_type("DELETE FROM users WHERE id = 1"),
            SqlStatementType::DmlWrite
        );
    }

    #[test]
    fn test_statement_type_merge() {
        // sqlparser supports MERGE syntax
        let sql = "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE SET target.val = source.val";
        assert_eq!(get_statement_type(sql), SqlStatementType::DmlWrite);
    }

    #[test]
    fn test_statement_type_select() {
        assert_eq!(
            get_statement_type("SELECT * FROM users"),
            SqlStatementType::Select
        );
    }

    #[test]
    fn test_statement_type_case_insensitive() {
        assert_eq!(
            get_statement_type("insert into users values (1)"),
            SqlStatementType::DmlWrite
        );
        assert_eq!(
            get_statement_type("INSERT INTO users VALUES (1)"),
            SqlStatementType::DmlWrite
        );
        assert_eq!(
            get_statement_type("Insert Into users Values (1)"),
            SqlStatementType::DmlWrite
        );
    }

    // =========================================================================
    // Tests for get_statement_type - DDL (US2)
    // =========================================================================

    #[test]
    fn test_statement_type_create() {
        assert_eq!(
            get_statement_type("CREATE TABLE test (id INT)"),
            SqlStatementType::Ddl
        );
    }

    #[test]
    fn test_statement_type_drop() {
        assert_eq!(
            get_statement_type("DROP TABLE users"),
            SqlStatementType::Ddl
        );
    }

    #[test]
    fn test_statement_type_alter() {
        assert_eq!(
            get_statement_type("ALTER TABLE users ADD COLUMN age INT"),
            SqlStatementType::Ddl
        );
    }

    #[test]
    fn test_statement_type_truncate() {
        assert_eq!(
            get_statement_type("TRUNCATE TABLE users"),
            SqlStatementType::Ddl
        );
    }

    // =========================================================================
    // Tests for get_statement_type - Transaction
    // =========================================================================

    #[test]
    fn test_statement_type_begin() {
        // sqlparser uses START TRANSACTION
        assert_eq!(
            get_statement_type("START TRANSACTION"),
            SqlStatementType::Transaction
        );
    }

    #[test]
    fn test_statement_type_commit() {
        assert_eq!(get_statement_type("COMMIT"), SqlStatementType::Transaction);
    }

    #[test]
    fn test_statement_type_rollback() {
        assert_eq!(
            get_statement_type("ROLLBACK"),
            SqlStatementType::Transaction
        );
    }

    // =========================================================================
    // Tests for get_statement_type - Procedure
    // =========================================================================

    #[test]
    fn test_statement_type_call() {
        assert_eq!(
            get_statement_type("CALL my_procedure()"),
            SqlStatementType::ProcedureCall
        );
    }

    #[test]
    fn test_statement_type_execute() {
        assert_eq!(
            get_statement_type("EXECUTE my_procedure"),
            SqlStatementType::ProcedureCall
        );
    }

    // =========================================================================
    // Tests for get_statement_type - Administrative
    // =========================================================================

    #[test]
    fn test_statement_type_grant() {
        assert_eq!(
            get_statement_type("GRANT SELECT ON users TO role1"),
            SqlStatementType::Administrative
        );
    }

    #[test]
    fn test_statement_type_vacuum() {
        assert_eq!(
            get_statement_type("VACUUM"),
            SqlStatementType::Administrative
        );
    }

    // =========================================================================
    // Tests for get_statement_type - Read operations
    // =========================================================================

    #[test]
    fn test_statement_type_with_cte() {
        assert_eq!(
            get_statement_type("WITH cte AS (SELECT 1) SELECT * FROM cte"),
            SqlStatementType::Select
        );
    }

    #[test]
    fn test_statement_type_show() {
        assert_eq!(get_statement_type("SHOW TABLES"), SqlStatementType::Select);
    }

    #[test]
    fn test_statement_type_describe() {
        assert_eq!(
            get_statement_type("DESCRIBE users"),
            SqlStatementType::Select
        );
    }

    // =========================================================================
    // Tests for EXPLAIN handling
    // =========================================================================

    #[test]
    fn test_explain_select_allowed() {
        assert_eq!(
            get_statement_type("EXPLAIN SELECT * FROM users"),
            SqlStatementType::Select
        );
    }

    #[test]
    fn test_explain_insert_blocked() {
        assert_eq!(
            get_statement_type("EXPLAIN INSERT INTO users VALUES (1)"),
            SqlStatementType::DmlWrite
        );
    }

    #[test]
    fn test_explain_analyze_select() {
        assert_eq!(
            get_statement_type("EXPLAIN ANALYZE SELECT * FROM users"),
            SqlStatementType::Select
        );
    }

    // =========================================================================
    // Tests for SQL with comments (sqlparser handles this correctly)
    // =========================================================================

    #[test]
    fn test_comment_before_select() {
        assert_eq!(
            get_statement_type("-- comment\nSELECT 1"),
            SqlStatementType::Select
        );
    }

    #[test]
    fn test_comment_before_insert() {
        assert_eq!(
            get_statement_type("/* comment */ INSERT INTO users VALUES (1)"),
            SqlStatementType::DmlWrite
        );
    }

    // =========================================================================
    // Tests for validate_readonly
    // =========================================================================

    #[test]
    fn test_validate_readonly_select_ok() {
        assert!(validate_readonly("SELECT * FROM users").is_ok());
    }

    #[test]
    fn test_validate_readonly_insert_error() {
        let result = validate_readonly("INSERT INTO users VALUES (1)");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, DbError::Permission { .. }));
    }

    #[test]
    fn test_validate_readonly_update_error() {
        let result = validate_readonly("UPDATE users SET name = 'test'");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_readonly_create_error() {
        let result = validate_readonly("CREATE TABLE test (id INT)");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_readonly_drop_error() {
        let result = validate_readonly("DROP TABLE users");
        assert!(result.is_err());
    }

    // =========================================================================
    // Tests for error messages (US3)
    // =========================================================================

    #[test]
    fn test_error_message_dml_contains_execute() {
        let result = validate_readonly("INSERT INTO users VALUES (1)");
        let err = result.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("execute"),
            "Error message should mention execute: {}",
            msg
        );
    }

    #[test]
    fn test_error_message_ddl_contains_schema() {
        let result = validate_readonly("CREATE TABLE test (id INT)");
        let err = result.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("schema"),
            "Error message should mention schema: {}",
            msg
        );
    }

    #[test]
    fn test_error_message_transaction_contains_tool() {
        let result = validate_readonly("COMMIT");
        let err = result.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("begin_transaction") || msg.contains("transaction"),
            "Error message should mention transaction tool: {}",
            msg
        );
    }

    // =========================================================================
    // Tests for complex SQL patterns (AST parsing handles correctly)
    // =========================================================================

    #[test]
    fn test_complex_select_with_subquery() {
        let sql = r#"
            SELECT u.name, (SELECT COUNT(*) FROM orders WHERE user_id = u.id) as order_count
            FROM users u
            WHERE u.id IN (SELECT user_id FROM active_users)
        "#;
        assert!(validate_readonly(sql).is_ok());
    }

    #[test]
    fn test_select_with_union() {
        let sql = "SELECT a FROM t1 UNION ALL SELECT b FROM t2";
        assert!(validate_readonly(sql).is_ok());
    }

    #[test]
    fn test_multiple_statements_blocked() {
        // If any statement is a write, the whole thing should be blocked
        let sql = "SELECT 1; INSERT INTO users VALUES (1)";
        assert!(validate_readonly(sql).is_err());
    }

    #[test]
    fn test_insert_select_blocked() {
        // INSERT ... SELECT should be blocked even though it contains SELECT
        let sql = "INSERT INTO archive SELECT * FROM users WHERE created_at < '2020-01-01'";
        assert!(validate_readonly(sql).is_err());
    }

    #[test]
    fn test_update_with_subquery_blocked() {
        let sql = "UPDATE users SET status = 'inactive' WHERE id IN (SELECT id FROM old_users)";
        assert!(validate_readonly(sql).is_err());
    }

    #[test]
    fn test_delete_with_join_blocked() {
        let sql = "DELETE FROM users USING old_users WHERE users.id = old_users.id";
        // This might not parse with GenericDialect, so just check it's not allowed
        let result = validate_readonly(sql);
        // Either parse error or permission error is acceptable
        assert!(result.is_err());
    }
}
