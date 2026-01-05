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
use crate::models::DatabaseType;
use sqlparser::ast::Statement;
use sqlparser::dialect::{Dialect, MySqlDialect, PostgreSqlDialect, SQLiteDialect};
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

/// Get the appropriate SQL dialect for the given database type.
fn get_dialect(db_type: DatabaseType) -> Box<dyn Dialect> {
    match db_type {
        DatabaseType::PostgreSQL => Box::new(PostgreSqlDialect {}),
        DatabaseType::MySQL => Box::new(MySqlDialect {}),
        DatabaseType::SQLite => Box::new(SQLiteDialect {}),
    }
}

/// Validate SQL for read-only execution in the query tool.
///
/// Returns `Ok(())` if the statement is allowed (SELECT, SHOW, DESCRIBE, etc.),
/// or `Err(DbError::Permission)` if the statement is a write operation.
///
/// Uses sqlparser for accurate AST-based validation with database-specific dialect support,
/// preventing any bypass through formatting tricks or SQL dialect variations.
///
/// # Arguments
///
/// * `sql` - The SQL statement to validate
/// * `db_type` - The database type to use for parsing (determines SQL dialect)
///
/// # Examples
///
/// ```
/// use db_mcp_server::tools::sql_validator::validate_readonly;
/// use db_mcp_server::models::DatabaseType;
///
/// // SELECT is allowed
/// assert!(validate_readonly("SELECT * FROM users", DatabaseType::PostgreSQL).is_ok());
///
/// // INSERT is blocked
/// assert!(validate_readonly("INSERT INTO users VALUES (1)", DatabaseType::PostgreSQL).is_err());
/// ```
pub fn validate_readonly(sql: &str, db_type: DatabaseType) -> DbResult<()> {
    let dialect = get_dialect(db_type);

    let statements = Parser::parse_sql(dialect.as_ref(), sql).map_err(|e| {
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


#[cfg(test)]
mod tests {
    use super::*;

    // Use PostgreSQL as default test database type
    const TEST_DB_TYPE: DatabaseType = DatabaseType::PostgreSQL;

    // =========================================================================
    // Tests for validate_readonly
    // =========================================================================

    #[test]
    fn test_validate_readonly_select_ok() {
        assert!(validate_readonly("SELECT * FROM users", TEST_DB_TYPE).is_ok());
    }

    #[test]
    fn test_validate_readonly_insert_error() {
        let result = validate_readonly("INSERT INTO users VALUES (1)", TEST_DB_TYPE);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, DbError::Permission { .. }));
    }

    #[test]
    fn test_validate_readonly_update_error() {
        let result = validate_readonly("UPDATE users SET name = 'test'", TEST_DB_TYPE);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_readonly_create_error() {
        let result = validate_readonly("CREATE TABLE test (id INT)", TEST_DB_TYPE);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_readonly_drop_error() {
        let result = validate_readonly("DROP TABLE users", TEST_DB_TYPE);
        assert!(result.is_err());
    }

    // =========================================================================
    // Tests for error messages (US3)
    // =========================================================================

    #[test]
    fn test_error_message_dml_contains_execute() {
        let result = validate_readonly("INSERT INTO users VALUES (1)", TEST_DB_TYPE);
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
        let result = validate_readonly("CREATE TABLE test (id INT)", TEST_DB_TYPE);
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
        let result = validate_readonly("COMMIT", TEST_DB_TYPE);
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
        assert!(validate_readonly(sql, TEST_DB_TYPE).is_ok());
    }

    #[test]
    fn test_select_with_union() {
        let sql = "SELECT a FROM t1 UNION ALL SELECT b FROM t2";
        assert!(validate_readonly(sql, TEST_DB_TYPE).is_ok());
    }

    #[test]
    fn test_multiple_statements_blocked() {
        // If any statement is a write, the whole thing should be blocked
        let sql = "SELECT 1; INSERT INTO users VALUES (1)";
        assert!(validate_readonly(sql, TEST_DB_TYPE).is_err());
    }

    #[test]
    fn test_insert_select_blocked() {
        // INSERT ... SELECT should be blocked even though it contains SELECT
        let sql = "INSERT INTO archive SELECT * FROM users WHERE created_at < '2020-01-01'";
        assert!(validate_readonly(sql, TEST_DB_TYPE).is_err());
    }

    #[test]
    fn test_update_with_subquery_blocked() {
        let sql = "UPDATE users SET status = 'inactive' WHERE id IN (SELECT id FROM old_users)";
        assert!(validate_readonly(sql, TEST_DB_TYPE).is_err());
    }

    #[test]
    fn test_delete_with_join_blocked() {
        let sql = "DELETE FROM users USING old_users WHERE users.id = old_users.id";
        // This might not parse with all dialects, so just check it's not allowed
        let result = validate_readonly(sql, TEST_DB_TYPE);
        // Either parse error or permission error is acceptable
        assert!(result.is_err());
    }
}
