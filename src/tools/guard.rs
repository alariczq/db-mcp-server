//! Dangerous operation guard for the execute tool.
//!
//! This module provides SQL statement analysis to detect dangerous operations
//! (DROP, TRUNCATE, DELETE/UPDATE without WHERE) that could cause data loss.
//! Uses sqlparser for accurate AST-based detection, preventing bypass through
//! formatting tricks or SQL comments.

use crate::error::{DbError, DbResult};
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Type of dangerous SQL operation detected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DangerousOperationType {
    /// DROP DATABASE statement
    DropDatabase,
    /// DROP TABLE statement
    DropTable,
    /// DROP INDEX statement
    DropIndex,
    /// ALTER TABLE DROP COLUMN statement
    AlterTableDropColumn,
    /// TRUNCATE TABLE statement
    Truncate,
    /// DELETE without WHERE clause
    DeleteWithoutWhere,
    /// UPDATE without WHERE clause
    UpdateWithoutWhere,
}

impl DangerousOperationType {
    /// Get the operation name for error messages.
    pub fn operation_name(&self) -> &'static str {
        match self {
            Self::DropDatabase => "DROP DATABASE",
            Self::DropTable => "DROP TABLE",
            Self::DropIndex => "DROP INDEX",
            Self::AlterTableDropColumn => "ALTER TABLE DROP COLUMN",
            Self::Truncate => "TRUNCATE",
            Self::DeleteWithoutWhere => "DELETE without WHERE",
            Self::UpdateWithoutWhere => "UPDATE without WHERE",
        }
    }

    /// Get the reason why this operation is dangerous.
    pub fn reason(&self) -> &'static str {
        match self {
            Self::DropDatabase => {
                "This will permanently delete the entire database and all its data"
            }
            Self::DropTable => "This will permanently delete the table and all its data",
            Self::DropIndex => "This will permanently delete the index",
            Self::AlterTableDropColumn => {
                "This will permanently delete the column and all its data"
            }
            Self::Truncate => "This will remove all rows from the table",
            Self::DeleteWithoutWhere => "This will delete all rows from the table",
            Self::UpdateWithoutWhere => "This will update all rows in the table",
        }
    }
}

/// Result of checking SQL for dangerous operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DangerousOperationResult {
    /// SQL is safe, no dangerous operations detected
    Safe,
    /// SQL contains a dangerous operation
    Dangerous(DangerousOperationType),
}

/// Result of checking if SQL is a read-only operation that shouldn't use execute tool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadOnlyCheckResult {
    /// SQL is a write operation, suitable for execute tool
    WriteOperation,
    /// SQL is a read-only operation (SELECT, SHOW, etc.), should use query tool
    ReadOnlyOperation,
}

/// Check if SQL contains dangerous operations.
///
/// Parses the SQL using sqlparser and analyzes the AST to detect:
/// - DROP DATABASE/TABLE/INDEX
/// - TRUNCATE TABLE
/// - DELETE without WHERE clause
/// - UPDATE without WHERE clause
///
/// Returns `Err` if parsing fails (no fallback to string-based detection).
///
/// # Examples
///
/// ```
/// use db_mcp_server::tools::guard::{check_dangerous_sql, DangerousOperationResult, DangerousOperationType};
///
/// // DROP is always dangerous
/// let result = check_dangerous_sql("DROP TABLE users").unwrap();
/// assert!(matches!(result, DangerousOperationResult::Dangerous(DangerousOperationType::DropTable)));
///
/// // DELETE without WHERE is dangerous
/// let result = check_dangerous_sql("DELETE FROM users").unwrap();
/// assert!(matches!(result, DangerousOperationResult::Dangerous(DangerousOperationType::DeleteWithoutWhere)));
///
/// // DELETE with WHERE is safe
/// let result = check_dangerous_sql("DELETE FROM users WHERE id = 1").unwrap();
/// assert!(matches!(result, DangerousOperationResult::Safe));
/// ```
pub fn check_dangerous_sql(sql: &str) -> DbResult<DangerousOperationResult> {
    let dialect = GenericDialect {};

    let statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| DbError::invalid_input(format!("Failed to parse SQL statement: {}", e)))?;

    if statements.is_empty() {
        return Err(DbError::invalid_input("Empty SQL statement"));
    }

    // Check all statements - if ANY is dangerous, return the first dangerous one
    for stmt in &statements {
        if let Some(dangerous_type) = check_statement_dangerous(stmt) {
            return Ok(DangerousOperationResult::Dangerous(dangerous_type));
        }
    }

    Ok(DangerousOperationResult::Safe)
}

/// Check if SQL contains only read-only operations that shouldn't use execute tool.
///
/// The execute tool is designed for write operations. This function detects
/// when a user tries to use execute for read operations like SELECT.
///
/// # Examples
///
/// ```
/// use db_mcp_server::tools::guard::{check_readonly_sql, ReadOnlyCheckResult};
///
/// // SELECT is read-only
/// let result = check_readonly_sql("SELECT * FROM users").unwrap();
/// assert!(matches!(result, ReadOnlyCheckResult::ReadOnlyOperation));
///
/// // INSERT is a write operation
/// let result = check_readonly_sql("INSERT INTO users (name) VALUES ('Alice')").unwrap();
/// assert!(matches!(result, ReadOnlyCheckResult::WriteOperation));
/// ```
pub fn check_readonly_sql(sql: &str) -> DbResult<ReadOnlyCheckResult> {
    let dialect = GenericDialect {};

    let statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| DbError::invalid_input(format!("Failed to parse SQL statement: {}", e)))?;

    if statements.is_empty() {
        return Err(DbError::invalid_input("Empty SQL statement"));
    }

    // If ALL statements are read-only, return ReadOnlyOperation
    // If ANY statement is a write operation, return WriteOperation
    for stmt in &statements {
        if !is_readonly_statement(stmt) {
            return Ok(ReadOnlyCheckResult::WriteOperation);
        }
    }

    Ok(ReadOnlyCheckResult::ReadOnlyOperation)
}

/// Check if a single statement is read-only.
fn is_readonly_statement(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::Query(_)
            | Statement::Explain { .. }
            | Statement::ShowCreate { .. }
            | Statement::ShowTables { .. }
            | Statement::ShowColumns { .. }
            | Statement::ShowDatabases { .. }
            | Statement::ShowSchemas { .. }
            | Statement::ShowFunctions { .. }
            | Statement::ShowVariable { .. }
            | Statement::ShowVariables { .. }
            | Statement::ShowStatus { .. }
            | Statement::ShowCollation { .. }
    )
}

/// Check if a single statement is dangerous.
fn check_statement_dangerous(stmt: &Statement) -> Option<DangerousOperationType> {
    match stmt {
        // DROP statements
        Statement::Drop { object_type, .. } => {
            use sqlparser::ast::ObjectType;
            match object_type {
                ObjectType::Table => Some(DangerousOperationType::DropTable),
                ObjectType::Index => Some(DangerousOperationType::DropIndex),
                ObjectType::Database => Some(DangerousOperationType::DropDatabase),
                ObjectType::Schema => Some(DangerousOperationType::DropDatabase),
                _ => None,
            }
        }

        // ALTER TABLE - check for DROP COLUMN
        Statement::AlterTable(alter_table) => {
            use sqlparser::ast::AlterTableOperation;
            for op in &alter_table.operations {
                if matches!(op, AlterTableOperation::DropColumn { .. }) {
                    return Some(DangerousOperationType::AlterTableDropColumn);
                }
            }
            None
        }

        // TRUNCATE is always dangerous
        Statement::Truncate { .. } => Some(DangerousOperationType::Truncate),

        // DELETE - dangerous only if no WHERE clause
        Statement::Delete(delete) => {
            if delete.selection.is_none() {
                Some(DangerousOperationType::DeleteWithoutWhere)
            } else {
                None
            }
        }

        // UPDATE - dangerous only if no WHERE clause
        Statement::Update(update) => {
            if update.selection.is_none() {
                Some(DangerousOperationType::UpdateWithoutWhere)
            } else {
                None
            }
        }

        // All other statements are safe
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // Tests for DROP detection (T007)
    // =========================================================================

    #[test]
    fn test_drop_table_detected() {
        let result = check_dangerous_sql("DROP TABLE users").unwrap();
        assert!(matches!(
            result,
            DangerousOperationResult::Dangerous(DangerousOperationType::DropTable)
        ));
    }

    #[test]
    fn test_drop_table_if_exists_detected() {
        let result = check_dangerous_sql("DROP TABLE IF EXISTS users").unwrap();
        assert!(matches!(
            result,
            DangerousOperationResult::Dangerous(DangerousOperationType::DropTable)
        ));
    }

    #[test]
    fn test_drop_database_detected() {
        let result = check_dangerous_sql("DROP DATABASE test_db").unwrap();
        assert!(matches!(
            result,
            DangerousOperationResult::Dangerous(DangerousOperationType::DropDatabase)
        ));
    }

    #[test]
    fn test_drop_index_detected() {
        let result = check_dangerous_sql("DROP INDEX idx_users").unwrap();
        assert!(matches!(
            result,
            DangerousOperationResult::Dangerous(DangerousOperationType::DropIndex)
        ));
    }

    // =========================================================================
    // Tests for TRUNCATE detection (T008)
    // =========================================================================

    #[test]
    fn test_truncate_detected() {
        let result = check_dangerous_sql("TRUNCATE TABLE users").unwrap();
        assert!(matches!(
            result,
            DangerousOperationResult::Dangerous(DangerousOperationType::Truncate)
        ));
    }

    #[test]
    fn test_truncate_without_table_keyword_detected() {
        let result = check_dangerous_sql("TRUNCATE users").unwrap();
        assert!(matches!(
            result,
            DangerousOperationResult::Dangerous(DangerousOperationType::Truncate)
        ));
    }

    // =========================================================================
    // Tests for DELETE without WHERE detection (T009)
    // =========================================================================

    #[test]
    fn test_delete_without_where_detected() {
        let result = check_dangerous_sql("DELETE FROM users").unwrap();
        assert!(matches!(
            result,
            DangerousOperationResult::Dangerous(DangerousOperationType::DeleteWithoutWhere)
        ));
    }

    #[test]
    fn test_delete_with_where_safe() {
        let result = check_dangerous_sql("DELETE FROM users WHERE id = 1").unwrap();
        assert!(matches!(result, DangerousOperationResult::Safe));
    }

    #[test]
    fn test_delete_with_complex_where_safe() {
        let result =
            check_dangerous_sql("DELETE FROM users WHERE id IN (SELECT id FROM old_users)")
                .unwrap();
        assert!(matches!(result, DangerousOperationResult::Safe));
    }

    // =========================================================================
    // Tests for UPDATE without WHERE detection (T010)
    // =========================================================================

    #[test]
    fn test_update_without_where_detected() {
        let result = check_dangerous_sql("UPDATE users SET active = false").unwrap();
        assert!(matches!(
            result,
            DangerousOperationResult::Dangerous(DangerousOperationType::UpdateWithoutWhere)
        ));
    }

    #[test]
    fn test_update_with_where_safe() {
        let result = check_dangerous_sql("UPDATE users SET active = false WHERE id = 1").unwrap();
        assert!(matches!(result, DangerousOperationResult::Safe));
    }

    // =========================================================================
    // Tests for comment bypass prevention (T011)
    // =========================================================================

    #[test]
    fn test_comment_before_delete_still_detected() {
        let result = check_dangerous_sql("-- comment\nDELETE FROM users").unwrap();
        assert!(matches!(
            result,
            DangerousOperationResult::Dangerous(DangerousOperationType::DeleteWithoutWhere)
        ));
    }

    #[test]
    fn test_inline_comment_still_detected() {
        let result = check_dangerous_sql("DELETE /* comment */ FROM users").unwrap();
        assert!(matches!(
            result,
            DangerousOperationResult::Dangerous(DangerousOperationType::DeleteWithoutWhere)
        ));
    }

    #[test]
    fn test_multiline_formatting_still_detected() {
        let result = check_dangerous_sql(
            r#"DELETE
               FROM
               users"#,
        )
        .unwrap();
        assert!(matches!(
            result,
            DangerousOperationResult::Dangerous(DangerousOperationType::DeleteWithoutWhere)
        ));
    }

    // =========================================================================
    // Tests for string literal false positive prevention (T012)
    // =========================================================================

    #[test]
    fn test_string_literal_not_flagged() {
        // SELECT with a string containing dangerous keywords should be safe
        let result =
            check_dangerous_sql("SELECT * FROM users WHERE name = 'DELETE FROM users'").unwrap();
        assert!(matches!(result, DangerousOperationResult::Safe));
    }

    #[test]
    fn test_insert_with_dangerous_string_safe() {
        let result =
            check_dangerous_sql("INSERT INTO logs (message) VALUES ('DROP TABLE users')").unwrap();
        assert!(matches!(result, DangerousOperationResult::Safe));
    }

    // =========================================================================
    // Tests for safe operations (US3)
    // =========================================================================

    #[test]
    fn test_insert_safe() {
        let result = check_dangerous_sql("INSERT INTO users (name) VALUES ('Alice')").unwrap();
        assert!(matches!(result, DangerousOperationResult::Safe));
    }

    #[test]
    fn test_create_table_safe() {
        let result = check_dangerous_sql("CREATE TABLE new_table (id INT)").unwrap();
        assert!(matches!(result, DangerousOperationResult::Safe));
    }

    #[test]
    fn test_alter_table_add_column_safe() {
        let result = check_dangerous_sql("ALTER TABLE users ADD COLUMN age INT").unwrap();
        assert!(matches!(result, DangerousOperationResult::Safe));
    }

    #[test]
    fn test_alter_table_drop_column_detected() {
        let result = check_dangerous_sql("ALTER TABLE users DROP COLUMN email").unwrap();
        assert!(matches!(
            result,
            DangerousOperationResult::Dangerous(DangerousOperationType::AlterTableDropColumn)
        ));
    }

    #[test]
    fn test_alter_table_drop_column_if_exists_detected() {
        let result = check_dangerous_sql("ALTER TABLE users DROP COLUMN IF EXISTS email").unwrap();
        assert!(matches!(
            result,
            DangerousOperationResult::Dangerous(DangerousOperationType::AlterTableDropColumn)
        ));
    }

    #[test]
    fn test_select_safe() {
        let result = check_dangerous_sql("SELECT * FROM users").unwrap();
        assert!(matches!(result, DangerousOperationResult::Safe));
    }

    // =========================================================================
    // Tests for multi-statement handling (T018)
    // =========================================================================

    #[test]
    fn test_multi_statement_with_dangerous_detected() {
        let result = check_dangerous_sql("INSERT INTO logs VALUES (1); DELETE FROM users").unwrap();
        assert!(matches!(
            result,
            DangerousOperationResult::Dangerous(DangerousOperationType::DeleteWithoutWhere)
        ));
    }

    #[test]
    fn test_multi_statement_all_safe() {
        let result =
            check_dangerous_sql("INSERT INTO logs VALUES (1); UPDATE users SET x = 1 WHERE id = 1")
                .unwrap();
        assert!(matches!(result, DangerousOperationResult::Safe));
    }

    // =========================================================================
    // Tests for parse error handling (T019)
    // =========================================================================

    #[test]
    fn test_parse_error_returns_error() {
        let result = check_dangerous_sql("NOT VALID SQL AT ALL !!!");
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_sql_returns_error() {
        let result = check_dangerous_sql("");
        assert!(result.is_err());
    }

    // =========================================================================
    // Tests for operation name and reason
    // =========================================================================

    #[test]
    fn test_operation_names() {
        assert_eq!(
            DangerousOperationType::DropTable.operation_name(),
            "DROP TABLE"
        );
        assert_eq!(
            DangerousOperationType::DeleteWithoutWhere.operation_name(),
            "DELETE without WHERE"
        );
    }

    #[test]
    fn test_operation_reasons() {
        assert!(
            DangerousOperationType::DropTable
                .reason()
                .contains("permanently")
        );
        assert!(
            DangerousOperationType::DeleteWithoutWhere
                .reason()
                .contains("all rows")
        );
    }

    // =========================================================================
    // Tests for read-only operation detection
    // =========================================================================

    #[test]
    fn test_select_is_readonly() {
        let result = check_readonly_sql("SELECT * FROM users").unwrap();
        assert!(matches!(result, ReadOnlyCheckResult::ReadOnlyOperation));
    }

    #[test]
    fn test_select_with_where_is_readonly() {
        let result = check_readonly_sql("SELECT * FROM users WHERE id = 1").unwrap();
        assert!(matches!(result, ReadOnlyCheckResult::ReadOnlyOperation));
    }

    #[test]
    fn test_insert_is_write() {
        let result = check_readonly_sql("INSERT INTO users (name) VALUES ('Alice')").unwrap();
        assert!(matches!(result, ReadOnlyCheckResult::WriteOperation));
    }

    #[test]
    fn test_update_is_write() {
        let result = check_readonly_sql("UPDATE users SET name = 'Bob' WHERE id = 1").unwrap();
        assert!(matches!(result, ReadOnlyCheckResult::WriteOperation));
    }

    #[test]
    fn test_delete_is_write() {
        let result = check_readonly_sql("DELETE FROM users WHERE id = 1").unwrap();
        assert!(matches!(result, ReadOnlyCheckResult::WriteOperation));
    }

    #[test]
    fn test_create_table_is_write() {
        let result = check_readonly_sql("CREATE TABLE test (id INT)").unwrap();
        assert!(matches!(result, ReadOnlyCheckResult::WriteOperation));
    }

    #[test]
    fn test_explain_is_readonly() {
        let result = check_readonly_sql("EXPLAIN SELECT * FROM users").unwrap();
        assert!(matches!(result, ReadOnlyCheckResult::ReadOnlyOperation));
    }

    #[test]
    fn test_show_tables_is_readonly() {
        let result = check_readonly_sql("SHOW TABLES").unwrap();
        assert!(matches!(result, ReadOnlyCheckResult::ReadOnlyOperation));
    }
}
