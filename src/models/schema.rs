//! Schema-related data models.
//!
//! This module defines types for database schema introspection.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    pub table_type: TableType,
    /// MySQL only
    #[serde(skip_serializing_if = "Option::is_none")]
    pub engine: Option<String>,
    /// MySQL only
    #[serde(skip_serializing_if = "Option::is_none")]
    pub charset: Option<String>,
    /// MySQL only
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collation: Option<String>,
    /// Bytes (excluding indexes)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_size: Option<u64>,
    /// Bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_size: Option<u64>,
    /// Bytes (data + indexes)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_size: Option<u64>,
    /// Deprecated: use total_size
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<DateTime<Utc>>,
}

impl TableInfo {
    /// Create a new table info.
    pub fn new(name: impl Into<String>, table_type: TableType) -> Self {
        Self {
            name: name.into(),
            schema: None,
            table_type,
            engine: None,
            charset: None,
            collation: None,
            data_size: None,
            index_size: None,
            total_size: None,
            size_bytes: None,
            row_count: None,
            comment: None,
            created_at: None,
            updated_at: None,
        }
    }

    /// Set the schema name.
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Set the storage engine (MySQL only).
    pub fn with_engine(mut self, engine: impl Into<String>) -> Self {
        self.engine = Some(engine.into());
        self
    }

    /// Set the character set (MySQL only).
    pub fn with_charset(mut self, charset: impl Into<String>) -> Self {
        self.charset = Some(charset.into());
        self
    }

    /// Set the collation rule (MySQL only).
    pub fn with_collation(mut self, collation: impl Into<String>) -> Self {
        self.collation = Some(collation.into());
        self
    }

    /// Set the data size in bytes (excluding indexes).
    pub fn with_data_size(mut self, data_size: u64) -> Self {
        self.data_size = Some(data_size);
        self
    }

    /// Set the index size in bytes.
    pub fn with_index_size(mut self, index_size: u64) -> Self {
        self.index_size = Some(index_size);
        self
    }

    /// Set the total size in bytes (data + indexes).
    /// Also sets size_bytes for backward compatibility.
    pub fn with_total_size(mut self, total_size: u64) -> Self {
        self.total_size = Some(total_size);
        self.size_bytes = Some(total_size);
        self
    }

    /// Set the table size in bytes (deprecated, use with_total_size instead).
    pub fn with_size_bytes(mut self, size_bytes: u64) -> Self {
        self.size_bytes = Some(size_bytes);
        self.total_size = Some(size_bytes);
        self
    }

    /// Set the estimated row count.
    pub fn with_row_count(mut self, row_count: u64) -> Self {
        self.row_count = Some(row_count);
        self
    }

    /// Set the table comment/description.
    pub fn with_comment(mut self, comment: impl Into<String>) -> Self {
        self.comment = Some(comment.into());
        self
    }

    /// Set the creation timestamp.
    pub fn with_created_at(mut self, created_at: DateTime<Utc>) -> Self {
        self.created_at = Some(created_at);
        self
    }

    /// Set the last update timestamp.
    pub fn with_updated_at(mut self, updated_at: DateTime<Utc>) -> Self {
        self.updated_at = Some(updated_at);
        self
    }
}

/// Type of database table object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TableType {
    Table,
    View,
    MaterializedView,
    SystemTable,
    TemporaryTable,
}

impl TableType {
    /// Parse table type from database-specific string.
    pub fn parse(s: &str) -> Self {
        let lower = s.to_lowercase();
        match lower.as_str() {
            "table" | "base table" => Self::Table,
            "view" => Self::View,
            "materialized view" | "matview" => Self::MaterializedView,
            "system table" => Self::SystemTable,
            "local temporary" | "temporary" | "temp" => Self::TemporaryTable,
            _ => Self::Table, // Default to table
        }
    }
}

impl std::fmt::Display for TableType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Table => write!(f, "table"),
            Self::View => write!(f, "view"),
            Self::MaterializedView => write!(f, "materialized_view"),
            Self::SystemTable => write!(f, "system_table"),
            Self::TemporaryTable => write!(f, "temporary_table"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub table_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_name: Option<String>,
    pub columns: Vec<ColumnDefinition>,
    pub primary_key: Vec<String>,
    pub foreign_keys: Vec<ForeignKey>,
    pub indexes: Vec<IndexInfo>,
}

impl TableSchema {
    /// Create a new table schema.
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            schema_name: None,
            columns: Vec::new(),
            primary_key: Vec::new(),
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
        }
    }

    /// Set the schema name.
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema_name = Some(schema.into());
        self
    }

    /// Add a column definition.
    pub fn with_column(mut self, column: ColumnDefinition) -> Self {
        self.columns.push(column);
        self
    }

    /// Set the primary key columns.
    pub fn with_primary_key(mut self, columns: Vec<String>) -> Self {
        self.primary_key = columns;
        self
    }

    /// Get the fully qualified table name.
    pub fn qualified_name(&self) -> String {
        match &self.schema_name {
            Some(schema) => format!("{}.{}", schema, self.table_name),
            None => self.table_name.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    /// Full type (e.g., `varchar(30)`, `bigint unsigned`)
    pub data_type: String,
    pub nullable: bool,
    /// Default value with appropriate JSON type based on column data type
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_value: Option<serde_json::Value>,
    pub is_primary_key: bool,
    /// MySQL only
    #[serde(skip_serializing_if = "Option::is_none")]
    pub character_set: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collation: Option<String>,
    /// MySQL only (e.g., `auto_increment`)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extra: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

impl ColumnDefinition {
    /// Create a new column definition.
    pub fn new(name: impl Into<String>, data_type: impl Into<String>, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type: data_type.into(),
            nullable,
            default_value: None,
            is_primary_key: false,
            character_set: None,
            collation: None,
            extra: None,
            comment: None,
        }
    }

    /// Set whether this is a primary key column.
    pub fn with_primary_key(mut self, is_pk: bool) -> Self {
        self.is_primary_key = is_pk;
        self
    }

    /// Set the default value (as JSON value).
    pub fn with_default(mut self, default_value: serde_json::Value) -> Self {
        self.default_value = Some(default_value);
        self
    }

    /// Set the default value from a string, converting to appropriate JSON type
    /// based on the column's data_type.
    pub fn with_default_str(mut self, default_str: &str) -> Self {
        self.default_value = Some(parse_default_value(default_str, &self.data_type));
        self
    }

    /// Set the character set (MySQL only).
    pub fn with_character_set(mut self, charset: impl Into<String>) -> Self {
        self.character_set = Some(charset.into());
        self
    }

    /// Set the collation rule.
    pub fn with_collation(mut self, collation: impl Into<String>) -> Self {
        self.collation = Some(collation.into());
        self
    }

    /// Set the extra attributes (MySQL only).
    pub fn with_extra(mut self, extra: impl Into<String>) -> Self {
        self.extra = Some(extra.into());
        self
    }

    /// Set the column comment.
    pub fn with_comment(mut self, comment: impl Into<String>) -> Self {
        self.comment = Some(comment.into());
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKey {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub column: String,
    pub references_table: String,
    pub references_column: String,
    pub on_delete: ForeignKeyAction,
    pub on_update: ForeignKeyAction,
}

impl ForeignKey {
    /// Create a new foreign key.
    pub fn new(
        column: impl Into<String>,
        references_table: impl Into<String>,
        references_column: impl Into<String>,
    ) -> Self {
        Self {
            name: None,
            column: column.into(),
            references_table: references_table.into(),
            references_column: references_column.into(),
            on_delete: ForeignKeyAction::NoAction,
            on_update: ForeignKeyAction::NoAction,
        }
    }

    /// Set the constraint name.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set the on delete action.
    pub fn with_on_delete(mut self, action: ForeignKeyAction) -> Self {
        self.on_delete = action;
        self
    }

    /// Set the on update action.
    pub fn with_on_update(mut self, action: ForeignKeyAction) -> Self {
        self.on_update = action;
        self
    }
}

/// Foreign key referential action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForeignKeyAction {
    /// No action (error if referenced)
    #[default]
    NoAction,
    /// Restrict (same as NoAction in most databases)
    Restrict,
    /// Cascade the operation
    Cascade,
    /// Set to NULL
    SetNull,
    /// Set to default value
    SetDefault,
}

impl ForeignKeyAction {
    /// Parse from database-specific string.
    pub fn parse(s: &str) -> Self {
        let upper = s.to_uppercase();
        match upper.as_str() {
            "CASCADE" => Self::Cascade,
            "SET NULL" => Self::SetNull,
            "SET DEFAULT" => Self::SetDefault,
            "RESTRICT" => Self::Restrict,
            _ => Self::NoAction,
        }
    }
}

impl std::fmt::Display for ForeignKeyAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoAction => write!(f, "NO ACTION"),
            Self::Restrict => write!(f, "RESTRICT"),
            Self::Cascade => write!(f, "CASCADE"),
            Self::SetNull => write!(f, "SET NULL"),
            Self::SetDefault => write!(f, "SET DEFAULT"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexInfo {
    pub name: String,
    pub columns: Vec<String>,
    pub is_unique: bool,
    pub is_primary: bool,
    /// BTREE, HASH, FULLTEXT, GIN, GIST, etc.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_algorithm: Option<String>,
}

impl IndexInfo {
    /// Create a new index info.
    pub fn new(name: impl Into<String>, columns: Vec<String>) -> Self {
        Self {
            name: name.into(),
            columns,
            is_unique: false,
            is_primary: false,
            index_algorithm: None,
        }
    }

    /// Set whether this is a unique index.
    pub fn with_unique(mut self, is_unique: bool) -> Self {
        self.is_unique = is_unique;
        self
    }

    /// Set whether this is the primary key index.
    pub fn with_primary(mut self, is_primary: bool) -> Self {
        self.is_primary = is_primary;
        if is_primary {
            self.is_unique = true;
        }
        self
    }

    /// Set the index algorithm (BTREE, HASH, FULLTEXT, GIN, etc.).
    pub fn with_algorithm(mut self, algorithm: impl Into<String>) -> Self {
        self.index_algorithm = Some(algorithm.into());
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListTablesRequest {
    pub connection_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    #[serde(default = "default_true")]
    pub include_views: bool,
}

fn default_true() -> bool {
    true
}

impl ListTablesRequest {
    /// Create a new list tables request.
    pub fn new(connection_id: impl Into<String>) -> Self {
        Self {
            connection_id: connection_id.into(),
            schema: None,
            include_views: true,
        }
    }

    /// Set the schema filter.
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribeTableRequest {
    pub connection_id: String,
    pub table_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
}

impl DescribeTableRequest {
    /// Create a new describe table request.
    pub fn new(connection_id: impl Into<String>, table_name: impl Into<String>) -> Self {
        Self {
            connection_id: connection_id.into(),
            table_name: table_name.into(),
            schema: None,
        }
    }

    /// Set the schema filter.
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }
}

/// Parse a default value string into the appropriate JSON type based on column data type.
///
/// - Integer types (int, bigint, smallint, tinyint) → JSON Number
/// - Float types (float, double, real) → JSON Number
/// - Boolean types → JSON Boolean
/// - JSON/JSONB types → Parsed JSON value
/// - Decimal/numeric → JSON String (preserve precision)
/// - String types (varchar, text, char) → JSON String
/// - Expressions (CURRENT_TIMESTAMP, nextval, etc.) → JSON String
pub fn parse_default_value(default_str: &str, data_type: &str) -> serde_json::Value {
    let dt_lower = data_type.to_lowercase();

    if dt_lower.contains("int")
        || dt_lower.contains("serial")
        || (dt_lower == "integer" || dt_lower.starts_with("integer"))
    {
        if let Ok(n) = default_str.parse::<i64>() {
            return serde_json::Value::Number(n.into());
        }
    }

    if (dt_lower.contains("float") || dt_lower.contains("double") || dt_lower == "real")
        && !dt_lower.contains("decimal")
        && !dt_lower.contains("numeric")
    {
        if let Ok(n) = default_str.parse::<f64>() {
            if let Some(num) = serde_json::Number::from_f64(n) {
                return serde_json::Value::Number(num);
            }
        }
    }

    if dt_lower.contains("bool") {
        match default_str.to_lowercase().as_str() {
            "true" | "1" | "t" => return serde_json::Value::Bool(true),
            "false" | "0" | "f" => return serde_json::Value::Bool(false),
            _ => {}
        }
    }

    // JSON/JSONB types - try to parse as JSON
    if dt_lower == "json" || dt_lower == "jsonb" {
        if let Ok(parsed) = serde_json::from_str(default_str) {
            return parsed;
        }
    }

    // Everything else: decimal/numeric, varchar, text, expressions, etc.
    serde_json::Value::String(default_str.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_type_parsing() {
        assert_eq!(TableType::parse("TABLE"), TableType::Table);
        assert_eq!(TableType::parse("BASE TABLE"), TableType::Table);
        assert_eq!(TableType::parse("VIEW"), TableType::View);
        assert_eq!(
            TableType::parse("MATERIALIZED VIEW"),
            TableType::MaterializedView
        );
    }

    #[test]
    fn test_table_schema_builder() {
        let schema = TableSchema::new("users")
            .with_schema("public")
            .with_column(ColumnDefinition::new("id", "bigint", false).with_primary_key(true))
            .with_column(ColumnDefinition::new("name", "varchar", false));

        assert_eq!(schema.qualified_name(), "public.users");
        assert_eq!(schema.columns.len(), 2);
    }

    #[test]
    fn test_foreign_key_action_parsing() {
        assert_eq!(
            ForeignKeyAction::parse("CASCADE"),
            ForeignKeyAction::Cascade
        );
        assert_eq!(
            ForeignKeyAction::parse("SET NULL"),
            ForeignKeyAction::SetNull
        );
        assert_eq!(
            ForeignKeyAction::parse("UNKNOWN"),
            ForeignKeyAction::NoAction
        );
    }

    #[test]
    fn test_index_info_builder() {
        let index = IndexInfo::new("users_pkey", vec!["id".to_string()])
            .with_primary(true)
            .with_algorithm("BTREE");

        assert!(index.is_primary);
        assert!(index.is_unique); // Primary implies unique
        assert_eq!(index.index_algorithm, Some("BTREE".to_string()));
    }

    #[test]
    fn test_table_info_with_metadata() {
        use chrono::TimeZone;

        let created = Utc.with_ymd_and_hms(2025, 6, 15, 10, 30, 0).unwrap();
        let updated = Utc.with_ymd_and_hms(2026, 1, 1, 8, 45, 0).unwrap();

        let table = TableInfo::new("users", TableType::Table)
            .with_schema("public")
            .with_size_bytes(1048576)
            .with_row_count(15000)
            .with_created_at(created)
            .with_updated_at(updated);

        assert_eq!(table.name, "users");
        assert_eq!(table.schema, Some("public".to_string()));
        assert_eq!(table.size_bytes, Some(1048576));
        assert_eq!(table.row_count, Some(15000));
        assert_eq!(table.created_at, Some(created));
        assert_eq!(table.updated_at, Some(updated));
    }

    #[test]
    fn test_table_info_serialization_without_optional_fields() {
        let table = TableInfo::new("users", TableType::Table);
        let json = serde_json::to_string(&table).unwrap();

        // Optional fields with None should not appear in JSON
        assert!(!json.contains("size_bytes"));
        assert!(!json.contains("row_count"));
        assert!(!json.contains("created_at"));
        assert!(!json.contains("updated_at"));
        assert!(!json.contains("schema"));
    }

    #[test]
    fn test_table_info_serialization_with_metadata() {
        use chrono::TimeZone;

        let created = Utc.with_ymd_and_hms(2025, 6, 15, 10, 30, 0).unwrap();

        let table = TableInfo::new("users", TableType::Table)
            .with_size_bytes(1024)
            .with_row_count(100)
            .with_created_at(created);

        let json = serde_json::to_string(&table).unwrap();

        assert!(json.contains("\"size_bytes\":1024"));
        assert!(json.contains("\"row_count\":100"));
        assert!(json.contains("created_at"));
    }

    #[test]
    fn test_parse_default_value_integer_types() {
        // int types should become JSON numbers
        assert_eq!(
            parse_default_value("0", "tinyint unsigned"),
            serde_json::Value::Number(0.into())
        );
        assert_eq!(
            parse_default_value("42", "int"),
            serde_json::Value::Number(42.into())
        );
        assert_eq!(
            parse_default_value("-100", "bigint"),
            serde_json::Value::Number((-100).into())
        );
        assert_eq!(
            parse_default_value("1", "smallint"),
            serde_json::Value::Number(1.into())
        );
        assert_eq!(
            parse_default_value("5", "serial"),
            serde_json::Value::Number(5.into())
        );
    }

    #[test]
    fn test_parse_default_value_float_types() {
        // float/double should become JSON numbers
        assert_eq!(parse_default_value("1.5", "float"), serde_json::json!(1.5));
        assert_eq!(
            parse_default_value("99.99", "double"),
            serde_json::json!(99.99)
        );
    }

    #[test]
    fn test_parse_default_value_decimal_stays_string() {
        // decimal/numeric should stay as string (precision)
        assert_eq!(
            parse_default_value("123.456789", "decimal(10,6)"),
            serde_json::Value::String("123.456789".to_string())
        );
        assert_eq!(
            parse_default_value("99.99", "numeric(5,2)"),
            serde_json::Value::String("99.99".to_string())
        );
    }

    #[test]
    fn test_parse_default_value_boolean() {
        assert_eq!(
            parse_default_value("true", "boolean"),
            serde_json::Value::Bool(true)
        );
        assert_eq!(
            parse_default_value("false", "bool"),
            serde_json::Value::Bool(false)
        );
        assert_eq!(
            parse_default_value("1", "boolean"),
            serde_json::Value::Bool(true)
        );
        assert_eq!(
            parse_default_value("0", "boolean"),
            serde_json::Value::Bool(false)
        );
    }

    #[test]
    fn test_parse_default_value_string_types() {
        // string types should stay as strings
        assert_eq!(
            parse_default_value("hello", "varchar(255)"),
            serde_json::Value::String("hello".to_string())
        );
        assert_eq!(
            parse_default_value("", "text"),
            serde_json::Value::String("".to_string())
        );
    }

    #[test]
    fn test_parse_default_value_expressions() {
        // Expressions should stay as strings
        assert_eq!(
            parse_default_value("CURRENT_TIMESTAMP", "timestamp"),
            serde_json::Value::String("CURRENT_TIMESTAMP".to_string())
        );
        assert_eq!(
            parse_default_value("nextval('users_id_seq'::regclass)", "bigint"),
            serde_json::Value::String("nextval('users_id_seq'::regclass)".to_string())
        );
    }

    #[test]
    fn test_parse_default_value_json_types() {
        // JSON types should be parsed as JSON values
        assert_eq!(parse_default_value("{}", "json"), serde_json::json!({}));
        assert_eq!(parse_default_value("[]", "jsonb"), serde_json::json!([]));
        assert_eq!(
            parse_default_value(r#"{"key": "value"}"#, "json"),
            serde_json::json!({"key": "value"})
        );
        assert_eq!(parse_default_value("null", "json"), serde_json::Value::Null);
        assert_eq!(
            parse_default_value("[1, 2, 3]", "jsonb"),
            serde_json::json!([1, 2, 3])
        );
        // Invalid JSON falls back to string
        assert_eq!(
            parse_default_value("not valid json", "json"),
            serde_json::Value::String("not valid json".to_string())
        );
    }
}
