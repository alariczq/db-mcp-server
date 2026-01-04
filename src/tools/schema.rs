//! Schema introspection tools.
//!
//! This module implements the `list_tables` and `describe_table` MCP tools.

use crate::db::ConnectionManager;
use crate::db::schema::SchemaInspector;
use crate::error::{DbError, DbResult};
use crate::models::{ColumnDefinition, ForeignKey, IndexInfo, TableInfo, TableSchema};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::info;

/// Input for the list_tables tool.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct ListTablesInput {
    /// Database connection ID from list_connections
    pub connection_id: String,
    /// Database name. Required for server-level connections (without database in URL).
    #[serde(default)]
    pub database: Option<String>,
    /// Include views in the result. Default: true
    #[serde(default = "default_true")]
    pub include_views: bool,
}

/// Input for the list_databases tool.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct ListDatabasesInput {
    /// Database connection ID from list_connections
    pub connection_id: String,
}

/// Information about a database on the server.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct DatabaseInfo {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_formatted: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collation: Option<String>,
}

/// Output for the list_databases tool.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct ListDatabasesOutput {
    pub databases: Vec<DatabaseInfo>,
    pub count: usize,
}

fn default_true() -> bool {
    true
}

/// Format bytes as human-readable size string.
///
/// Uses binary units (1 KB = 1024 bytes) consistent with database tools.
/// Powered by the `humansize` crate with WINDOWS preset (1024-based, KB/MB/GB units).
///
/// # Examples
///
/// ```
/// use db_mcp_server::tools::schema::format_size;
///
/// assert_eq!(format_size(512), "512 B");
/// assert_eq!(format_size(1024), "1 kB");
/// assert_eq!(format_size(1048576), "1 MB");
/// ```
pub fn format_size(bytes: u64) -> String {
    humansize::format_size(bytes, humansize::WINDOWS)
}

/// Output from the list_tables tool.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct ListTablesOutput {
    /// List of tables/views with metadata
    pub tables: Vec<TableInfoOutput>,
    /// Total number of tables/views returned
    pub count: usize,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct TableInfoOutput {
    pub name: String,
    /// "TABLE" or "VIEW"
    #[serde(rename = "type")]
    pub table_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_size_formatted: Option<String>,
    /// Bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_size_formatted: Option<String>,
    /// Bytes (data + indexes)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_size_formatted: Option<String>,
    /// Deprecated: use total_size
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_count: Option<u64>,
    /// Alias for row_count
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_row: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

impl From<TableInfo> for TableInfoOutput {
    fn from(info: TableInfo) -> Self {
        Self {
            name: info.name,
            table_type: info.table_type.to_string(),
            schema: info.schema,
            engine: info.engine,
            charset: info.charset,
            collation: info.collation,
            data_size: info.data_size,
            data_size_formatted: info.data_size.map(format_size),
            index_size: info.index_size,
            index_size_formatted: info.index_size.map(format_size),
            total_size: info.total_size,
            total_size_formatted: info.total_size.map(format_size),
            size_bytes: info.size_bytes,
            row_count: info.row_count,
            estimated_row: info.row_count,
            comment: info.comment,
            created_at: info.created_at.map(|dt| dt.to_rfc3339()),
            updated_at: info.updated_at.map(|dt| dt.to_rfc3339()),
        }
    }
}

/// Input for the describe_table tool.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct DescribeTableInput {
    /// Database connection ID from list_connections
    pub connection_id: String,
    /// Name of the table to describe
    pub table_name: String,
    /// Database name containing the table. Required for server-level connections (without database in URL).
    #[serde(default)]
    pub database: Option<String>,
}

/// Output from the describe_table tool.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct DescribeTableOutput {
    /// Name of the described table
    pub table_name: String,
    /// Schema/database containing the table
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    /// Column definitions with types and constraints
    pub columns: Vec<ColumnOutput>,
    /// Column names that form the primary key
    pub primary_key: Vec<String>,
    /// Foreign key relationships to other tables
    pub foreign_keys: Vec<ForeignKeyOutput>,
    /// Index definitions on the table
    pub indexes: Vec<IndexOutput>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[schemars(inline)]
pub struct ForeignKeyRef {
    pub table: String,
    pub column: String,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct ColumnOutput {
    pub name: String,
    /// Full type (e.g., varchar(30), bigint unsigned)
    pub data_type: String,
    pub nullable: bool,
    /// Default value with appropriate JSON type (number for int, string for varchar, etc.)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_value: Option<serde_json::Value>,
    pub is_primary_key: bool,
    /// MySQL only
    #[serde(skip_serializing_if = "Option::is_none")]
    pub character_set: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collation: Option<String>,
    /// MySQL only (e.g., auto_increment)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extra: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub foreign_key: Option<ForeignKeyRef>,
}

impl From<ColumnDefinition> for ColumnOutput {
    fn from(col: ColumnDefinition) -> Self {
        Self {
            name: col.name,
            data_type: col.data_type,
            nullable: col.nullable,
            default_value: col.default_value,
            is_primary_key: col.is_primary_key,
            character_set: col.character_set,
            collation: col.collation,
            extra: col.extra,
            comment: col.comment,
            foreign_key: None, // Set later in describe_table handler
        }
    }
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct ForeignKeyOutput {
    pub column: String,
    pub references_table: String,
    pub references_column: String,
    /// CASCADE, SET NULL, SET DEFAULT, RESTRICT, or NO ACTION
    pub on_delete: String,
    /// CASCADE, SET NULL, SET DEFAULT, RESTRICT, or NO ACTION
    pub on_update: String,
}

impl From<ForeignKey> for ForeignKeyOutput {
    fn from(fk: ForeignKey) -> Self {
        Self {
            column: fk.column,
            references_table: fk.references_table,
            references_column: fk.references_column,
            on_delete: fk.on_delete.to_string(),
            on_update: fk.on_update.to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct IndexOutput {
    pub name: String,
    pub columns: Vec<String>,
    pub is_unique: bool,
    pub is_primary: bool,
    /// BTREE, HASH, FULLTEXT, GIN, GIST, etc.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_algorithm: Option<String>,
}

impl From<IndexInfo> for IndexOutput {
    fn from(idx: IndexInfo) -> Self {
        Self {
            name: idx.name,
            columns: idx.columns,
            is_unique: idx.is_unique,
            is_primary: idx.is_primary,
            index_algorithm: idx.index_algorithm,
        }
    }
}

impl From<TableSchema> for DescribeTableOutput {
    fn from(schema: TableSchema) -> Self {
        use std::collections::HashMap;

        let fk_map: HashMap<String, ForeignKeyRef> = schema
            .foreign_keys
            .iter()
            .map(|fk| {
                (
                    fk.column.clone(),
                    ForeignKeyRef {
                        table: fk.references_table.clone(),
                        column: fk.references_column.clone(),
                    },
                )
            })
            .collect();

        let columns: Vec<ColumnOutput> = schema
            .columns
            .into_iter()
            .map(|col| {
                let mut output: ColumnOutput = col.into();
                output.foreign_key = fk_map.get(&output.name).cloned();
                output
            })
            .collect();

        Self {
            table_name: schema.table_name,
            schema: schema.schema_name,
            columns,
            primary_key: schema.primary_key,
            foreign_keys: schema.foreign_keys.into_iter().map(Into::into).collect(),
            indexes: schema.indexes.into_iter().map(Into::into).collect(),
        }
    }
}

pub struct SchemaToolHandler {
    connection_manager: Arc<ConnectionManager>,
}

impl SchemaToolHandler {
    pub fn new(connection_manager: Arc<ConnectionManager>) -> Self {
        Self { connection_manager }
    }

    pub async fn list_tables(&self, input: ListTablesInput) -> DbResult<ListTablesOutput> {
        let config = self
            .connection_manager
            .get_config(&input.connection_id)
            .await?;

        // For server-level connections, database parameter is required
        if config.server_level && input.database.is_none() {
            return Err(DbError::invalid_input(
                "Server-level connections require a 'database' parameter to specify which database to query. \
                Use list_databases first to discover available databases, then call list_tables with database=<database_name>.",
            ));
        }

        let database = input.database.as_deref();
        let pool = self
            .connection_manager
            .get_pool_for_database(&input.connection_id, database)
            .await?;

        let result = SchemaInspector::list_tables(&pool, database, input.include_views).await;

        self.connection_manager
            .release_pool_for_database(&input.connection_id, database)
            .await;

        let tables = result?;
        let count = tables.len();

        info!(
            connection_id = %input.connection_id,
            count = count,
            "Listed tables"
        );

        Ok(ListTablesOutput {
            tables: tables.into_iter().map(Into::into).collect(),
            count,
        })
    }

    pub async fn describe_table(&self, input: DescribeTableInput) -> DbResult<DescribeTableOutput> {
        let config = self
            .connection_manager
            .get_config(&input.connection_id)
            .await?;

        // For server-level connections, database parameter is required
        if config.server_level && input.database.is_none() {
            return Err(DbError::invalid_input(
                "Server-level connections require a 'database' parameter to specify which database to query. \
                Use list_databases first to discover available databases, then call describe_table with database=<database_name>.",
            ));
        }

        let database = input.database.as_deref();
        let pool = self
            .connection_manager
            .get_pool_for_database(&input.connection_id, database)
            .await?;

        let result = SchemaInspector::describe_table(&pool, &input.table_name, database).await;

        self.connection_manager
            .release_pool_for_database(&input.connection_id, database)
            .await;

        let schema = result?;

        info!(
            connection_id = %input.connection_id,
            table = %input.table_name,
            columns = schema.columns.len(),
            "Described table"
        );

        Ok(schema.into())
    }

    /// SQLite returns an error as it doesn't support listing databases.
    pub async fn list_databases(&self, input: ListDatabasesInput) -> DbResult<ListDatabasesOutput> {
        let config = self
            .connection_manager
            .get_config(&input.connection_id)
            .await?;

        // For server-level connections, use a system database to list all databases
        let system_db = if config.server_level {
            match config.db_type {
                crate::models::DatabaseType::MySQL => Some("information_schema"),
                crate::models::DatabaseType::PostgreSQL => Some("postgres"),
                crate::models::DatabaseType::SQLite => None, // SQLite doesn't support list_databases
            }
        } else {
            None
        };

        let pool = self
            .connection_manager
            .get_pool_for_database(&input.connection_id, system_db)
            .await?;

        let result = SchemaInspector::list_databases(&pool).await;

        self.connection_manager
            .release_pool_for_database(&input.connection_id, system_db)
            .await;

        let db_rows = result?;
        let count = db_rows.len();

        let databases: Vec<DatabaseInfo> = db_rows
            .into_iter()
            .map(|row| DatabaseInfo {
                name: row.name,
                size_bytes: row.size_bytes,
                size_formatted: row.size_bytes.map(format_size),
                owner: row.owner,
                encoding: row.encoding,
                collation: row.collation,
            })
            .collect();

        info!(
            connection_id = %input.connection_id,
            count = count,
            "Listed databases"
        );

        Ok(ListDatabasesOutput { databases, count })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_size_bytes() {
        assert_eq!(format_size(0), "0 B");
        assert_eq!(format_size(512), "512 B");
        assert_eq!(format_size(1023), "1023 B");
    }

    #[test]
    fn test_format_size_kilobytes() {
        assert_eq!(format_size(1024), "1 kB");
        assert_eq!(format_size(16384), "16 kB");
        assert_eq!(format_size(1024 * 1023), "1023 kB");
    }

    #[test]
    fn test_format_size_megabytes() {
        assert_eq!(format_size(1024 * 1024), "1 MB");
        assert_eq!(format_size(1024 * 1024 + 512 * 1024), "1.50 MB");
        assert_eq!(format_size(256 * 1024 * 1024), "256 MB");
    }

    #[test]
    fn test_format_size_gigabytes() {
        assert_eq!(format_size(1024 * 1024 * 1024), "1 GB");
        assert_eq!(
            format_size(1024 * 1024 * 1024 + 512 * 1024 * 1024),
            "1.50 GB"
        );
        assert_eq!(format_size(4 * 1024 * 1024 * 1024), "4 GB");
    }

    #[test]
    fn test_format_size_terabytes() {
        assert_eq!(format_size(1024_u64 * 1024 * 1024 * 1024), "1 TB");
        assert_eq!(
            format_size(2_u64 * 1024 * 1024 * 1024 * 1024 + 512 * 1024 * 1024 * 1024),
            "2.50 TB"
        );
    }

    #[test]
    fn test_format_size_petabytes() {
        assert_eq!(format_size(1024_u64 * 1024 * 1024 * 1024 * 1024), "1 PB");
        assert_eq!(
            format_size(2_u64 * 1024 * 1024 * 1024 * 1024 * 1024),
            "2 PB"
        );
    }

    #[test]
    fn test_list_tables_input_defaults() {
        let json = r#"{"connection_id": "conn1"}"#;
        let input: ListTablesInput = serde_json::from_str(json).unwrap();

        assert!(input.include_views);
        assert!(input.database.is_none());
    }

    #[test]
    fn test_describe_table_input() {
        let json = r#"{
            "connection_id": "conn1",
            "table_name": "users",
            "database": "public"
        }"#;

        let input: DescribeTableInput = serde_json::from_str(json).unwrap();
        assert_eq!(input.table_name, "users");
        assert_eq!(input.database, Some("public".to_string()));
    }

    #[test]
    fn test_output_serialization() {
        let output = ListTablesOutput {
            tables: vec![TableInfoOutput {
                name: "users".to_string(),
                table_type: "table".to_string(),
                schema: Some("public".to_string()),
                engine: None,
                charset: None,
                collation: None,
                data_size: None,
                data_size_formatted: None,
                index_size: None,
                index_size_formatted: None,
                total_size: None,
                total_size_formatted: None,
                size_bytes: None,
                row_count: None,
                estimated_row: None,
                comment: None,
                created_at: None,
                updated_at: None,
            }],
            count: 1,
        };

        let json = serde_json::to_string(&output).unwrap();
        assert!(json.contains("\"count\":1"));
        assert!(json.contains("\"name\":\"users\""));
    }

    #[test]
    fn test_table_info_output_with_metadata() {
        let output = TableInfoOutput {
            name: "users".to_string(),
            table_type: "table".to_string(),
            schema: Some("public".to_string()),
            engine: Some("InnoDB".to_string()),
            charset: Some("utf8mb4".to_string()),
            collation: Some("utf8mb4_unicode_ci".to_string()),
            data_size: Some(196608),
            data_size_formatted: Some("192 KB".to_string()),
            index_size: Some(16384),
            index_size_formatted: Some("16 KB".to_string()),
            total_size: Some(212992),
            total_size_formatted: Some("208 KB".to_string()),
            size_bytes: Some(212992),
            row_count: Some(15000),
            estimated_row: Some(15000),
            comment: Some("User accounts table".to_string()),
            created_at: Some("2025-06-15T10:30:00+00:00".to_string()),
            updated_at: Some("2026-01-01T08:45:00+00:00".to_string()),
        };

        let json = serde_json::to_string(&output).unwrap();
        assert!(json.contains("\"size_bytes\":212992"));
        assert!(json.contains("\"row_count\":15000"));
        assert!(json.contains("\"engine\":\"InnoDB\""));
        assert!(json.contains("\"charset\":\"utf8mb4\""));
        assert!(json.contains("\"collation\":\"utf8mb4_unicode_ci\""));
        assert!(json.contains("\"data_size\":196608"));
        assert!(json.contains("\"data_size_formatted\":\"192 KB\""));
        assert!(json.contains("\"index_size\":16384"));
        assert!(json.contains("\"total_size\":212992"));
        assert!(json.contains("\"total_size_formatted\":\"208 KB\""));
        assert!(json.contains("\"comment\":\"User accounts table\""));
        assert!(json.contains("\"created_at\":\"2025-06-15T10:30:00+00:00\""));
        assert!(json.contains("\"updated_at\":\"2026-01-01T08:45:00+00:00\""));
    }

    #[test]
    fn test_table_info_output_from_table_info() {
        use crate::models::TableType;
        use chrono::TimeZone;
        use chrono::Utc;

        let created = Utc.with_ymd_and_hms(2025, 6, 15, 10, 30, 0).unwrap();

        let table_info = TableInfo::new("orders", TableType::Table)
            .with_schema("public")
            .with_engine("InnoDB")
            .with_charset("utf8mb4")
            .with_collation("utf8mb4_unicode_ci")
            .with_data_size(196608)
            .with_index_size(16384)
            .with_total_size(212992)
            .with_row_count(125000)
            .with_comment("Order records")
            .with_created_at(created);

        let output: TableInfoOutput = table_info.into();

        assert_eq!(output.name, "orders");
        assert_eq!(output.table_type, "table");
        assert_eq!(output.schema, Some("public".to_string()));
        assert_eq!(output.engine, Some("InnoDB".to_string()));
        assert_eq!(output.charset, Some("utf8mb4".to_string()));
        assert_eq!(output.collation, Some("utf8mb4_unicode_ci".to_string()));
        assert_eq!(output.data_size, Some(196608));
        assert_eq!(output.data_size_formatted, Some("192 kB".to_string()));
        assert_eq!(output.index_size, Some(16384));
        assert_eq!(output.index_size_formatted, Some("16 kB".to_string()));
        assert_eq!(output.total_size, Some(212992));
        assert_eq!(output.total_size_formatted, Some("208 kB".to_string()));
        assert_eq!(output.size_bytes, Some(212992));
        assert_eq!(output.row_count, Some(125000));
        assert_eq!(output.estimated_row, Some(125000));
        assert_eq!(output.comment, Some("Order records".to_string()));
        assert!(output.created_at.is_some());
        assert!(output.updated_at.is_none());
    }

    #[test]
    fn test_table_info_output_optional_fields_skipped() {
        let output = TableInfoOutput {
            name: "view_summary".to_string(),
            table_type: "view".to_string(),
            schema: Some("public".to_string()),
            engine: None,
            charset: None,
            collation: None,
            data_size: None,
            data_size_formatted: None,
            index_size: None,
            index_size_formatted: None,
            total_size: None,
            total_size_formatted: None,
            size_bytes: None,
            row_count: None,
            estimated_row: None,
            comment: None,
            created_at: None,
            updated_at: None,
        };

        let json = serde_json::to_string(&output).unwrap();
        // Optional None fields should not appear in JSON
        assert!(!json.contains("size_bytes"));
        assert!(!json.contains("row_count"));
        assert!(!json.contains("created_at"));
        assert!(!json.contains("updated_at"));
        assert!(!json.contains("engine"));
        assert!(!json.contains("charset"));
        assert!(!json.contains("collation"));
        assert!(!json.contains("data_size"));
        assert!(!json.contains("index_size"));
        assert!(!json.contains("total_size"));
        assert!(!json.contains("comment"));
    }

    #[test]
    fn test_engine_field_serialization_mysql() {
        use crate::models::TableType;

        let table_info = TableInfo::new("users", TableType::Table).with_engine("InnoDB");

        let output: TableInfoOutput = table_info.into();
        let json = serde_json::to_string(&output).unwrap();

        assert!(json.contains("\"engine\":\"InnoDB\""));
    }

    #[test]
    fn test_engine_field_omitted_non_mysql() {
        use crate::models::TableType;

        // PostgreSQL/SQLite tables without engine
        let table_info = TableInfo::new("users", TableType::Table);

        let output: TableInfoOutput = table_info.into();
        let json = serde_json::to_string(&output).unwrap();

        assert!(!json.contains("engine"));
    }

    #[test]
    fn test_charset_collation_serialization_mysql() {
        use crate::models::TableType;

        let table_info = TableInfo::new("users", TableType::Table)
            .with_charset("utf8mb4")
            .with_collation("utf8mb4_unicode_ci");

        let output: TableInfoOutput = table_info.into();
        let json = serde_json::to_string(&output).unwrap();

        assert!(json.contains("\"charset\":\"utf8mb4\""));
        assert!(json.contains("\"collation\":\"utf8mb4_unicode_ci\""));
    }

    #[test]
    fn test_charset_collation_omitted_non_mysql() {
        use crate::models::TableType;

        // PostgreSQL/SQLite tables without charset/collation
        let table_info = TableInfo::new("users", TableType::Table);

        let output: TableInfoOutput = table_info.into();
        let json = serde_json::to_string(&output).unwrap();

        assert!(!json.contains("charset"));
        assert!(!json.contains("collation"));
    }

    #[test]
    fn test_size_breakdown_fields() {
        use crate::models::TableType;

        let table_info = TableInfo::new("users", TableType::Table)
            .with_data_size(1048576)
            .with_index_size(262144)
            .with_total_size(1310720);

        let output: TableInfoOutput = table_info.into();

        assert_eq!(output.data_size, Some(1048576));
        assert_eq!(output.data_size_formatted, Some("1 MB".to_string()));
        assert_eq!(output.index_size, Some(262144));
        assert_eq!(output.index_size_formatted, Some("256 kB".to_string()));
        assert_eq!(output.total_size, Some(1310720));
        assert_eq!(output.total_size_formatted, Some("1.25 MB".to_string()));
    }

    #[test]
    fn test_size_fields_omitted_for_views() {
        use crate::models::TableType;

        // Views don't have size info
        let table_info = TableInfo::new("user_summary", TableType::View);

        let output: TableInfoOutput = table_info.into();
        let json = serde_json::to_string(&output).unwrap();

        assert!(!json.contains("data_size"));
        assert!(!json.contains("index_size"));
        assert!(!json.contains("total_size"));
    }

    #[test]
    fn test_formatted_size_fields_present_when_raw_present() {
        use crate::models::TableType;

        let table_info = TableInfo::new("users", TableType::Table)
            .with_data_size(16384)
            .with_index_size(8192)
            .with_total_size(24576);

        let output: TableInfoOutput = table_info.into();

        assert!(output.data_size_formatted.is_some());
        assert!(output.index_size_formatted.is_some());
        assert!(output.total_size_formatted.is_some());
    }

    #[test]
    fn test_formatted_size_fields_omitted_when_raw_absent() {
        use crate::models::TableType;

        let table_info = TableInfo::new("users", TableType::Table);

        let output: TableInfoOutput = table_info.into();

        assert!(output.data_size_formatted.is_none());
        assert!(output.index_size_formatted.is_none());
        assert!(output.total_size_formatted.is_none());
    }

    #[test]
    fn test_comment_field_serialization() {
        use crate::models::TableType;

        let table_info =
            TableInfo::new("users", TableType::Table).with_comment("User accounts table");

        let output: TableInfoOutput = table_info.into();
        let json = serde_json::to_string(&output).unwrap();

        assert!(json.contains("\"comment\":\"User accounts table\""));
    }

    #[test]
    fn test_comment_field_omitted_when_absent() {
        use crate::models::TableType;

        let table_info = TableInfo::new("users", TableType::Table);

        let output: TableInfoOutput = table_info.into();
        let json = serde_json::to_string(&output).unwrap();

        assert!(!json.contains("comment"));
    }

    #[test]
    fn test_column_output_with_empty_default_value() {
        // Empty string default should be preserved in output
        let col = ColumnDefinition::new("client_id", "varchar(64)", false)
            .with_default(serde_json::Value::String("".to_string()));

        let output: ColumnOutput = col.into();
        let json = serde_json::to_string(&output).unwrap();

        // Empty string should be serialized as "default_value":""
        assert!(
            json.contains("\"default_value\":\"\""),
            "Empty string default should be present in JSON: {}",
            json
        );
    }

    #[test]
    fn test_column_output_without_default_value() {
        // No default should result in no default_value field
        let col = ColumnDefinition::new("id", "bigint", false);

        let output: ColumnOutput = col.into();
        let json = serde_json::to_string(&output).unwrap();

        // default_value should not appear
        assert!(
            !json.contains("default_value"),
            "default_value should not be present when None: {}",
            json
        );
    }
}
