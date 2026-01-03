//! Schema introspection module.
//!
//! This module provides database schema introspection functionality
//! for SQLite, PostgreSQL, and MySQL databases.
//!
//! # Architecture
//!
//! SQL queries are organized in the `queries` submodule with constants for each
//! database type. Database-specific implementations are in their respective
//! submodules (postgres, mysql, sqlite), each providing the same interface.

use crate::db::pool::DbPool;
use crate::error::{DbError, DbResult};
use crate::models::{
    ColumnDefinition, ForeignKey, ForeignKeyAction, IndexInfo, TableInfo, TableSchema, TableType,
};
use tracing::debug;

#[derive(Debug, Clone)]
pub struct DatabaseInfoRow {
    pub name: String,
    pub size_bytes: Option<u64>,
    pub owner: Option<String>,
    pub encoding: Option<String>,
    pub collation: Option<String>,
}

/// Schema inspector for database introspection.
pub struct SchemaInspector;

impl SchemaInspector {
    /// List all tables in the database.
    pub async fn list_tables(
        pool: &DbPool,
        schema: Option<&str>,
        include_views: bool,
    ) -> DbResult<Vec<TableInfo>> {
        match pool {
            DbPool::Postgres(p) => postgres::list_tables(p, schema, include_views).await,
            DbPool::MySql(p) => mysql::list_tables(p, schema, include_views).await,
            DbPool::SQLite(p) => sqlite::list_tables(p, include_views).await,
        }
    }

    /// Describe a table's schema.
    pub async fn describe_table(
        pool: &DbPool,
        table_name: &str,
        schema: Option<&str>,
    ) -> DbResult<TableSchema> {
        match pool {
            DbPool::Postgres(p) => postgres::describe_table(p, table_name, schema).await,
            DbPool::MySql(p) => mysql::describe_table(p, table_name, schema).await,
            DbPool::SQLite(p) => sqlite::describe_table(p, table_name).await,
        }
    }

    /// List all databases on the server.
    /// Supported for MySQL and PostgreSQL. SQLite returns an error (file-based).
    pub async fn list_databases(pool: &DbPool) -> DbResult<Vec<DatabaseInfoRow>> {
        match pool {
            DbPool::Postgres(p) => postgres::list_databases(p).await,
            DbPool::MySql(p) => mysql::list_databases(p).await,
            DbPool::SQLite(_) => Err(DbError::invalid_input(
                "SQLite does not support listing databases. SQLite is file-based; each file is a database.",
            )),
        }
    }
}

// =============================================================================
// SQL Query Templates
// =============================================================================
//
// Centralized SQL queries for schema introspection. Each database has its own
// submodule with queries adapted to its specific system catalogs.

mod queries {
    pub mod postgres {
        pub const LIST_DATABASES: &str = r#"
            SELECT
                datname AS name,
                pg_database_size(datname) AS size_bytes,
                pg_catalog.pg_get_userbyid(datdba) AS owner,
                pg_encoding_to_char(encoding) AS encoding,
                datcollate AS collation
            FROM pg_database
            WHERE datistemplate = false
            ORDER BY datname
            "#;

        pub const LIST_TABLES_WITH_VIEWS: &str = r#"
            SELECT
                t.table_name,
                t.table_type,
                CASE
                    WHEN t.table_type = 'BASE TABLE' THEN pg_relation_size(quote_ident($1) || '.' || quote_ident(t.table_name))
                    ELSE NULL
                END as data_size,
                CASE
                    WHEN t.table_type = 'BASE TABLE' THEN pg_indexes_size(quote_ident($1) || '.' || quote_ident(t.table_name))
                    ELSE NULL
                END as index_size,
                CASE
                    WHEN t.table_type = 'BASE TABLE' THEN pg_total_relation_size(quote_ident($1) || '.' || quote_ident(t.table_name))
                    ELSE NULL
                END as total_size,
                s.n_live_tup as row_count,
                GREATEST(s.last_vacuum, s.last_autovacuum, s.last_analyze, s.last_autoanalyze) as updated_at,
                obj_description((quote_ident($1) || '.' || quote_ident(t.table_name))::regclass) as comment
            FROM information_schema.tables t
            LEFT JOIN pg_stat_user_tables s
                ON s.schemaname = t.table_schema AND s.relname = t.table_name
            WHERE t.table_schema = $1
            AND t.table_type IN ('BASE TABLE', 'VIEW')
            ORDER BY t.table_name
            "#;

        pub const LIST_TABLES_NO_VIEWS: &str = r#"
            SELECT
                t.table_name,
                t.table_type,
                pg_relation_size(quote_ident($1) || '.' || quote_ident(t.table_name)) as data_size,
                pg_indexes_size(quote_ident($1) || '.' || quote_ident(t.table_name)) as index_size,
                pg_total_relation_size(quote_ident($1) || '.' || quote_ident(t.table_name)) as total_size,
                s.n_live_tup as row_count,
                GREATEST(s.last_vacuum, s.last_autovacuum, s.last_analyze, s.last_autoanalyze) as updated_at,
                obj_description((quote_ident($1) || '.' || quote_ident(t.table_name))::regclass) as comment
            FROM information_schema.tables t
            LEFT JOIN pg_stat_user_tables s
                ON s.schemaname = t.table_schema AND s.relname = t.table_name
            WHERE t.table_schema = $1
            AND t.table_type = 'BASE TABLE'
            ORDER BY t.table_name
            "#;

        pub const DESCRIBE_COLUMNS: &str = r#"
        SELECT
            c.column_name,
            format_type(a.atttypid, a.atttypmod) as column_type,
            c.is_nullable,
            c.column_default,
            CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_primary_key,
            col_description(t.oid, a.attnum) as column_comment,
            (SELECT collname FROM pg_collation WHERE oid = a.attcollation) as collation_name
        FROM information_schema.columns c
        JOIN pg_class t ON t.relname = c.table_name
        JOIN pg_namespace n ON n.oid = t.relnamespace AND n.nspname = c.table_schema
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attname = c.column_name
        LEFT JOIN (
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.table_name = $1
            AND tc.table_schema = $2
            AND tc.constraint_type = 'PRIMARY KEY'
        ) pk ON c.column_name = pk.column_name
        WHERE c.table_name = $1 AND c.table_schema = $2
        ORDER BY c.ordinal_position
        "#;

        pub const DESCRIBE_FOREIGN_KEYS: &str = r#"
        SELECT
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name,
            rc.delete_rule,
            rc.update_rule
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        JOIN information_schema.referential_constraints rc
            ON rc.constraint_name = tc.constraint_name
            AND rc.constraint_schema = tc.table_schema
        WHERE tc.table_name = $1
        AND tc.table_schema = $2
        AND tc.constraint_type = 'FOREIGN KEY'
        "#;

        pub const DESCRIBE_INDEXES: &str = r#"
        SELECT
            i.relname as index_name,
            array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)) as column_names,
            ix.indisunique as is_unique,
            ix.indisprimary as is_primary,
            am.amname as index_algorithm
        FROM pg_index ix
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_class t ON t.oid = ix.indrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        LEFT JOIN pg_am am ON am.oid = i.relam
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
        WHERE t.relname = $1 AND n.nspname = $2
        GROUP BY i.relname, ix.indisunique, ix.indisprimary, am.amname
        "#;
    }

    pub mod mysql {
        pub const LIST_DATABASES: &str = r#"SHOW DATABASES"#;

        pub const LIST_TABLES_WITH_VIEWS: &str = r#"
            SELECT
                CONVERT(TABLE_NAME USING utf8) AS TABLE_NAME,
                CONVERT(TABLE_TYPE USING utf8) AS TABLE_TYPE,
                CONVERT(ENGINE USING utf8) AS ENGINE,
                CONVERT(TABLE_COLLATION USING utf8) AS TABLE_COLLATION,
                DATA_LENGTH as DATA_SIZE,
                INDEX_LENGTH as INDEX_SIZE,
                CAST(DATA_LENGTH + COALESCE(INDEX_LENGTH, 0) AS UNSIGNED) as TOTAL_SIZE,
                TABLE_ROWS as ROW_COUNT,
                CREATE_TIME as CREATED_AT,
                UPDATE_TIME as UPDATED_AT,
                CONVERT(TABLE_COMMENT USING utf8) AS TABLE_COMMENT
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = COALESCE(?, DATABASE())
            AND TABLE_TYPE IN ('BASE TABLE', 'VIEW')
            ORDER BY TABLE_NAME
            "#;

        pub const LIST_TABLES_NO_VIEWS: &str = r#"
            SELECT
                CONVERT(TABLE_NAME USING utf8) AS TABLE_NAME,
                CONVERT(TABLE_TYPE USING utf8) AS TABLE_TYPE,
                CONVERT(ENGINE USING utf8) AS ENGINE,
                CONVERT(TABLE_COLLATION USING utf8) AS TABLE_COLLATION,
                DATA_LENGTH as DATA_SIZE,
                INDEX_LENGTH as INDEX_SIZE,
                CAST(DATA_LENGTH + COALESCE(INDEX_LENGTH, 0) AS UNSIGNED) as TOTAL_SIZE,
                TABLE_ROWS as ROW_COUNT,
                CREATE_TIME as CREATED_AT,
                UPDATE_TIME as UPDATED_AT,
                CONVERT(TABLE_COMMENT USING utf8) AS TABLE_COMMENT
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = COALESCE(?, DATABASE())
            AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
            "#;

        pub const DESCRIBE_COLUMNS: &str = r#"
        SELECT
            CONVERT(COLUMN_NAME USING utf8) AS COLUMN_NAME,
            CONVERT(COLUMN_TYPE USING utf8) AS COLUMN_TYPE,
            CONVERT(IS_NULLABLE USING utf8) AS IS_NULLABLE,
            CONVERT(COLUMN_DEFAULT USING utf8) AS COLUMN_DEFAULT,
            CONVERT(COLUMN_KEY USING utf8) AS COLUMN_KEY,
            CONVERT(EXTRA USING utf8) AS EXTRA,
            CONVERT(CHARACTER_SET_NAME USING utf8) AS CHARACTER_SET_NAME,
            CONVERT(COLLATION_NAME USING utf8) AS COLLATION_NAME,
            CONVERT(COLUMN_COMMENT USING utf8) AS COLUMN_COMMENT
        FROM information_schema.columns
        WHERE TABLE_NAME = ? AND TABLE_SCHEMA = COALESCE(?, DATABASE())
        ORDER BY ORDINAL_POSITION
        "#;

        pub const DESCRIBE_FOREIGN_KEYS: &str = r#"
        SELECT
            CONVERT(COLUMN_NAME USING utf8) AS COLUMN_NAME,
            CONVERT(REFERENCED_TABLE_NAME USING utf8) AS REFERENCED_TABLE_NAME,
            CONVERT(REFERENCED_COLUMN_NAME USING utf8) AS REFERENCED_COLUMN_NAME
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE TABLE_NAME = ?
        AND TABLE_SCHEMA = COALESCE(?, DATABASE())
        AND REFERENCED_TABLE_NAME IS NOT NULL
        "#;

        pub const DESCRIBE_INDEXES: &str = r#"
        SELECT
            CONVERT(INDEX_NAME USING utf8) AS INDEX_NAME,
            CONVERT(GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) USING utf8) as COLUMN_NAMES,
            NOT NON_UNIQUE as IS_UNIQUE,
            CONVERT(INDEX_TYPE USING utf8) AS INDEX_ALGORITHM
        FROM information_schema.STATISTICS
        WHERE TABLE_NAME = ? AND TABLE_SCHEMA = COALESCE(?, DATABASE())
        GROUP BY INDEX_NAME, NON_UNIQUE, INDEX_TYPE
        "#;
    }

    pub mod sqlite {
        pub const LIST_TABLES_WITH_VIEWS: &str = r#"
            SELECT name, type FROM sqlite_master
            WHERE type IN ('table', 'view')
            AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            "#;

        pub const LIST_TABLES_NO_VIEWS: &str = r#"
            SELECT name, type FROM sqlite_master
            WHERE type = 'table'
            AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            "#;

        pub const TABLE_SIZE: &str = "SELECT SUM(pgsize) as size_bytes FROM dbstat WHERE name = ?";
    }
}

// =============================================================================
// Database-Specific Implementations
// =============================================================================

mod postgres {
    use super::*;
    use chrono::{DateTime, Utc};
    use sqlx::{PgPool, Row};

    pub async fn list_tables(
        pool: &PgPool,
        schema: Option<&str>,
        include_views: bool,
    ) -> DbResult<Vec<TableInfo>> {
        let schema_name = schema.unwrap_or("public");
        let query = if include_views {
            queries::postgres::LIST_TABLES_WITH_VIEWS
        } else {
            queries::postgres::LIST_TABLES_NO_VIEWS
        };

        let rows = sqlx::query(query).bind(schema_name).fetch_all(pool).await?;

        let tables = rows
            .iter()
            .filter_map(|row| {
                let name: String = row.get("table_name");
                if name.is_empty() {
                    return None;
                }

                let type_str: String = row.get("table_type");
                let mut table =
                    TableInfo::new(&name, TableType::parse(&type_str)).with_schema(schema_name);

                if type_str == "BASE TABLE" {
                    if let Ok(data_size) = row.try_get::<i64, _>("data_size") {
                        table = table.with_data_size(data_size as u64);
                    }
                    if let Ok(index_size) = row.try_get::<i64, _>("index_size") {
                        table = table.with_index_size(index_size as u64);
                    }
                    if let Ok(total_size) = row.try_get::<i64, _>("total_size") {
                        table = table.with_total_size(total_size as u64);
                    }
                    if let Ok(count) = row.try_get::<i64, _>("row_count") {
                        table = table.with_row_count(count as u64);
                    }
                    if let Ok(Some(updated)) = row.try_get::<Option<DateTime<Utc>>, _>("updated_at")
                    {
                        table = table.with_updated_at(updated);
                    }
                }

                if let Ok(Some(comment)) = row.try_get::<Option<String>, _>("comment") {
                    if !comment.is_empty() {
                        table = table.with_comment(comment);
                    }
                }

                Some(table)
            })
            .collect::<Vec<_>>();

        debug!(
            count = tables.len(),
            schema = schema_name,
            "Listed PostgreSQL tables"
        );
        Ok(tables)
    }

    pub async fn list_databases(pool: &PgPool) -> DbResult<Vec<DatabaseInfoRow>> {
        let rows = sqlx::query(queries::postgres::LIST_DATABASES)
            .fetch_all(pool)
            .await?;

        let databases = rows
            .iter()
            .map(|row| {
                let name: String = row.get("name");
                let size_bytes: Option<i64> = row.try_get("size_bytes").ok();
                let owner: Option<String> = row.try_get("owner").ok().flatten();
                let encoding: Option<String> = row.try_get("encoding").ok().flatten();
                let collation: Option<String> = row.try_get("collation").ok().flatten();

                DatabaseInfoRow {
                    name,
                    size_bytes: size_bytes.map(|s| s as u64),
                    owner,
                    encoding,
                    collation,
                }
            })
            .collect::<Vec<_>>();

        debug!(count = databases.len(), "Listed PostgreSQL databases");
        Ok(databases)
    }

    pub async fn describe_table(
        pool: &PgPool,
        table_name: &str,
        schema: Option<&str>,
    ) -> DbResult<TableSchema> {
        let schema_name = schema.unwrap_or("public");

        let columns = fetch_columns(pool, table_name, schema_name).await?;
        if columns.is_empty() {
            return Err(DbError::schema(
                format!("Table '{}' not found", table_name),
                table_name.to_string(),
            ));
        }

        let primary_key = columns
            .iter()
            .filter(|c| c.is_primary_key)
            .map(|c| c.name.clone())
            .collect();

        let foreign_keys = fetch_foreign_keys(pool, table_name, schema_name).await?;
        let indexes = fetch_indexes(pool, table_name, schema_name).await;

        Ok(TableSchema {
            table_name: table_name.to_string(),
            schema_name: Some(schema_name.to_string()),
            columns,
            primary_key,
            foreign_keys,
            indexes,
        })
    }

    async fn fetch_columns(
        pool: &PgPool,
        table_name: &str,
        schema_name: &str,
    ) -> DbResult<Vec<ColumnDefinition>> {
        let rows = sqlx::query(queries::postgres::DESCRIBE_COLUMNS)
            .bind(table_name)
            .bind(schema_name)
            .fetch_all(pool)
            .await?;

        Ok(rows
            .iter()
            .map(|row| {
                let name: String = row.get("column_name");
                let column_type: String = row.get("column_type");
                let nullable: String = row.get("is_nullable");
                let default_value: Option<String> = row.try_get("column_default").ok().flatten();
                let is_pk: bool = row.get("is_primary_key");
                let comment: Option<String> = row.get("column_comment");
                let collation: Option<String> = row.get("collation_name");

                let mut col = ColumnDefinition::new(&name, &column_type, nullable == "YES")
                    .with_primary_key(is_pk);

                if let Some(ref def) = default_value {
                    col = col.with_default_str(def);
                }
                if let Some(ref c) = comment {
                    if !c.is_empty() {
                        col = col.with_comment(c);
                    }
                }
                if let Some(ref coll) = collation {
                    if !coll.is_empty() {
                        col = col.with_collation(coll);
                    }
                }
                col
            })
            .collect())
    }

    async fn fetch_foreign_keys(
        pool: &PgPool,
        table_name: &str,
        schema_name: &str,
    ) -> DbResult<Vec<ForeignKey>> {
        let rows = sqlx::query(queries::postgres::DESCRIBE_FOREIGN_KEYS)
            .bind(table_name)
            .bind(schema_name)
            .fetch_all(pool)
            .await?;

        Ok(rows
            .iter()
            .map(|row| {
                let column: String = row.get("column_name");
                let ref_table: String = row.get("foreign_table_name");
                let ref_column: String = row.get("foreign_column_name");
                let delete_rule: String = row.get("delete_rule");
                let update_rule: String = row.get("update_rule");

                ForeignKey::new(column, ref_table, ref_column)
                    .with_on_delete(ForeignKeyAction::parse(&delete_rule))
                    .with_on_update(ForeignKeyAction::parse(&update_rule))
            })
            .collect())
    }

    async fn fetch_indexes(pool: &PgPool, table_name: &str, schema_name: &str) -> Vec<IndexInfo> {
        let rows = sqlx::query(queries::postgres::DESCRIBE_INDEXES)
            .bind(table_name)
            .bind(schema_name)
            .fetch_all(pool)
            .await
            .unwrap_or_default();

        rows.iter()
            .filter_map(|row| {
                let name: String = row.get("index_name");
                let columns: Vec<String> = row.get("column_names");
                let is_unique: bool = row.get("is_unique");
                let is_primary: bool = row.get("is_primary");
                let index_algorithm: Option<String> = row.get("index_algorithm");

                if columns.is_empty() {
                    None
                } else {
                    let mut idx = IndexInfo::new(name, columns)
                        .with_unique(is_unique)
                        .with_primary(is_primary);

                    if let Some(ref algo) = index_algorithm {
                        if !algo.is_empty() {
                            idx = idx.with_algorithm(algo);
                        }
                    }
                    Some(idx)
                }
            })
            .collect()
    }
}

mod mysql {
    use super::*;
    use chrono::NaiveDateTime;
    use sqlx::{MySqlPool, Row};

    /// Derive charset from collation (prefix before first underscore).
    /// e.g., "utf8mb4_unicode_ci" -> "utf8mb4"
    fn derive_charset_from_collation(collation: &str) -> String {
        collation.split('_').next().unwrap_or(collation).to_string()
    }

    /// Try to get a u64 value from a row, handling MySQL version differences.
    /// MySQL 5.x may return BIGINT (i64), MySQL 8.x returns BIGINT UNSIGNED (u64).
    fn try_get_u64(row: &sqlx::mysql::MySqlRow, column: &str) -> Option<u64> {
        // Try u64 first (MySQL 8.x / BIGINT UNSIGNED)
        if let Ok(Some(v)) = row.try_get::<Option<u64>, _>(column) {
            return Some(v);
        }
        // Fallback to i64 (MySQL 5.x / MariaDB / BIGINT)
        if let Ok(Some(v)) = row.try_get::<Option<i64>, _>(column) {
            return Some(v as u64);
        }
        None
    }

    /// Safely get a string from a MySQL row.
    /// MySQL may return VARBINARY instead of VARCHAR depending on charset configuration.
    fn get_string(row: &sqlx::mysql::MySqlRow, column: &str) -> String {
        row.try_get::<String, _>(column)
            .ok()
            .or_else(|| {
                row.try_get::<Vec<u8>, _>(column)
                    .ok()
                    .and_then(|bytes| String::from_utf8(bytes).ok())
            })
            .unwrap_or_default()
    }

    /// Safely get an optional string from a MySQL row.
    fn get_optional_string(row: &sqlx::mysql::MySqlRow, column: &str) -> Option<String> {
        row.try_get::<Option<String>, _>(column)
            .ok()
            .flatten()
            .or_else(|| {
                row.try_get::<Option<Vec<u8>>, _>(column)
                    .ok()
                    .flatten()
                    .and_then(|bytes| String::from_utf8(bytes).ok())
            })
    }

    /// Safely get a string from a MySQL row by index.
    fn get_string_by_index(row: &sqlx::mysql::MySqlRow, index: usize) -> Option<String> {
        row.try_get::<String, _>(index).ok().or_else(|| {
            row.try_get::<Vec<u8>, _>(index)
                .ok()
                .and_then(|bytes| String::from_utf8(bytes).ok())
        })
    }

    pub async fn list_databases(pool: &MySqlPool) -> DbResult<Vec<DatabaseInfoRow>> {
        let rows = sqlx::query(queries::mysql::LIST_DATABASES)
            .fetch_all(pool)
            .await?;

        let databases = rows
            .iter()
            .filter_map(|row| {
                // SHOW DATABASES returns a single column "Database"
                get_string_by_index(row, 0).map(|name| DatabaseInfoRow {
                    name,
                    size_bytes: None,
                    owner: None,
                    encoding: None,
                    collation: None,
                })
            })
            .collect::<Vec<_>>();

        debug!(count = databases.len(), "Listed MySQL databases");
        Ok(databases)
    }

    pub async fn list_tables(
        pool: &MySqlPool,
        schema: Option<&str>,
        include_views: bool,
    ) -> DbResult<Vec<TableInfo>> {
        let query = if include_views {
            queries::mysql::LIST_TABLES_WITH_VIEWS
        } else {
            queries::mysql::LIST_TABLES_NO_VIEWS
        };

        let rows = sqlx::query(query).bind(schema).fetch_all(pool).await?;

        let tables = rows
            .iter()
            .filter_map(|row| {
                let name = get_string(row, "TABLE_NAME");
                if name.is_empty() {
                    return None;
                }

                let type_str = get_string(row, "TABLE_TYPE");
                let mut table = TableInfo::new(&name, TableType::parse(&type_str));
                if let Some(s) = schema {
                    table = table.with_schema(s);
                }

                if type_str == "BASE TABLE" {
                    // Storage engine (MySQL-specific)
                    if let Some(engine) = get_optional_string(row, "ENGINE") {
                        if !engine.is_empty() {
                            table = table.with_engine(engine);
                        }
                    }

                    // Collation and derived charset (MySQL-specific)
                    if let Some(collation) = get_optional_string(row, "TABLE_COLLATION") {
                        if !collation.is_empty() {
                            let charset = derive_charset_from_collation(&collation);
                            table = table.with_charset(charset);
                            table = table.with_collation(collation);
                        }
                    }

                    // Size breakdown (handles MySQL version differences)
                    if let Some(data_size) = try_get_u64(row, "DATA_SIZE") {
                        table = table.with_data_size(data_size);
                    }
                    if let Some(index_size) = try_get_u64(row, "INDEX_SIZE") {
                        table = table.with_index_size(index_size);
                    }
                    if let Some(total_size) = try_get_u64(row, "TOTAL_SIZE") {
                        table = table.with_total_size(total_size);
                    }

                    // Row count (handles MySQL version differences)
                    if let Some(count) = try_get_u64(row, "ROW_COUNT") {
                        table = table.with_row_count(count);
                    }

                    if let Ok(Some(created)) = row.try_get::<Option<NaiveDateTime>, _>("CREATED_AT")
                    {
                        table = table.with_created_at(created.and_utc());
                    }
                    if let Ok(Some(updated)) = row.try_get::<Option<NaiveDateTime>, _>("UPDATED_AT")
                    {
                        table = table.with_updated_at(updated.and_utc());
                    }

                    if let Some(comment) = get_optional_string(row, "TABLE_COMMENT") {
                        if !comment.is_empty() {
                            table = table.with_comment(comment);
                        }
                    }
                }

                Some(table)
            })
            .collect::<Vec<_>>();

        debug!(count = tables.len(), "Listed MySQL tables");
        Ok(tables)
    }

    pub async fn describe_table(
        pool: &MySqlPool,
        table_name: &str,
        schema: Option<&str>,
    ) -> DbResult<TableSchema> {
        let columns = fetch_columns(pool, table_name, schema).await?;
        if columns.is_empty() {
            return Err(DbError::schema(
                format!("Table '{}' not found", table_name),
                table_name.to_string(),
            ));
        }

        let primary_key = columns
            .iter()
            .filter(|c| c.is_primary_key)
            .map(|c| c.name.clone())
            .collect();

        let foreign_keys = fetch_foreign_keys(pool, table_name, schema).await?;
        let indexes = fetch_indexes(pool, table_name, schema).await;

        Ok(TableSchema {
            table_name: table_name.to_string(),
            schema_name: schema.map(|s| s.to_string()),
            columns,
            primary_key,
            foreign_keys,
            indexes,
        })
    }

    async fn fetch_columns(
        pool: &MySqlPool,
        table_name: &str,
        schema: Option<&str>,
    ) -> DbResult<Vec<ColumnDefinition>> {
        let rows = sqlx::query(queries::mysql::DESCRIBE_COLUMNS)
            .bind(table_name)
            .bind(schema)
            .fetch_all(pool)
            .await?;

        Ok(rows
            .iter()
            .map(|row| {
                let name = get_string(row, "COLUMN_NAME");
                let column_type = get_string(row, "COLUMN_TYPE");
                let nullable = get_string(row, "IS_NULLABLE");
                let default_value = get_optional_string(row, "COLUMN_DEFAULT");
                let column_key = get_string(row, "COLUMN_KEY");
                let extra = get_optional_string(row, "EXTRA");
                let character_set = get_optional_string(row, "CHARACTER_SET_NAME");
                let collation = get_optional_string(row, "COLLATION_NAME");
                let comment = get_optional_string(row, "COLUMN_COMMENT");
                let is_pk = column_key == "PRI";

                let mut col = ColumnDefinition::new(&name, &column_type, nullable == "YES")
                    .with_primary_key(is_pk);

                if let Some(ref def) = default_value {
                    col = col.with_default_str(def);
                }
                if let Some(ref e) = extra {
                    if !e.is_empty() {
                        col = col.with_extra(e);
                    }
                }
                if let Some(ref cs) = character_set {
                    if !cs.is_empty() {
                        col = col.with_character_set(cs);
                    }
                }
                if let Some(ref coll) = collation {
                    if !coll.is_empty() {
                        col = col.with_collation(coll);
                    }
                }
                if let Some(ref c) = comment {
                    if !c.is_empty() {
                        col = col.with_comment(c);
                    }
                }
                col
            })
            .collect())
    }

    async fn fetch_foreign_keys(
        pool: &MySqlPool,
        table_name: &str,
        schema: Option<&str>,
    ) -> DbResult<Vec<ForeignKey>> {
        let rows = sqlx::query(queries::mysql::DESCRIBE_FOREIGN_KEYS)
            .bind(table_name)
            .bind(schema)
            .fetch_all(pool)
            .await?;

        Ok(rows
            .iter()
            .map(|row| {
                let column = get_string(row, "COLUMN_NAME");
                let ref_table = get_string(row, "REFERENCED_TABLE_NAME");
                let ref_column = get_string(row, "REFERENCED_COLUMN_NAME");
                ForeignKey::new(column, ref_table, ref_column)
            })
            .collect())
    }

    async fn fetch_indexes(
        pool: &MySqlPool,
        table_name: &str,
        schema: Option<&str>,
    ) -> Vec<IndexInfo> {
        let rows = sqlx::query(queries::mysql::DESCRIBE_INDEXES)
            .bind(table_name)
            .bind(schema)
            .fetch_all(pool)
            .await
            .unwrap_or_default();

        rows.iter()
            .map(|row| {
                let name = get_string(row, "INDEX_NAME");
                let columns_str = get_string(row, "COLUMN_NAMES");
                let is_unique: i64 = row.try_get("IS_UNIQUE").unwrap_or(0);
                let index_algorithm = get_optional_string(row, "INDEX_ALGORITHM");
                let columns: Vec<String> = columns_str.split(',').map(|s| s.to_string()).collect();
                let is_primary = name == "PRIMARY";

                let mut idx = IndexInfo::new(name, columns)
                    .with_unique(is_unique != 0 || is_primary)
                    .with_primary(is_primary);

                if let Some(ref algo) = index_algorithm {
                    if !algo.is_empty() {
                        idx = idx.with_algorithm(algo);
                    }
                }
                idx
            })
            .collect()
    }
}

mod sqlite {
    use super::*;
    use sqlx::{Row, SqlitePool};

    pub async fn list_tables(pool: &SqlitePool, include_views: bool) -> DbResult<Vec<TableInfo>> {
        let query = if include_views {
            queries::sqlite::LIST_TABLES_WITH_VIEWS
        } else {
            queries::sqlite::LIST_TABLES_NO_VIEWS
        };

        let rows = sqlx::query(query).fetch_all(pool).await?;

        let mut tables = Vec::with_capacity(rows.len());
        for row in &rows {
            let name: String = row.get("name");
            let type_str: String = row.get("type");
            let mut table = TableInfo::new(&name, TableType::parse(&type_str));

            if type_str == "table" {
                if let Some(size) = fetch_table_size(pool, &name).await {
                    table = table.with_total_size(size);
                }
            }

            tables.push(table);
        }

        debug!(count = tables.len(), "Listed SQLite tables");
        Ok(tables)
    }

    async fn fetch_table_size(pool: &SqlitePool, table_name: &str) -> Option<u64> {
        sqlx::query(queries::sqlite::TABLE_SIZE)
            .bind(table_name)
            .fetch_one(pool)
            .await
            .ok()
            .and_then(|row| row.try_get::<i64, _>("size_bytes").ok())
            .map(|size| size as u64)
    }

    pub async fn describe_table(pool: &SqlitePool, table_name: &str) -> DbResult<TableSchema> {
        let columns = fetch_columns(pool, table_name).await?;
        if columns.is_empty() {
            return Err(DbError::schema(
                format!("Table '{}' not found", table_name),
                table_name.to_string(),
            ));
        }

        let primary_key = columns
            .iter()
            .filter(|c| c.is_primary_key)
            .map(|c| c.name.clone())
            .collect();

        let foreign_keys = fetch_foreign_keys(pool, table_name).await;
        let indexes = fetch_indexes(pool, table_name).await;

        Ok(TableSchema {
            table_name: table_name.to_string(),
            schema_name: None,
            columns,
            primary_key,
            foreign_keys,
            indexes,
        })
    }

    async fn fetch_columns(pool: &SqlitePool, table_name: &str) -> DbResult<Vec<ColumnDefinition>> {
        let pragma_query = format!("PRAGMA table_info('{}')", table_name);
        let rows = sqlx::query(&pragma_query).fetch_all(pool).await?;

        Ok(rows
            .iter()
            .map(|row| {
                let name: String = row.get("name");
                let data_type: String = row.get("type");
                let notnull: i32 = row.get("notnull");
                let default_value: Option<String> = row.try_get("dflt_value").ok().flatten();
                let pk: i32 = row.get("pk");
                let is_pk = pk > 0;

                let mut col =
                    ColumnDefinition::new(&name, &data_type, notnull == 0).with_primary_key(is_pk);

                if let Some(ref def) = default_value {
                    col = col.with_default_str(def);
                }
                col
            })
            .collect())
    }

    async fn fetch_foreign_keys(pool: &SqlitePool, table_name: &str) -> Vec<ForeignKey> {
        let fk_query = format!("PRAGMA foreign_key_list('{}')", table_name);
        let rows = sqlx::query(&fk_query)
            .fetch_all(pool)
            .await
            .unwrap_or_default();

        rows.iter()
            .map(|row| {
                let column: String = row.get("from");
                let ref_table: String = row.get("table");
                let ref_column: String = row.get("to");
                let on_delete: String = row.try_get("on_delete").unwrap_or_default();
                let on_update: String = row.try_get("on_update").unwrap_or_default();

                ForeignKey::new(column, ref_table, ref_column)
                    .with_on_delete(ForeignKeyAction::parse(&on_delete))
                    .with_on_update(ForeignKeyAction::parse(&on_update))
            })
            .collect()
    }

    async fn fetch_indexes(pool: &SqlitePool, table_name: &str) -> Vec<IndexInfo> {
        let idx_query = format!("PRAGMA index_list('{}')", table_name);
        let idx_list = sqlx::query(&idx_query)
            .fetch_all(pool)
            .await
            .unwrap_or_default();

        let mut indexes = Vec::new();
        for idx_row in &idx_list {
            let name: String = idx_row.get("name");
            let is_unique: i32 = idx_row.get("unique");
            let origin: String = idx_row.try_get("origin").unwrap_or_default();
            let is_primary = origin == "pk";

            let columns = fetch_index_columns(pool, &name).await;
            if !columns.is_empty() {
                indexes.push(
                    IndexInfo::new(name, columns)
                        .with_unique(is_unique != 0)
                        .with_primary(is_primary),
                );
            }
        }
        indexes
    }

    async fn fetch_index_columns(pool: &SqlitePool, index_name: &str) -> Vec<String> {
        let query = format!("PRAGMA index_info('{}')", index_name);
        sqlx::query(&query)
            .fetch_all(pool)
            .await
            .unwrap_or_default()
            .iter()
            .map(|row| row.get("name"))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_type_parsing() {
        assert_eq!(TableType::parse("BASE TABLE"), TableType::Table);
        assert_eq!(TableType::parse("VIEW"), TableType::View);
        assert_eq!(TableType::parse("table"), TableType::Table);
    }
}
