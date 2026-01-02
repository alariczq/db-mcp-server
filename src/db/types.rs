//! Database-agnostic type mappings.
//!
//! This module provides utilities for mapping between database-specific types
//! and our unified type system.
//!
//! # Architecture
//!
//! Type conversion uses a two-phase approach:
//! 1. `TypeCategory` classifies column types into logical categories
//! 2. Database-specific decoders handle the actual value extraction
//!
//! This design centralizes type classification logic while allowing
//! database-specific handling where needed.

use crate::db::DatabaseType;
use crate::models::ColumnMetadata;
use serde_json::Value as JsonValue;
use sqlx::mysql::{MySqlRow, MySqlTypeInfo, MySqlValueRef};
use sqlx::postgres::{PgRow, PgTypeInfo, PgValueRef};
use sqlx::sqlite::SqliteRow;
use sqlx::{Column, Decode, Row, Type, TypeInfo};

// =============================================================================
// Type Classification
// =============================================================================

/// Logical category for database column types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeCategory {
    Integer,
    Float,
    Decimal,
    Boolean,
    Text,
    Binary,
    Json,
    Uuid,
    Unknown,
}

/// Classify a database type name into a logical category.
pub fn categorize_type(type_name: &str, db: DatabaseType) -> TypeCategory {
    let lower = type_name.to_lowercase();

    // Decimal/Numeric - check first as it overlaps with "numeric" in float checks
    if lower.contains("decimal") || lower.contains("numeric") {
        // SQLite's NUMERIC is actually a float
        if db == DatabaseType::SQLite && lower == "numeric" {
            return TypeCategory::Float;
        }
        return TypeCategory::Decimal;
    }

    // Integer types
    if lower.contains("int") || lower.contains("serial") || lower.contains("tiny") {
        return TypeCategory::Integer;
    }

    // Boolean
    if lower == "bool" || lower == "boolean" {
        return TypeCategory::Boolean;
    }

    // Float types
    if lower.contains("float")
        || lower.contains("double")
        || lower == "real"
        || lower == "float4"
        || lower == "float8"
    {
        return TypeCategory::Float;
    }

    // JSON types
    if lower == "json" || lower == "jsonb" {
        return TypeCategory::Json;
    }

    // UUID (PostgreSQL)
    if lower == "uuid" {
        return TypeCategory::Uuid;
    }

    // Binary types
    if lower.contains("blob") || lower.contains("binary") || lower == "bytea" {
        return TypeCategory::Binary;
    }

    // Default to text for everything else (varchar, text, char, date, time, etc.)
    TypeCategory::Unknown
}

// =============================================================================
// Decimal Type Support
// =============================================================================

/// Wrapper type for raw DECIMAL/NUMERIC values as strings.
/// This preserves the exact database representation.
#[derive(Debug)]
pub struct RawDecimal(pub String);

impl Type<sqlx::MySql> for RawDecimal {
    fn type_info() -> MySqlTypeInfo {
        <String as Type<sqlx::MySql>>::type_info()
    }

    fn compatible(ty: &MySqlTypeInfo) -> bool {
        let name = ty.name().to_lowercase();
        name.contains("decimal") || name.contains("numeric")
    }
}

impl<'r> Decode<'r, sqlx::MySql> for RawDecimal {
    fn decode(value: MySqlValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let s = <&str as Decode<sqlx::MySql>>::decode(value)?;
        Ok(RawDecimal(s.to_string()))
    }
}

impl Type<sqlx::Postgres> for RawDecimal {
    fn type_info() -> PgTypeInfo {
        <String as Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &PgTypeInfo) -> bool {
        let name = ty.name().to_lowercase();
        name.contains("numeric") || name.contains("decimal")
    }
}

impl<'r> Decode<'r, sqlx::Postgres> for RawDecimal {
    fn decode(value: PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let s = <&str as Decode<sqlx::Postgres>>::decode(value)?;
        Ok(RawDecimal(s.to_string()))
    }
}

// =============================================================================
// Binary Encoding
// =============================================================================

/// Decode binary data to JSON value.
///
/// If `decode_binary` is true, attempts to decode as UTF-8 text first.
/// Falls back to base64 encoding if not valid UTF-8 or if `decode_binary` is false.
pub fn decode_binary_value(bytes: &[u8], decode_binary: bool) -> JsonValue {
    use base64::{Engine as _, engine::general_purpose::STANDARD};

    if decode_binary {
        match std::str::from_utf8(bytes) {
            Ok(s) => JsonValue::String(s.to_string()),
            Err(_) => JsonValue::String(STANDARD.encode(bytes)),
        }
    } else {
        JsonValue::String(STANDARD.encode(bytes))
    }
}

// =============================================================================
// Row to JSON Trait
// =============================================================================

/// Trait for converting database rows to JSON maps.
pub trait RowToJson {
    fn to_json_map(&self) -> serde_json::Map<String, JsonValue>;
    fn to_json_map_with_options(&self, decode_binary: bool) -> serde_json::Map<String, JsonValue>;
    fn get_column_metadata(&self) -> Vec<ColumnMetadata>;
}

impl RowToJson for MySqlRow {
    fn to_json_map(&self) -> serde_json::Map<String, JsonValue> {
        self.to_json_map_with_options(false)
    }

    fn to_json_map_with_options(&self, decode_binary: bool) -> serde_json::Map<String, JsonValue> {
        self.columns()
            .iter()
            .enumerate()
            .map(|(idx, col)| {
                let type_name = col.type_info().name();
                let category = categorize_type(type_name, DatabaseType::MySql);
                let value = mysql::decode_column(self, idx, type_name, category, decode_binary);
                (col.name().to_string(), value)
            })
            .collect()
    }

    fn get_column_metadata(&self) -> Vec<ColumnMetadata> {
        self.columns()
            .iter()
            .map(|col| {
                ColumnMetadata::new(
                    col.name(),
                    col.type_info().name(),
                    !col.type_info().is_null(),
                )
            })
            .collect()
    }
}

impl RowToJson for PgRow {
    fn to_json_map(&self) -> serde_json::Map<String, JsonValue> {
        self.to_json_map_with_options(false)
    }

    fn to_json_map_with_options(&self, decode_binary: bool) -> serde_json::Map<String, JsonValue> {
        self.columns()
            .iter()
            .enumerate()
            .map(|(idx, col)| {
                let type_name = col.type_info().name();
                let category = categorize_type(type_name, DatabaseType::Postgres);
                let value = postgres::decode_column(self, idx, type_name, category, decode_binary);
                (col.name().to_string(), value)
            })
            .collect()
    }

    fn get_column_metadata(&self) -> Vec<ColumnMetadata> {
        self.columns()
            .iter()
            .map(|col| {
                ColumnMetadata::new(
                    col.name(),
                    col.type_info().name(),
                    !col.type_info().is_null(),
                )
            })
            .collect()
    }
}

impl RowToJson for SqliteRow {
    fn to_json_map(&self) -> serde_json::Map<String, JsonValue> {
        self.to_json_map_with_options(false)
    }

    fn to_json_map_with_options(&self, decode_binary: bool) -> serde_json::Map<String, JsonValue> {
        self.columns()
            .iter()
            .enumerate()
            .map(|(idx, col)| {
                let type_name = col.type_info().name();
                let category = categorize_type(type_name, DatabaseType::SQLite);
                let value = sqlite::decode_column(self, idx, type_name, category, decode_binary);
                (col.name().to_string(), value)
            })
            .collect()
    }

    fn get_column_metadata(&self) -> Vec<ColumnMetadata> {
        self.columns()
            .iter()
            .map(|col| {
                ColumnMetadata::new(
                    col.name(),
                    col.type_info().name(),
                    !col.type_info().is_null(),
                )
            })
            .collect()
    }
}

// =============================================================================
// Database-Specific Decoders
// =============================================================================

mod mysql {
    use super::*;

    pub fn decode_column(
        row: &MySqlRow,
        idx: usize,
        type_name: &str,
        category: TypeCategory,
        decode_binary: bool,
    ) -> JsonValue {
        match category {
            TypeCategory::Decimal => decode_decimal(row, idx),
            TypeCategory::Integer => decode_integer(row, idx),
            TypeCategory::Boolean => decode_boolean(row, idx),
            TypeCategory::Float => decode_float(row, idx),
            TypeCategory::Binary => decode_binary_col(row, idx, decode_binary),
            TypeCategory::Json => decode_json_string(row, idx),
            _ => decode_text(row, idx, type_name),
        }
    }

    fn decode_decimal(row: &MySqlRow, idx: usize) -> JsonValue {
        match row.try_get::<Option<RawDecimal>, _>(idx) {
            Ok(Some(v)) => JsonValue::String(v.0),
            Ok(None) => JsonValue::Null,
            Err(e) => {
                tracing::error!("Failed to decode DECIMAL: {:?}", e);
                JsonValue::Null
            }
        }
    }

    fn decode_integer(row: &MySqlRow, idx: usize) -> JsonValue {
        // Check NULL first
        if let Ok(None) = row.try_get::<Option<i64>, _>(idx) {
            return JsonValue::Null;
        }
        // Try signed types
        if let Ok(Some(v)) = row.try_get::<Option<i8>, _>(idx) {
            return JsonValue::Number(v.into());
        }
        if let Ok(Some(v)) = row.try_get::<Option<i16>, _>(idx) {
            return JsonValue::Number(v.into());
        }
        if let Ok(Some(v)) = row.try_get::<Option<i32>, _>(idx) {
            return JsonValue::Number(v.into());
        }
        if let Ok(Some(v)) = row.try_get::<Option<i64>, _>(idx) {
            return JsonValue::Number(v.into());
        }
        // Try unsigned types
        if let Ok(Some(v)) = row.try_get::<Option<u8>, _>(idx) {
            return JsonValue::Number(v.into());
        }
        if let Ok(Some(v)) = row.try_get::<Option<u16>, _>(idx) {
            return JsonValue::Number(v.into());
        }
        if let Ok(Some(v)) = row.try_get::<Option<u32>, _>(idx) {
            return JsonValue::Number(v.into());
        }
        if let Ok(Some(v)) = row.try_get::<Option<u64>, _>(idx) {
            return JsonValue::Number(v.into());
        }
        JsonValue::Null
    }

    fn decode_boolean(row: &MySqlRow, idx: usize) -> JsonValue {
        row.try_get::<Option<bool>, _>(idx)
            .ok()
            .flatten()
            .map(JsonValue::Bool)
            .unwrap_or(JsonValue::Null)
    }

    fn decode_float(row: &MySqlRow, idx: usize) -> JsonValue {
        if let Ok(Some(v)) = row.try_get::<Option<f64>, _>(idx) {
            return serde_json::Number::from_f64(v)
                .map(JsonValue::Number)
                .unwrap_or_else(|| JsonValue::String(v.to_string()));
        }
        if let Ok(Some(v)) = row.try_get::<Option<f32>, _>(idx) {
            return serde_json::Number::from_f64(v as f64)
                .map(JsonValue::Number)
                .unwrap_or_else(|| JsonValue::String(v.to_string()));
        }
        JsonValue::Null
    }

    fn decode_binary_col(row: &MySqlRow, idx: usize, decode_binary: bool) -> JsonValue {
        row.try_get::<Option<Vec<u8>>, _>(idx)
            .ok()
            .flatten()
            .map(|v| decode_binary_value(&v, decode_binary))
            .unwrap_or(JsonValue::Null)
    }

    fn decode_json_string(row: &MySqlRow, idx: usize) -> JsonValue {
        // MySQL JSON type should be decoded as serde_json::Value directly
        row.try_get::<Option<serde_json::Value>, _>(idx)
            .ok()
            .flatten()
            .unwrap_or(JsonValue::Null)
    }

    fn decode_text(row: &MySqlRow, idx: usize, type_name: &str) -> JsonValue {
        if let Ok(Some(v)) = row.try_get::<Option<String>, _>(idx) {
            // Check if this might be JSON
            if type_name.to_lowercase().contains("json") {
                if let Ok(json) = serde_json::from_str::<JsonValue>(&v) {
                    return json;
                }
            }
            return JsonValue::String(v);
        }
        JsonValue::Null
    }
}

mod postgres {
    use super::*;

    pub fn decode_column(
        row: &PgRow,
        idx: usize,
        _type_name: &str,
        category: TypeCategory,
        decode_binary: bool,
    ) -> JsonValue {
        match category {
            TypeCategory::Decimal => decode_decimal(row, idx),
            TypeCategory::Integer => decode_integer(row, idx),
            TypeCategory::Boolean => decode_boolean(row, idx),
            TypeCategory::Float => decode_float(row, idx),
            TypeCategory::Binary => decode_binary_col(row, idx, decode_binary),
            TypeCategory::Json => decode_json(row, idx),
            TypeCategory::Uuid => decode_uuid(row, idx),
            _ => decode_text(row, idx),
        }
    }

    fn decode_decimal(row: &PgRow, idx: usize) -> JsonValue {
        match row.try_get::<Option<RawDecimal>, _>(idx) {
            Ok(Some(v)) => JsonValue::String(v.0),
            Ok(None) => JsonValue::Null,
            Err(e) => {
                tracing::error!("Failed to decode NUMERIC: {:?}", e);
                JsonValue::Null
            }
        }
    }

    fn decode_integer(row: &PgRow, idx: usize) -> JsonValue {
        if let Ok(None) = row.try_get::<Option<i64>, _>(idx) {
            return JsonValue::Null;
        }
        if let Ok(Some(v)) = row.try_get::<Option<i16>, _>(idx) {
            return JsonValue::Number(v.into());
        }
        if let Ok(Some(v)) = row.try_get::<Option<i32>, _>(idx) {
            return JsonValue::Number(v.into());
        }
        if let Ok(Some(v)) = row.try_get::<Option<i64>, _>(idx) {
            return JsonValue::Number(v.into());
        }
        JsonValue::Null
    }

    fn decode_boolean(row: &PgRow, idx: usize) -> JsonValue {
        row.try_get::<Option<bool>, _>(idx)
            .ok()
            .flatten()
            .map(JsonValue::Bool)
            .unwrap_or(JsonValue::Null)
    }

    fn decode_float(row: &PgRow, idx: usize) -> JsonValue {
        if let Ok(Some(v)) = row.try_get::<Option<f64>, _>(idx) {
            return serde_json::Number::from_f64(v)
                .map(JsonValue::Number)
                .unwrap_or_else(|| JsonValue::String(v.to_string()));
        }
        if let Ok(Some(v)) = row.try_get::<Option<f32>, _>(idx) {
            return serde_json::Number::from_f64(v as f64)
                .map(JsonValue::Number)
                .unwrap_or_else(|| JsonValue::String(v.to_string()));
        }
        JsonValue::Null
    }

    fn decode_binary_col(row: &PgRow, idx: usize, decode_binary: bool) -> JsonValue {
        row.try_get::<Option<Vec<u8>>, _>(idx)
            .ok()
            .flatten()
            .map(|v| decode_binary_value(&v, decode_binary))
            .unwrap_or(JsonValue::Null)
    }

    fn decode_json(row: &PgRow, idx: usize) -> JsonValue {
        row.try_get::<Option<serde_json::Value>, _>(idx)
            .ok()
            .flatten()
            .unwrap_or(JsonValue::Null)
    }

    fn decode_uuid(row: &PgRow, idx: usize) -> JsonValue {
        row.try_get::<Option<String>, _>(idx)
            .ok()
            .flatten()
            .map(JsonValue::String)
            .unwrap_or(JsonValue::Null)
    }

    fn decode_text(row: &PgRow, idx: usize) -> JsonValue {
        row.try_get::<Option<String>, _>(idx)
            .ok()
            .flatten()
            .map(JsonValue::String)
            .unwrap_or(JsonValue::Null)
    }
}

mod sqlite {
    use super::*;

    pub fn decode_column(
        row: &SqliteRow,
        idx: usize,
        type_name: &str,
        category: TypeCategory,
        decode_binary: bool,
    ) -> JsonValue {
        match category {
            TypeCategory::Integer => decode_integer(row, idx),
            TypeCategory::Boolean => decode_boolean(row, idx),
            TypeCategory::Float | TypeCategory::Decimal => decode_float(row, idx),
            TypeCategory::Binary => decode_binary_col(row, idx, decode_binary),
            _ => decode_text(row, idx, type_name),
        }
    }

    fn decode_integer(row: &SqliteRow, idx: usize) -> JsonValue {
        if let Ok(None) = row.try_get::<Option<i64>, _>(idx) {
            return JsonValue::Null;
        }
        if let Ok(Some(v)) = row.try_get::<Option<i64>, _>(idx) {
            return JsonValue::Number(v.into());
        }
        if let Ok(Some(v)) = row.try_get::<Option<i32>, _>(idx) {
            return JsonValue::Number(v.into());
        }
        JsonValue::Null
    }

    fn decode_boolean(row: &SqliteRow, idx: usize) -> JsonValue {
        row.try_get::<Option<bool>, _>(idx)
            .ok()
            .flatten()
            .map(JsonValue::Bool)
            .unwrap_or(JsonValue::Null)
    }

    fn decode_float(row: &SqliteRow, idx: usize) -> JsonValue {
        if let Ok(Some(v)) = row.try_get::<Option<f64>, _>(idx) {
            return serde_json::Number::from_f64(v)
                .map(JsonValue::Number)
                .unwrap_or_else(|| JsonValue::String(v.to_string()));
        }
        JsonValue::Null
    }

    fn decode_binary_col(row: &SqliteRow, idx: usize, decode_binary: bool) -> JsonValue {
        row.try_get::<Option<Vec<u8>>, _>(idx)
            .ok()
            .flatten()
            .map(|v| decode_binary_value(&v, decode_binary))
            .unwrap_or(JsonValue::Null)
    }

    fn decode_text(row: &SqliteRow, idx: usize, type_name: &str) -> JsonValue {
        if let Ok(Some(v)) = row.try_get::<Option<String>, _>(idx) {
            if type_name.to_lowercase().contains("json") {
                if let Ok(json) = serde_json::from_str::<JsonValue>(&v) {
                    return json;
                }
            }
            return JsonValue::String(v);
        }
        JsonValue::Null
    }
}

// =============================================================================
// Type Normalization
// =============================================================================

/// Normalize a database type name to a more consistent format.
pub fn normalize_type_name(type_name: &str) -> String {
    let lower = type_name.to_lowercase();

    match lower.as_str() {
        // Integers
        "int4" | "integer" | "int" => "integer".to_string(),
        "int8" | "bigint" | "bigserial" => "bigint".to_string(),
        "int2" | "smallint" => "smallint".to_string(),
        "tinyint" | "tiny" => "tinyint".to_string(),
        // Text
        "varchar" | "character varying" | "text" | "string" => "text".to_string(),
        "char" | "character" | "bpchar" => "char".to_string(),
        // Boolean
        "bool" | "boolean" => "boolean".to_string(),
        // Float
        "float4" | "real" | "float" => "real".to_string(),
        "float8" | "double precision" | "double" => "double".to_string(),
        // Binary
        "bytea" | "blob" | "binary" | "varbinary" => "binary".to_string(),
        // Date/Time
        "timestamp" | "timestamptz" | "datetime" => "timestamp".to_string(),
        "date" => "date".to_string(),
        "time" | "timetz" => "time".to_string(),
        // JSON
        "json" | "jsonb" => "json".to_string(),
        // UUID
        "uuid" => "uuid".to_string(),
        // Default
        _ => lower,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_categorize_type_integer() {
        assert_eq!(
            categorize_type("INT", DatabaseType::MySql),
            TypeCategory::Integer
        );
        assert_eq!(
            categorize_type("BIGINT", DatabaseType::Postgres),
            TypeCategory::Integer
        );
        assert_eq!(
            categorize_type("TINYINT", DatabaseType::MySql),
            TypeCategory::Integer
        );
        assert_eq!(
            categorize_type("SERIAL", DatabaseType::Postgres),
            TypeCategory::Integer
        );
    }

    #[test]
    fn test_categorize_type_decimal() {
        assert_eq!(
            categorize_type("DECIMAL", DatabaseType::MySql),
            TypeCategory::Decimal
        );
        assert_eq!(
            categorize_type("NUMERIC", DatabaseType::Postgres),
            TypeCategory::Decimal
        );
        // SQLite NUMERIC is a float
        assert_eq!(
            categorize_type("numeric", DatabaseType::SQLite),
            TypeCategory::Float
        );
    }

    #[test]
    fn test_categorize_type_json() {
        assert_eq!(
            categorize_type("json", DatabaseType::Postgres),
            TypeCategory::Json
        );
        assert_eq!(
            categorize_type("jsonb", DatabaseType::Postgres),
            TypeCategory::Json
        );
    }

    #[test]
    fn test_normalize_type_name() {
        assert_eq!(normalize_type_name("INT4"), "integer");
        assert_eq!(normalize_type_name("INTEGER"), "integer");
        assert_eq!(normalize_type_name("VARCHAR"), "text");
        assert_eq!(normalize_type_name("BOOLEAN"), "boolean");
        assert_eq!(normalize_type_name("FLOAT8"), "double");
        assert_eq!(normalize_type_name("BYTEA"), "binary");
        assert_eq!(normalize_type_name("JSONB"), "json");
        assert_eq!(normalize_type_name("TINYINT"), "tinyint");
    }

    #[test]
    fn test_decode_binary_value_with_valid_utf8() {
        let bytes = b"hello world";
        let result = decode_binary_value(bytes, true);
        assert_eq!(result, JsonValue::String("hello world".to_string()));

        let result = decode_binary_value(bytes, false);
        assert_eq!(result, JsonValue::String("aGVsbG8gd29ybGQ=".to_string()));
    }

    #[test]
    fn test_decode_binary_value_with_invalid_utf8() {
        let bytes: &[u8] = &[0xFF, 0xFE, 0x00, 0x01];
        let result = decode_binary_value(bytes, true);
        assert_eq!(result, JsonValue::String("//4AAQ==".to_string()));

        let result = decode_binary_value(bytes, false);
        assert_eq!(result, JsonValue::String("//4AAQ==".to_string()));
    }

    #[test]
    fn test_decode_binary_value_empty() {
        let bytes: &[u8] = &[];
        let result = decode_binary_value(bytes, true);
        assert_eq!(result, JsonValue::String("".to_string()));

        let result = decode_binary_value(bytes, false);
        assert_eq!(result, JsonValue::String("".to_string()));
    }
}
