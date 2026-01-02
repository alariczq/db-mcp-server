//! Query-related data models.
//!
//! This module defines types for SQL query requests and results.

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// Default row limit for query results.
pub const DEFAULT_ROW_LIMIT: u32 = 100;

/// Maximum allowed row limit.
pub const MAX_ROW_LIMIT: u32 = 10000;

/// Default query timeout in seconds.
pub const DEFAULT_QUERY_TIMEOUT_SECS: u32 = 30;

/// Maximum query timeout in seconds.
pub const MAX_QUERY_TIMEOUT_SECS: u32 = 300;

/// A parameter value for parameterized queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum QueryParam {
    /// NULL value
    Null,
    /// Boolean value
    Bool(bool),
    /// Integer value (stored as i64 for maximum range)
    Int(i64),
    /// Floating point value
    Float(f64),
    /// String value
    String(String),
    /// Binary data (base64 encoded in JSON)
    #[serde(with = "base64_bytes")]
    Bytes(Vec<u8>),
}

impl QueryParam {
    /// Check if this parameter is null.
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Get the type name of this parameter for debugging.
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Bool(_) => "bool",
            Self::Int(_) => "int",
            Self::Float(_) => "float",
            Self::String(_) => "string",
            Self::Bytes(_) => "bytes",
        }
    }
}

/// Custom serialization for binary data as base64.
mod base64_bytes {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(bytes: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        STANDARD.encode(bytes).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        STANDARD.decode(&s).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRequest {
    pub connection_id: String,
    pub sql: String,
    #[serde(default)]
    pub params: Vec<QueryParam>,
    /// Default: 100, max: 10000
    #[serde(default)]
    pub limit: Option<u32>,
    /// Default: 30, max: 300
    #[serde(default)]
    pub timeout_secs: Option<u32>,
    /// Default: false
    #[serde(default = "default_decode_binary")]
    pub decode_binary: bool,
}

fn default_decode_binary() -> bool {
    false
}

impl QueryRequest {
    /// Create a new query request with default options.
    pub fn new(connection_id: impl Into<String>, sql: impl Into<String>) -> Self {
        Self {
            connection_id: connection_id.into(),
            sql: sql.into(),
            params: Vec::new(),
            limit: None,
            timeout_secs: None,
            decode_binary: false,
        }
    }

    /// Add a parameter to this query.
    pub fn with_param(mut self, param: QueryParam) -> Self {
        self.params.push(param);
        self
    }

    /// Set the row limit.
    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set the timeout.
    pub fn with_timeout(mut self, timeout_secs: u32) -> Self {
        self.timeout_secs = Some(timeout_secs);
        self
    }

    /// Get the effective row limit (with bounds checking).
    pub fn effective_limit(&self) -> u32 {
        self.limit
            .map(|l| l.min(MAX_ROW_LIMIT))
            .unwrap_or(DEFAULT_ROW_LIMIT)
    }

    /// Get the effective timeout (with bounds checking).
    pub fn effective_timeout(&self) -> u32 {
        self.timeout_secs
            .map(|t| t.min(MAX_QUERY_TIMEOUT_SECS))
            .unwrap_or(DEFAULT_QUERY_TIMEOUT_SECS)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMetadata {
    pub name: String,
    /// Database-specific type (e.g., "int8", "varchar", "TEXT")
    pub type_name: String,
    pub nullable: bool,
}

impl ColumnMetadata {
    /// Create new column metadata.
    pub fn new(name: impl Into<String>, type_name: impl Into<String>, nullable: bool) -> Self {
        Self {
            name: name.into(),
            type_name: type_name.into(),
            nullable,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<ColumnMetadata>,
    pub rows: Vec<serde_json::Map<String, JsonValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_affected: Option<u64>,
    pub truncated: bool,
    pub execution_time_ms: u64,
}

impl QueryResult {
    /// Create an empty result (for non-SELECT queries).
    pub fn empty(execution_time_ms: u64) -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: None,
            truncated: false,
            execution_time_ms,
        }
    }

    /// Create a result for write operations (INSERT/UPDATE/DELETE).
    pub fn write_result(rows_affected: u64, execution_time_ms: u64) -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: Some(rows_affected),
            truncated: false,
            execution_time_ms,
        }
    }

    /// Get the number of rows in the result.
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Check if the result is empty.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty() && self.rows_affected.is_none()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteRequest {
    pub connection_id: String,
    pub sql: String,
    #[serde(default)]
    pub params: Vec<QueryParam>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_id: Option<String>,
}

impl ExecuteRequest {
    /// Create a new execute request.
    pub fn new(connection_id: impl Into<String>, sql: impl Into<String>) -> Self {
        Self {
            connection_id: connection_id.into(),
            sql: sql.into(),
            params: Vec::new(),
            transaction_id: None,
        }
    }

    /// Add a parameter.
    pub fn with_param(mut self, param: QueryParam) -> Self {
        self.params.push(param);
        self
    }

    /// Set the transaction ID.
    pub fn with_transaction(mut self, transaction_id: impl Into<String>) -> Self {
        self.transaction_id = Some(transaction_id.into());
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteResult {
    pub rows_affected: u64,
    pub execution_time_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_param_types() {
        assert!(QueryParam::Null.is_null());
        assert!(!QueryParam::Bool(true).is_null());
        assert_eq!(QueryParam::Int(42).type_name(), "int");
        assert_eq!(
            QueryParam::String("hello".to_string()).type_name(),
            "string"
        );
    }

    #[test]
    fn test_query_request_defaults() {
        let req = QueryRequest::new("conn1", "SELECT * FROM users");
        assert_eq!(req.effective_limit(), DEFAULT_ROW_LIMIT);
        assert_eq!(req.effective_timeout(), DEFAULT_QUERY_TIMEOUT_SECS);
    }

    #[test]
    fn test_query_request_bounds() {
        let req = QueryRequest::new("conn1", "SELECT * FROM users")
            .with_limit(99999)
            .with_timeout(999);

        assert_eq!(req.effective_limit(), MAX_ROW_LIMIT);
        assert_eq!(req.effective_timeout(), MAX_QUERY_TIMEOUT_SECS);
    }

    #[test]
    fn test_query_result_empty() {
        let result = QueryResult::empty(10);
        assert!(result.is_empty());
        assert_eq!(result.row_count(), 0);
    }

    #[test]
    fn test_query_result_write() {
        let result = QueryResult::write_result(5, 20);
        assert!(!result.is_empty());
        assert_eq!(result.rows_affected, Some(5));
    }
}
