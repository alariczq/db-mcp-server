//! Output formatting utilities for MCP tools.
//!
//! This module provides shared output format types and formatting functions
//! used by query, explain, and other tools that return tabular data.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use unicode_width::UnicodeWidthStr;

/// Output format for query/explain results.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum OutputFormat {
    /// JSON format (default)
    #[default]
    Json,
    /// ASCII table format (like MySQL CLI)
    Table,
    /// Markdown table format
    Markdown,
}

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
}

impl ColumnInfo {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

pub fn format_value(value: &JsonValue) -> String {
    match value {
        JsonValue::Null => "NULL".to_string(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::String(s) => s.clone(),
        JsonValue::Array(arr) => serde_json::to_string(arr).unwrap_or_default(),
        JsonValue::Object(obj) => serde_json::to_string(obj).unwrap_or_default(),
    }
}

pub fn format_as_table(
    columns: &[ColumnInfo],
    rows: &[serde_json::Map<String, JsonValue>],
    row_count: usize,
    execution_time_ms: u64,
) -> String {
    if columns.is_empty() {
        return "Empty set".to_string();
    }

    let mut widths: Vec<usize> = columns.iter().map(|c| c.name.width()).collect();
    for row in rows {
        for (i, col) in columns.iter().enumerate() {
            if let Some(value) = row.get(&col.name) {
                let val_width = format_value(value).width();
                widths[i] = widths[i].max(val_width);
            }
        }
    }

    let mut output = String::new();
    let separator: String = widths
        .iter()
        .map(|w| format!("+{}", "-".repeat(w + 2)))
        .collect::<String>()
        + "+\n";

    output.push_str(&separator);
    let header: String = columns
        .iter()
        .zip(&widths)
        .map(|(col, w)| format!("| {:^width$} ", col.name, width = w))
        .collect::<String>()
        + "|\n";
    output.push_str(&header);
    output.push_str(&separator);

    for row in rows {
        let row_str: String = columns
            .iter()
            .zip(&widths)
            .map(|(col, w)| {
                let value = row.get(&col.name).cloned().unwrap_or(JsonValue::Null);
                let formatted = format_value(&value);
                if matches!(value, JsonValue::Number(_)) {
                    format!("| {:>width$} ", formatted, width = w)
                } else {
                    format!("| {:<width$} ", formatted, width = w)
                }
            })
            .collect::<String>()
            + "|\n";
        output.push_str(&row_str);
    }

    output.push_str(&separator);

    let row_text = if row_count == 1 { "row" } else { "rows" };
    output.push_str(&format!(
        "{} {} in set ({:.2} sec)\n",
        row_count,
        row_text,
        execution_time_ms as f64 / 1000.0
    ));

    output
}

pub fn format_as_markdown(
    columns: &[ColumnInfo],
    rows: &[serde_json::Map<String, JsonValue>],
    row_count: usize,
) -> String {
    if columns.is_empty() {
        return "*Empty set*".to_string();
    }

    let mut output = String::new();

    let header: String = columns
        .iter()
        .map(|c| format!("| {} ", c.name))
        .collect::<String>()
        + "|\n";
    output.push_str(&header);

    let sep: String = columns.iter().map(|_| "|---").collect::<String>() + "|\n";
    output.push_str(&sep);

    for row in rows {
        let row_str: String = columns
            .iter()
            .map(|col| {
                let value = row.get(&col.name).cloned().unwrap_or(JsonValue::Null);
                format!("| {} ", format_value(&value))
            })
            .collect::<String>()
            + "|\n";
        output.push_str(&row_str);
    }

    output.push_str(&format!("\n*{} rows*", row_count));

    output
}
