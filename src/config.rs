//! Configuration handling for the DB MCP Server.
//!
//! This module provides configuration management via CLI arguments and environment variables.

use clap::{Parser, ValueEnum};
use std::collections::HashMap;
use std::time::Duration;
use url::Url;

/// Default HTTP host.
pub const DEFAULT_HTTP_HOST: &str = "127.0.0.1";

/// Default HTTP port.
pub const DEFAULT_HTTP_PORT: u16 = 8080;

/// Default MCP endpoint path.
pub const DEFAULT_MCP_ENDPOINT: &str = "/";

/// Default query timeout in seconds.
pub const DEFAULT_QUERY_TIMEOUT_SECS: u64 = 30;

/// Default connection timeout in seconds.
pub const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 10;

/// Default transaction timeout in seconds.
pub const DEFAULT_TRANSACTION_TIMEOUT_SECS: u64 = 60;

/// Transport mode for the MCP server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum)]
pub enum TransportMode {
    /// Standard input/output (for CLI integration)
    #[default]
    Stdio,
    /// HTTP with Server-Sent Events (for web clients)
    Http,
}

impl std::fmt::Display for TransportMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stdio => write!(f, "stdio"),
            Self::Http => write!(f, "http"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub id: String,
    pub connection_string: String,
    /// Default: false for safety
    pub writable: bool,
    /// True if connection is at server level (no specific database in URL)
    pub server_level: bool,
}

impl DatabaseConfig {
    /// Parse a database config from CLI argument.
    ///
    /// # Format
    ///
    /// - `connection_string` - Uses database name as ID, read-only by default
    /// - `id=connection_string` - Named connection, read-only by default
    /// - `connection_string?writable=true` - Enable write operations
    /// - `id=connection_string?writable=true` - Named writable connection
    ///
    /// # Examples
    ///
    /// ```text
    /// mysql://user:pass@host:3306/mydb                    # read-only
    /// mysql://user:pass@host:3306/mydb?writable=true      # writable
    /// mydb=postgres://user:pass@host/db?writable=true     # named, writable
    /// ```
    pub fn parse(s: &str) -> Result<Self, String> {
        // Split id=url format (only if '=' before '://')
        let scheme_pos = s.find("://").unwrap_or(s.len());
        let (explicit_id, url_str) = match s[..scheme_pos].find('=') {
            Some(idx) => (Some(&s[..idx]), &s[idx + 1..]),
            None => (None, s),
        };

        let mut url = Url::parse(url_str).map_err(|e| format!("Invalid URL: {e}"))?;
        let mut opts = Self::extract_options(&mut url, &["writable"]);

        let writable = opts
            .remove("writable")
            .is_some_and(|v| v.eq_ignore_ascii_case("true"));

        // Detect server-level connection (no database in URL path)
        let db_name = Self::db_name(&url);
        let server_level = db_name.is_none();

        // Check if SQLite is being used without a file path
        let scheme = url.scheme().to_lowercase();
        if server_level && (scheme == "sqlite" || scheme.starts_with("sqlite")) {
            return Err(
                "SQLite requires a database file path. Server-level connections are only supported for MySQL and PostgreSQL.".to_string()
            );
        }

        let id = explicit_id
            .map(String::from)
            .or(db_name)
            .unwrap_or_else(|| "default".into());

        Ok(Self {
            id,
            connection_string: url.to_string(),
            writable,
            server_level,
        })
    }

    /// Extract MCP-specific options from URL query params, keeping others for the driver.
    fn extract_options(url: &mut Url, keys: &[&str]) -> HashMap<String, String> {
        let mut opts = HashMap::new();
        let remaining: Vec<_> = url
            .query_pairs()
            .filter_map(|(k, v)| {
                let key_lower = k.to_ascii_lowercase();
                if keys.contains(&key_lower.as_str()) {
                    opts.insert(key_lower, v.into_owned());
                    None
                } else {
                    Some(format!("{k}={v}"))
                }
            })
            .collect();

        if remaining.is_empty() {
            url.set_query(None);
        } else {
            url.set_query(Some(&remaining.join("&")));
        }
        opts
    }

    fn db_name(url: &Url) -> Option<String> {
        url.path()
            .rsplit('/')
            .next()
            .filter(|s| !s.is_empty())
            .map(|s| s.trim_end_matches(".sqlite").trim_end_matches(".db"))
            .filter(|s| !s.is_empty())
            .map(String::from)
    }
}

/// Configuration for the DB MCP Server.
#[derive(Debug, Clone, Parser)]
#[command(
    name = "db-mcp-server",
    about = "MCP server for database operations - enables AI assistants to query SQL databases",
    version,
    author
)]
pub struct Config {
    /// Preconfigured database connections.
    /// Format: "connection_string" or "id=connection_string"
    /// Add ?writable=true to enable write operations.
    /// Can be specified multiple times for multiple databases.
    #[arg(
        short = 'd',
        long = "database",
        value_name = "URL",
        env = "MCP_DATABASE"
    )]
    pub databases: Vec<String>,

    /// Transport mode (stdio or http)
    #[arg(
        short,
        long,
        value_enum,
        default_value = "stdio",
        env = "MCP_TRANSPORT"
    )]
    pub transport: TransportMode,

    /// HTTP host to bind to (only used with http transport)
    #[arg(
        long,
        default_value = DEFAULT_HTTP_HOST,
        env = "MCP_HTTP_HOST"
    )]
    pub http_host: String,

    /// HTTP port to bind to (only used with http transport)
    #[arg(
        long,
        default_value_t = DEFAULT_HTTP_PORT,
        env = "MCP_HTTP_PORT"
    )]
    pub http_port: u16,

    /// MCP endpoint path (only used with http transport)
    #[arg(
        long,
        default_value = DEFAULT_MCP_ENDPOINT,
        env = "MCP_ENDPOINT"
    )]
    pub mcp_endpoint: String,

    /// Query timeout in seconds
    #[arg(
        long,
        default_value_t = DEFAULT_QUERY_TIMEOUT_SECS,
        env = "MCP_QUERY_TIMEOUT"
    )]
    pub query_timeout: u64,

    /// Connection timeout in seconds
    #[arg(
        long,
        default_value_t = DEFAULT_CONNECT_TIMEOUT_SECS,
        env = "MCP_CONNECT_TIMEOUT"
    )]
    pub connect_timeout: u64,

    /// Transaction timeout in seconds
    #[arg(
        long,
        default_value_t = DEFAULT_TRANSACTION_TIMEOUT_SECS,
        env = "MCP_TRANSACTION_TIMEOUT"
    )]
    pub transaction_timeout: u64,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info", env = "MCP_LOG_LEVEL")]
    pub log_level: String,

    /// Enable JSON logging format
    #[arg(long, env = "MCP_JSON_LOGS")]
    pub json_logs: bool,
}

impl Config {
    /// Parse configuration from command line arguments.
    pub fn parse_args() -> Self {
        Self::parse()
    }

    /// Create a default configuration (useful for testing).
    pub fn default_config() -> Self {
        Self {
            databases: Vec::new(),
            transport: TransportMode::Stdio,
            http_host: DEFAULT_HTTP_HOST.to_string(),
            http_port: DEFAULT_HTTP_PORT,
            mcp_endpoint: DEFAULT_MCP_ENDPOINT.to_string(),
            query_timeout: DEFAULT_QUERY_TIMEOUT_SECS,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT_SECS,
            transaction_timeout: DEFAULT_TRANSACTION_TIMEOUT_SECS,
            log_level: "info".to_string(),
            json_logs: false,
        }
    }

    /// Parse all database configurations.
    pub fn parse_databases(&self) -> Result<Vec<DatabaseConfig>, String> {
        self.databases
            .iter()
            .map(|s| DatabaseConfig::parse(s))
            .collect()
    }

    /// Get the HTTP bind address.
    pub fn http_bind_addr(&self) -> String {
        format!("{}:{}", self.http_host, self.http_port)
    }

    /// Get the query timeout as a Duration.
    pub fn query_timeout_duration(&self) -> Duration {
        Duration::from_secs(self.query_timeout)
    }

    /// Get the connection timeout as a Duration.
    pub fn connect_timeout_duration(&self) -> Duration {
        Duration::from_secs(self.connect_timeout)
    }

    /// Get the transaction timeout as a Duration.
    pub fn transaction_timeout_duration(&self) -> Duration {
        Duration::from_secs(self.transaction_timeout)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::default_config()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.transport, TransportMode::Stdio);
        assert_eq!(config.http_host, DEFAULT_HTTP_HOST);
        assert_eq!(config.http_port, DEFAULT_HTTP_PORT);
    }

    #[test]
    fn test_http_bind_addr() {
        let config = Config {
            http_host: "0.0.0.0".to_string(),
            http_port: 3000,
            ..Config::default()
        };
        assert_eq!(config.http_bind_addr(), "0.0.0.0:3000");
    }

    #[test]
    fn test_timeout_durations() {
        let config = Config {
            query_timeout: 60,
            connect_timeout: 15,
            transaction_timeout: 120,
            ..Config::default()
        };
        assert_eq!(config.query_timeout_duration(), Duration::from_secs(60));
        assert_eq!(config.connect_timeout_duration(), Duration::from_secs(15));
        assert_eq!(
            config.transaction_timeout_duration(),
            Duration::from_secs(120)
        );
    }

    // US1: URL writable parameter tests

    #[test]
    fn test_parse_writable_true() {
        let config =
            DatabaseConfig::parse("mysql://user:pass@host:3306/mydb?writable=true").unwrap();
        assert!(config.writable);
        assert!(!config.connection_string.contains("writable"));
    }

    #[test]
    fn test_parse_writable_false() {
        let config =
            DatabaseConfig::parse("mysql://user:pass@host:3306/mydb?writable=false").unwrap();
        assert!(!config.writable);
    }

    #[test]
    fn test_parse_no_writable_param_defaults_false() {
        let config = DatabaseConfig::parse("postgres://user:pass@host:5432/mydb").unwrap();
        assert!(!config.writable);
    }

    #[test]
    fn test_parse_writable_case_insensitive() {
        let config1 = DatabaseConfig::parse("mysql://host/db?writable=TRUE").unwrap();
        let config2 = DatabaseConfig::parse("mysql://host/db?writable=True").unwrap();
        let config3 = DatabaseConfig::parse("mysql://host/db?writable=true").unwrap();
        assert!(config1.writable);
        assert!(config2.writable);
        assert!(config3.writable);
    }

    #[test]
    fn test_parse_writable_invalid_value_defaults_false() {
        let config1 = DatabaseConfig::parse("mysql://host/db?writable=yes").unwrap();
        let config2 = DatabaseConfig::parse("mysql://host/db?writable=1").unwrap();
        let config3 = DatabaseConfig::parse("mysql://host/db?writable=").unwrap();
        assert!(!config1.writable);
        assert!(!config2.writable);
        assert!(!config3.writable);
    }

    #[test]
    fn test_parse_writable_last_value_wins() {
        let config = DatabaseConfig::parse("mysql://host/db?writable=false&writable=true").unwrap();
        assert!(config.writable);
    }

    // US2: Combine writable with other URL parameters

    #[test]
    fn test_parse_writable_with_other_params() {
        let config = DatabaseConfig::parse(
            "mysql://user:pass@host:3306/mydb?ssl-mode=required&writable=true",
        )
        .unwrap();
        assert!(config.writable);
        assert!(config.connection_string.contains("ssl-mode=required"));
    }

    #[test]
    fn test_parse_writable_preserves_other_params() {
        let config = DatabaseConfig::parse(
            "postgres://user:pass@host:5432/mydb?sslmode=require&writable=true&connect_timeout=10",
        )
        .unwrap();
        assert!(config.writable);
        assert!(config.connection_string.contains("sslmode=require"));
        assert!(config.connection_string.contains("connect_timeout=10"));
    }

    #[test]
    fn test_parse_writable_strips_from_connection_string() {
        let config = DatabaseConfig::parse("mysql://host/db?writable=true&charset=utf8").unwrap();
        assert!(config.writable);
        assert!(!config.connection_string.contains("writable"));
        assert!(config.connection_string.contains("charset=utf8"));
        assert_eq!(config.connection_string, "mysql://host/db?charset=utf8");
    }

    // Server-level URL tests

    #[test]
    fn test_parse_mysql_url_without_database_sets_server_level() {
        let config = DatabaseConfig::parse("mysql://user:pass@host:3306").unwrap();
        assert!(config.server_level);
        assert_eq!(config.id, "default");
    }

    #[test]
    fn test_parse_mysql_url_without_database_with_writable() {
        let config = DatabaseConfig::parse("mysql://user:pass@host:3306?writable=true").unwrap();
        assert!(config.server_level);
        assert!(config.writable);
        assert_eq!(config.id, "default");
    }

    #[test]
    fn test_parse_postgres_url_without_database_sets_server_level() {
        let config = DatabaseConfig::parse("postgres://user:pass@host:5432").unwrap();
        assert!(config.server_level);
        assert_eq!(config.id, "default");
    }

    #[test]
    fn test_parse_postgres_url_without_database_with_slash() {
        let config = DatabaseConfig::parse("postgres://user:pass@host:5432/").unwrap();
        assert!(config.server_level);
        assert_eq!(config.id, "default");
    }

    #[test]
    fn test_parse_url_with_database_sets_server_level_false() {
        let config = DatabaseConfig::parse("mysql://user:pass@host:3306/mydb").unwrap();
        assert!(!config.server_level);
        assert_eq!(config.id, "mydb");
    }

    #[test]
    fn test_parse_sqlite_url_without_path_returns_error() {
        let result = DatabaseConfig::parse("sqlite://");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("SQLite requires a database file path")
        );
    }

    #[test]
    fn test_parse_sqlite_url_with_path_sets_server_level_false() {
        let config = DatabaseConfig::parse("sqlite://path/to/db.sqlite").unwrap();
        assert!(!config.server_level);
    }

    #[test]
    fn test_parse_named_server_level_connection() {
        let config = DatabaseConfig::parse("myserver=mysql://user:pass@host:3306").unwrap();
        assert!(config.server_level);
        assert_eq!(config.id, "myserver");
    }
}
