//! Configuration handling for the DB MCP Server.
//!
//! This module provides configuration management via CLI arguments and environment variables.

use clap::{Parser, ValueEnum};
use std::collections::HashMap;
use std::time::Duration;
use url::Url;

pub const DEFAULT_HTTP_HOST: &str = "127.0.0.1";
pub const DEFAULT_HTTP_PORT: u16 = 8080;
pub const DEFAULT_MCP_ENDPOINT: &str = "/";
pub const DEFAULT_QUERY_TIMEOUT_SECS: u64 = 30;
pub const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 10;
pub const DEFAULT_TRANSACTION_TIMEOUT_SECS: u64 = 60;

// Pool configuration defaults
pub const DEFAULT_MAX_CONNECTIONS: u32 = 10;
pub const DEFAULT_MAX_CONNECTIONS_SQLITE: u32 = 1;
pub const DEFAULT_MIN_CONNECTIONS: u32 = 1;
pub const DEFAULT_IDLE_TIMEOUT_SECS: u64 = 600;
pub const DEFAULT_ACQUIRE_TIMEOUT_SECS: u64 = 30;

// Database pool configuration defaults (for server-level connections)
pub const DEFAULT_DATABASE_POOL_IDLE_TIMEOUT_SECS: u64 = 600;
pub const DEFAULT_DATABASE_POOL_CLEANUP_INTERVAL_SECS: u64 = 60;

/// Connection pool configuration options parsed from database URL.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct PoolOptions {
    /// Maximum connections in pool (default: 10 for MySQL/PostgreSQL, 1 for SQLite)
    pub max_connections: Option<u32>,
    /// Minimum connections in pool (default: 1)
    pub min_connections: Option<u32>,
    /// Idle timeout in seconds (default: 600)
    pub idle_timeout_secs: Option<u64>,
    /// Connection acquire timeout in seconds (default: 30)
    pub acquire_timeout_secs: Option<u64>,
    /// Whether to test connections before use (default: true)
    pub test_before_acquire: Option<bool>,
    /// Idle timeout for database pools in seconds (default: 600)
    pub database_pool_idle_timeout_secs: Option<u64>,
    /// Cleanup interval for database pools in seconds (default: 60)
    pub database_pool_cleanup_interval_secs: Option<u64>,
}

impl PoolOptions {
    /// Get max_connections with default value based on database type.
    pub fn max_connections_or_default(&self, is_sqlite: bool) -> u32 {
        self.max_connections.unwrap_or(if is_sqlite {
            DEFAULT_MAX_CONNECTIONS_SQLITE
        } else {
            DEFAULT_MAX_CONNECTIONS
        })
    }

    /// Get min_connections with default value.
    pub fn min_connections_or_default(&self) -> u32 {
        self.min_connections.unwrap_or(DEFAULT_MIN_CONNECTIONS)
    }

    /// Get idle_timeout with default value.
    pub fn idle_timeout_or_default(&self) -> u64 {
        self.idle_timeout_secs.unwrap_or(DEFAULT_IDLE_TIMEOUT_SECS)
    }

    /// Get acquire_timeout with default value.
    pub fn acquire_timeout_or_default(&self) -> u64 {
        self.acquire_timeout_secs
            .unwrap_or(DEFAULT_ACQUIRE_TIMEOUT_SECS)
    }

    /// Get test_before_acquire with default value.
    pub fn test_before_acquire_or_default(&self) -> bool {
        self.test_before_acquire.unwrap_or(true)
    }

    /// Get database_pool_idle_timeout with default value.
    pub fn database_pool_idle_timeout_or_default(&self) -> u64 {
        self.database_pool_idle_timeout_secs
            .unwrap_or(DEFAULT_DATABASE_POOL_IDLE_TIMEOUT_SECS)
    }

    /// Get database_pool_cleanup_interval with default value.
    pub fn database_pool_cleanup_interval_or_default(&self) -> u64 {
        self.database_pool_cleanup_interval_secs
            .unwrap_or(DEFAULT_DATABASE_POOL_CLEANUP_INTERVAL_SECS)
    }

    /// Validate pool options and return an error message if invalid.
    pub fn validate(&self) -> Result<(), String> {
        if let Some(max) = self.max_connections {
            if max == 0 {
                return Err("max_connections must be greater than 0".to_string());
            }
        }
        if let Some(min) = self.min_connections {
            if min == 0 {
                return Err("min_connections must be greater than 0".to_string());
            }
            if let Some(max) = self.max_connections {
                if min > max {
                    return Err(format!(
                        "min_connections ({}) cannot exceed max_connections ({})",
                        min, max
                    ));
                }
            }
        }
        Ok(())
    }
}

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

/// Database connection configuration parsed from CLI arguments.
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    /// Connection identifier. From "id=url" format, or derived from database name, or "default".
    pub id: String,
    /// Full connection URL (sensitive - not logged).
    pub connection_string: String,
    /// Default: false for safety
    pub writable: bool,
    /// True if connection is at server level (no specific database in URL)
    pub server_level: bool,
    /// Database name extracted from URL path. None for server-level connections.
    pub database: Option<String>,
    /// Connection pool configuration options parsed from URL query parameters.
    pub pool_options: PoolOptions,
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
    /// Pool option keys that we extract from URL query parameters.
    const POOL_OPTION_KEYS: &'static [&'static str] = &[
        "writable",
        "max_connections",
        "min_connections",
        "idle_timeout",
        "acquire_timeout",
        "test_before_acquire",
        "database_pool_idle_timeout",
        "database_pool_cleanup_interval",
    ];

    pub fn parse(s: &str) -> Result<Self, String> {
        // Split name=url format (only if '=' before '://')
        let scheme_pos = s.find("://").unwrap_or(s.len());
        let (explicit_name, url_str) = match s[..scheme_pos].find('=') {
            Some(idx) => (Some(&s[..idx]), &s[idx + 1..]),
            None => (None, s),
        };

        // Validate that "default" is not used as explicit connection ID
        if let Some(name) = explicit_name {
            if name.trim().eq_ignore_ascii_case("default") {
                return Err(
                    "Connection ID 'default' is reserved and cannot be used explicitly. \
                    Please choose a different ID or omit the ID to use the database name."
                        .to_string(),
                );
            }
        }

        let mut url = Url::parse(url_str).map_err(|e| format!("Invalid URL: {e}"))?;
        let mut opts = Self::extract_options(&mut url, Self::POOL_OPTION_KEYS);

        let writable = opts
            .remove("writable")
            .is_some_and(|v| v.eq_ignore_ascii_case("true"));

        // Parse and validate pool options
        let pool_options = Self::parse_pool_options(&mut opts);
        pool_options.validate()?;

        // Extract database name from URL path
        let database = Self::db_name(&url);
        let server_level = database.is_none();

        // Check if SQLite is being used without a file path
        let scheme = url.scheme().to_lowercase();
        if server_level && (scheme == "sqlite" || scheme.starts_with("sqlite")) {
            return Err(
                "SQLite requires a database file path. Server-level connections are only supported for MySQL and PostgreSQL.".to_string()
            );
        }

        // ID priority: explicit name > database name > "default"
        let id = explicit_name
            .map(String::from)
            .or_else(|| database.clone())
            .unwrap_or_else(|| "default".to_string());

        Ok(Self {
            id,
            connection_string: url.to_string(),
            writable,
            server_level,
            database,
            pool_options,
        })
    }

    /// Parse pool options from extracted URL query parameters.
    fn parse_pool_options(opts: &mut HashMap<String, String>) -> PoolOptions {
        PoolOptions {
            max_connections: opts.remove("max_connections").and_then(|v| v.parse().ok()),
            min_connections: opts.remove("min_connections").and_then(|v| v.parse().ok()),
            idle_timeout_secs: opts.remove("idle_timeout").and_then(|v| v.parse().ok()),
            acquire_timeout_secs: opts.remove("acquire_timeout").and_then(|v| v.parse().ok()),
            test_before_acquire: opts.remove("test_before_acquire").and_then(|v| {
                if v.eq_ignore_ascii_case("true") {
                    Some(true)
                } else if v.eq_ignore_ascii_case("false") {
                    Some(false)
                } else {
                    None // Invalid value ignored
                }
            }),
            database_pool_idle_timeout_secs: opts
                .remove("database_pool_idle_timeout")
                .and_then(|v| v.parse().ok()),
            database_pool_cleanup_interval_secs: opts
                .remove("database_pool_cleanup_interval")
                .and_then(|v| v.parse().ok()),
        }
    }

    /// Extract MCP-specific options from URL query params, keeping others for the driver.
    /// Uses proper URL encoding to preserve special characters in remaining params.
    fn extract_options(url: &mut Url, keys: &[&str]) -> HashMap<String, String> {
        let mut opts = HashMap::new();
        let remaining: Vec<(String, String)> = url
            .query_pairs()
            .filter_map(|(k, v)| {
                let key_lower = k.to_ascii_lowercase();
                if keys.contains(&key_lower.as_str()) {
                    opts.insert(key_lower, v.into_owned());
                    None
                } else {
                    Some((k.into_owned(), v.into_owned()))
                }
            })
            .collect();

        if remaining.is_empty() {
            url.set_query(None);
        } else {
            // Use query_pairs_mut for proper URL encoding
            url.query_pairs_mut().clear().extend_pairs(remaining);
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
        env = "MCP_DATABASE",
        value_delimiter = ','
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

    /// Enable logging output (disabled by default to avoid interfering with stdio transport)
    #[arg(long, env = "MCP_ENABLE_LOGS")]
    pub enable_logs: bool,

    /// Authentication tokens for HTTP transport.
    /// Can be specified multiple times or as comma-separated values.
    /// When set, all HTTP requests must include a valid Bearer token.
    #[arg(
        long = "auth-token",
        value_name = "TOKEN",
        env = "MCP_AUTH_TOKENS",
        value_delimiter = ','
    )]
    pub auth_tokens: Vec<String>,
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
            enable_logs: false,
            auth_tokens: Vec::new(),
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
        assert!(config.database.is_none());
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
        // ID is derived from database name
        assert_eq!(config.id, "mydb");
        assert_eq!(config.database, Some("mydb".to_string()));
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
        // ID is derived from database name (without extension)
        assert_eq!(config.id, "db");
    }

    #[test]
    fn test_parse_named_connection() {
        let config = DatabaseConfig::parse("myserver=mysql://user:pass@host:3306/db").unwrap();
        assert!(!config.server_level);
        // ID is explicit name
        assert_eq!(config.id, "myserver");
        assert_eq!(config.database, Some("db".to_string()));
    }

    #[test]
    fn test_parse_named_server_level_connection() {
        let config = DatabaseConfig::parse("myserver=mysql://user:pass@host:3306").unwrap();
        assert!(config.server_level);
        // ID is explicit name
        assert_eq!(config.id, "myserver");
        assert!(config.database.is_none());
    }

    // =========================================================================
    // Connection ID tests
    // =========================================================================

    #[test]
    fn test_connection_id_from_explicit_name() {
        let config = DatabaseConfig::parse("myname=mysql://host/db").unwrap();
        assert_eq!(config.id, "myname");
    }

    #[test]
    fn test_connection_id_from_database_name() {
        let config = DatabaseConfig::parse("mysql://host/mydb").unwrap();
        assert_eq!(config.id, "mydb");
    }

    #[test]
    fn test_connection_id_default_when_no_database() {
        let config = DatabaseConfig::parse("mysql://host:3306").unwrap();
        assert_eq!(config.id, "default");
    }

    #[test]
    fn test_reserved_connection_id_default_rejected() {
        let result = DatabaseConfig::parse("default=mysql://host:3306/mydb");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("reserved"));
        assert!(err.contains("default"));
    }

    #[test]
    fn test_reserved_connection_id_default_case_insensitive() {
        let test_cases = vec!["DEFAULT", "Default", "DeFaUlT"];
        for case in test_cases {
            let result = DatabaseConfig::parse(&format!("{}=mysql://host/db", case));
            assert!(result.is_err(), "Should reject '{}'", case);
            assert!(result.unwrap_err().contains("reserved"));
        }
    }

    #[test]
    fn test_reserved_connection_id_default_with_whitespace() {
        let result = DatabaseConfig::parse(" default =mysql://host/db");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("reserved"));
    }

    // =========================================================================
    // User Story 2: Database Field Extraction
    // =========================================================================

    #[test]
    fn test_database_extraction_mysql() {
        // T018: Test MySQL URL database extraction
        let config = DatabaseConfig::parse("mysql://user:pass@host:3306/mydb").unwrap();
        assert_eq!(config.database, Some("mydb".to_string()));

        // With query params
        let config2 = DatabaseConfig::parse("mysql://host:3306/production?charset=utf8").unwrap();
        assert_eq!(config2.database, Some("production".to_string()));
    }

    #[test]
    fn test_database_extraction_postgres() {
        // T019: Test PostgreSQL URL database extraction
        let config = DatabaseConfig::parse("postgres://user:pass@host:5432/analytics").unwrap();
        assert_eq!(config.database, Some("analytics".to_string()));

        // Alternative scheme
        let config2 = DatabaseConfig::parse("postgresql://host/mydb").unwrap();
        assert_eq!(config2.database, Some("mydb".to_string()));
    }

    #[test]
    fn test_database_extraction_sqlite() {
        // T020: Test SQLite file path extraction (strips extensions)
        let config = DatabaseConfig::parse("sqlite://path/to/local.db").unwrap();
        assert_eq!(config.database, Some("local".to_string()));

        let config2 = DatabaseConfig::parse("sqlite://path/to/test.sqlite").unwrap();
        assert_eq!(config2.database, Some("test".to_string()));

        // Without extension
        let config3 = DatabaseConfig::parse("sqlite:./data/mydata").unwrap();
        assert_eq!(config3.database, Some("mydata".to_string()));
    }

    #[test]
    fn test_database_extraction_server_level() {
        // T021: Test server-level URLs return None
        let config = DatabaseConfig::parse("mysql://host:3306").unwrap();
        assert!(
            config.database.is_none(),
            "Server-level MySQL should have no database"
        );

        let config2 = DatabaseConfig::parse("postgres://host:5432/").unwrap();
        assert!(
            config2.database.is_none(),
            "Server-level Postgres should have no database"
        );
    }

    // =========================================================================
    // Pool Options Tests
    // =========================================================================

    #[test]
    fn test_pool_options_defaults() {
        let opts = PoolOptions::default();
        assert_eq!(opts.max_connections_or_default(false), 10);
        assert_eq!(opts.max_connections_or_default(true), 1);
        assert_eq!(opts.min_connections_or_default(), 1);
        assert_eq!(opts.idle_timeout_or_default(), 600);
        assert_eq!(opts.acquire_timeout_or_default(), 30);
        assert!(opts.test_before_acquire_or_default());
    }

    #[test]
    fn test_pool_options_custom_values() {
        let opts = PoolOptions {
            max_connections: Some(20),
            min_connections: Some(5),
            idle_timeout_secs: Some(300),
            acquire_timeout_secs: Some(60),
            test_before_acquire: Some(false),
            database_pool_idle_timeout_secs: None,
            database_pool_cleanup_interval_secs: None,
        };
        assert_eq!(opts.max_connections_or_default(false), 20);
        assert_eq!(opts.max_connections_or_default(true), 20);
        assert_eq!(opts.min_connections_or_default(), 5);
        assert_eq!(opts.idle_timeout_or_default(), 300);
        assert_eq!(opts.acquire_timeout_or_default(), 60);
        assert!(!opts.test_before_acquire_or_default());
    }

    #[test]
    fn test_parse_pool_options_from_url() {
        let config = DatabaseConfig::parse(
            "mysql://host/db?max_connections=20&min_connections=5&idle_timeout=300",
        )
        .unwrap();

        assert_eq!(config.pool_options.max_connections, Some(20));
        assert_eq!(config.pool_options.min_connections, Some(5));
        assert_eq!(config.pool_options.idle_timeout_secs, Some(300));
        assert!(config.pool_options.acquire_timeout_secs.is_none());
        assert!(config.pool_options.test_before_acquire.is_none());
    }

    #[test]
    fn test_parse_pool_options_acquire_timeout() {
        let config = DatabaseConfig::parse(
            "postgres://host/db?acquire_timeout=120&test_before_acquire=true",
        )
        .unwrap();

        assert_eq!(config.pool_options.acquire_timeout_secs, Some(120));
        assert_eq!(config.pool_options.test_before_acquire, Some(true));
    }

    #[test]
    fn test_parse_pool_options_test_before_acquire_false() {
        let config = DatabaseConfig::parse("mysql://host/db?test_before_acquire=false").unwrap();

        assert_eq!(config.pool_options.test_before_acquire, Some(false));
    }

    #[test]
    fn test_pool_options_stripped_from_connection_string() {
        let config = DatabaseConfig::parse(
            "mysql://host/db?max_connections=20&charset=utf8&idle_timeout=300",
        )
        .unwrap();

        assert_eq!(config.pool_options.max_connections, Some(20));
        assert_eq!(config.pool_options.idle_timeout_secs, Some(300));
        assert!(config.connection_string.contains("charset=utf8"));
        assert!(!config.connection_string.contains("max_connections"));
        assert!(!config.connection_string.contains("idle_timeout"));
    }

    #[test]
    fn test_pool_options_with_writable() {
        let config =
            DatabaseConfig::parse("mysql://host/db?writable=true&max_connections=50").unwrap();

        assert!(config.writable);
        assert_eq!(config.pool_options.max_connections, Some(50));
        assert!(!config.connection_string.contains("writable"));
        assert!(!config.connection_string.contains("max_connections"));
    }

    #[test]
    fn test_pool_options_invalid_value_ignored() {
        let config = DatabaseConfig::parse("mysql://host/db?max_connections=invalid").unwrap();

        assert!(config.pool_options.max_connections.is_none());
    }

    #[test]
    fn test_pool_options_invalid_boolean_ignored() {
        let config = DatabaseConfig::parse("mysql://host/db?test_before_acquire=garbage").unwrap();
        assert!(config.pool_options.test_before_acquire.is_none());

        let config2 = DatabaseConfig::parse("mysql://host/db?test_before_acquire=yes").unwrap();
        assert!(config2.pool_options.test_before_acquire.is_none());
    }

    #[test]
    fn test_pool_options_validation_max_zero() {
        let result = DatabaseConfig::parse("mysql://host/db?max_connections=0");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("max_connections"));
    }

    #[test]
    fn test_pool_options_validation_min_zero() {
        let result = DatabaseConfig::parse("mysql://host/db?min_connections=0");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("min_connections"));
    }

    #[test]
    fn test_pool_options_validation_min_exceeds_max() {
        let result = DatabaseConfig::parse("mysql://host/db?min_connections=10&max_connections=5");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("min_connections"));
        assert!(err.contains("cannot exceed"));
    }

    #[test]
    fn test_url_encoding_preserved_in_connection_string() {
        // Test that special characters in remaining params are preserved
        let config = DatabaseConfig::parse(
            "mysql://host/db?sslcert=%2Ftmp%2Fcert%26key.pem&max_connections=20",
        )
        .unwrap();

        assert_eq!(config.pool_options.max_connections, Some(20));
        // The connection string should still be valid for the driver
        assert!(config.connection_string.contains("sslcert="));
        // The %26 should be preserved (either as %26 or properly re-encoded)
        assert!(!config.connection_string.contains("max_connections"));
    }
}
