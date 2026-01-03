//! DB MCP Server - Main entry point.
//!
//! This server provides MCP (Model Context Protocol) tools for AI assistants
//! to interact with SQL databases (SQLite, PostgreSQL, MySQL).

use clap::Parser;
use db_mcp_server::config::{Config, TransportMode};
use db_mcp_server::db::{ConnectionManager, TransactionRegistry};
use db_mcp_server::models::{ConnectionConfig, DatabaseType};
use db_mcp_server::transport::{HttpTransport, StdioTransport, Transport};
use std::sync::Arc;
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

/// Initialize the tracing subscriber for logging.
fn init_tracing(config: &Config) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.log_level));

    let subscriber = tracing_subscriber::registry().with(filter);

    if config.json_logs {
        subscriber.with(fmt::layer().json()).init();
    } else {
        subscriber
            .with(fmt::layer().with_target(true).with_thread_ids(false))
            .init();
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::parse();
    init_tracing(&config);

    // Require at least one database to be configured
    if config.databases.is_empty() {
        eprintln!("Error: At least one database must be configured.");
        eprintln!();
        eprintln!("Usage: db-mcp-server --database <connection_string>");
        eprintln!("       db-mcp-server --database <id>=<connection_string>");
        eprintln!("       db-mcp-server --database <connection_string>?writable=true");
        eprintln!();
        eprintln!("Examples:");
        eprintln!("  db-mcp-server --database sqlite:data.db");
        eprintln!("  db-mcp-server --database mydb=postgres://user:pass@localhost/mydb");
        eprintln!("  db-mcp-server --database mysql://user:pass@localhost/sales?writable=true");
        eprintln!("  db-mcp-server --database db1=sqlite:one.db --database db2=sqlite:two.db");
        eprintln!();
        eprintln!("Server-level connections (without database in URL):");
        eprintln!("  db-mcp-server --database mysql://user:pass@localhost:3306");
        eprintln!("  db-mcp-server --database postgres://user:pass@localhost:5432?writable=true");
        std::process::exit(1);
    }

    info!(
        transport = %config.transport,
        "Starting DB MCP Server v{}",
        env!("CARGO_PKG_VERSION")
    );

    let connection_manager = Arc::new(ConnectionManager::new());
    let transaction_registry = Arc::new(TransactionRegistry::with_defaults(
        config.transaction_timeout as u32,
    ));
    TransactionRegistry::start_cleanup_task(transaction_registry.clone());

    let db_configs = config.parse_databases()?;
    info!(
        count = db_configs.len(),
        "Connecting to preconfigured databases"
    );

    for db_config in &db_configs {
        info!(
            id = %db_config.id,
            database = ?db_config.database,
            writable = db_config.writable,
            "Connecting to database"
        );

        let db_type = DatabaseType::from_connection_string(&db_config.connection_string)
            .ok_or_else(|| format!("Unknown database type for connection: {}", db_config.id))?;

        let conn_config = ConnectionConfig {
            id: db_config.id.clone(),
            db_type,
            connection_string: db_config.connection_string.clone(),
            writable: db_config.writable,
            server_level: db_config.server_level,
            database: db_config.database.clone(),
            pool_options: db_config.pool_options.clone(),
        };

        connection_manager.connect(conn_config).await?;
    }

    let result = match config.transport {
        TransportMode::Stdio => {
            info!("Using stdio transport");
            let transport = StdioTransport::with_config(
                connection_manager,
                transaction_registry,
                config.query_timeout,
                100, // Default row limit
            );
            transport.run().await
        }
        TransportMode::Http => {
            info!(
                host = %config.http_host,
                port = config.http_port,
                endpoint = %config.mcp_endpoint,
                "Using HTTP transport"
            );
            let transport = HttpTransport::with_config(
                connection_manager,
                transaction_registry,
                &config.http_host,
                config.http_port,
                &config.mcp_endpoint,
                config.query_timeout,
                100, // Default row limit
            );
            transport.run().await
        }
    };

    if let Err(e) = result {
        error!(error = %e, "Server error");
        return Err(e.into());
    }

    info!("Server shutdown complete");
    Ok(())
}
