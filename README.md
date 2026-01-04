# DB MCP Server

A Rust-based [MCP](https://modelcontextprotocol.io/) server that enables AI assistants to interact with SQL databases (SQLite, PostgreSQL, MySQL) safely and efficiently.

## Features

- **Multi-database support**: SQLite, PostgreSQL, MySQL
- **Read-only by default**: Write operations require explicit `?writable=true` flag
- **Transaction support**: Full ACID transaction management across multiple MCP calls
- **Two transport modes**:
  - stdio for CLI integration (Claude Desktop, etc.)
  - HTTP with Server-Sent Events for web clients
- **Parameterized queries**: Built-in SQL injection protection
- **Server-level connections**: Connect to database server without selecting a database
- **Dangerous operation guard**: AST-based protection against DROP, TRUNCATE, and unqualified DELETE/UPDATE
- **Lazy per-database pools**: Efficient connection pooling for server-level connections
- **Output formatting**: JSON, ASCII table, or Markdown table formats

## Installation

```bash
cargo install --path .
```

Or build from source:

```bash
cargo build --release
```

## Usage

### Basic Examples

```bash
# SQLite (read-only)
db-mcp-server --database sqlite:data.db

# SQLite (writable)
db-mcp-server --database sqlite:data.db?writable=true

# PostgreSQL (specific database)
db-mcp-server --database postgres://user:pass@localhost/mydb

# PostgreSQL (server-level, no default database)
db-mcp-server --database postgres://user:pass@localhost:5432?writable=true

# MySQL with write access
db-mcp-server --database mysql://user:pass@localhost/mydb?writable=true

# Multiple databases
db-mcp-server -d db1=sqlite:one.db -d db2=postgres://localhost/two

# HTTP mode (with SSE)
db-mcp-server --transport http --database sqlite:data.db

# HTTP mode with custom host/port
db-mcp-server --transport http --database sqlite:data.db --http-host 0.0.0.0 --http-port 3000
```

### Connection String Format

```
# SQLite
sqlite:path/to/db.db
sqlite:path/to/db.db?writable=true

# PostgreSQL
postgres://user:pass@host:port/database
postgres://user:pass@host:5432                    # server-level
postgres://user:pass@host/database?writable=true

# MySQL
mysql://user:pass@host:port/database
mysql://user:pass@host:3306                       # server-level
mysql://user:pass@host/database?writable=true
```

## MCP Tools

### Read-Only Tools

| Tool | Description |
|------|-------------|
| `list_connections` | List available database connections with type and read-only status |
| `query` | Execute SELECT queries with optional output formatting (json/table/markdown) |
| `list_tables` | List tables and views in a database |
| `describe_table` | Get detailed table schema (columns, primary keys, foreign keys, indexes) |
| `list_databases` | List all databases on server (MySQL/PostgreSQL only) |
| `explain` | Show query execution plan without executing |
| `list_transactions` | List all active transactions with duration |

### Write Tools (require `read_only: false`)

| Tool | Description |
|------|-------------|
| `execute` | Execute INSERT/UPDATE/DELETE/DDL statements |
| `begin_transaction` | Start a new transaction (returns transaction_id) |
| `commit` | Commit a transaction by transaction_id |
| `rollback` | Rollback a transaction by transaction_id |

### Key Features

- **Parameterized queries**: Use `?` or `$1, $2, ...` placeholders with `params` array
- **Output formatting**: `query` and `explain` support `format` parameter (json, table, markdown)
- **Context control**: Use `-A`, `-B`, `-C` parameters for context lines around query results
- **Transaction workflow**: `begin_transaction` → `query`/`execute` with `transaction_id` → `commit`/`rollback`
- **Dangerous operation protection**: DROP, TRUNCATE, DELETE/UPDATE without WHERE require `dangerous_operation_allowed: true`
- **Server-level operations**: Use `database` parameter to target specific database for server-level connections

## Claude Desktop Configuration

Add to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "database": {
      "command": "db-mcp-server",
      "args": [
        "--database", "sqlite:/path/to/your.db?writable=true"
      ]
    }
  }
}
```

Or for multiple databases:

```json
{
  "mcpServers": {
    "database": {
      "command": "db-mcp-server",
      "args": [
        "-d", "app=sqlite:app.db?writable=true",
        "-d", "analytics=postgres://user:pass@localhost/analytics"
      ]
    }
  }
}
```

Or using environment variables:

```json
{
  "mcpServers": {
    "database": {
      "command": "db-mcp-server",
      "env": {
        "MCP_DATABASE": "mysql://root:password@localhost/mydb?writable=true"
      }
    }
  }
}
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `MCP_DATABASE` | Database connection string | - |
| `MCP_TRANSPORT` | Transport mode (stdio/http) | stdio |
| `MCP_HTTP_HOST` | HTTP bind host | 127.0.0.1 |
| `MCP_HTTP_PORT` | HTTP bind port | 8080 |
| `MCP_LOG_LEVEL` | Log level (trace/debug/info/warn/error) | info |

## Architecture

### Connection Management

- **ConnectionManager**: Central registry for all database connections
- **DbPool**: Database-specific pool enum (MySqlPool, PgPool, SqlitePool)
- **Two connection modes**:
  - **Direct database**: Single pool for specific database
  - **Server-level**: Lazy per-database pool creation for server-level connections

### Transaction Management

- **Per-transaction locking**: Each transaction uses `Arc<Mutex<TxEntry>>` instead of global locking
- **Automatic cleanup**: Expired transactions cleaned up every 5 seconds
- **Timeout management**: Default 60s, max 300s
- **Panic-safe**: `PoolGuard` ensures cleanup even on panic

### Security Features

- **Read-only by default**: Connections require `?writable=true` for write operations
- **SQL injection protection**: Parameterized queries with type-safe binding
- **Dangerous operation guard**: AST-based detection prevents:
  - DROP DATABASE/TABLE/INDEX
  - ALTER TABLE DROP COLUMN
  - TRUNCATE
  - DELETE/UPDATE without WHERE clause
- **SQL validation**: AST parsing ensures only SELECT in read-only queries

## Development

### Build Commands

```bash
# Build release version
cargo build --release

# Run all tests
cargo test

# Run specific test
cargo test test_query_server_level

# Run with MySQL (requires local server)
TEST_MYSQL_URL="mysql://root:root@localhost:3306?writable=true" cargo test

# Lint with clippy
cargo clippy -- -D warnings

# Format code
cargo fmt

# Check formatting
cargo fmt -- --check
```

### Testing

**Test Organization**
- Unit tests: Inline in source files
- Integration tests: `tests/` directory
- Guard tests: `tests/dangerous_guard_test.rs`
- Transaction tests: `tests/transaction_test.rs`
- Fuzz tests: `tests/fuzz_test.rs`, `tests/fuzz_with_db_test.rs`

**Running Database-Specific Tests**

MySQL and PostgreSQL tests require environment variables:

```bash
# MySQL tests
TEST_MYSQL_URL="mysql://root:password@localhost:3306?writable=true" cargo test

# PostgreSQL tests
TEST_POSTGRES_URL="postgres://user:pass@localhost:5432?writable=true" cargo test
```

SQLite tests use temporary databases via `tempfile` crate.

### Code Style

- **Rustfmt**: Edition 2024, max width 100, tab spaces 4
- **Clippy**: MSRV 1.85.0, cognitive complexity ≤ 25
- **Error handling**: All errors use `thiserror` with actionable messages

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

Make sure to:
- Run `cargo fmt` before committing
- Pass all tests with `cargo test`
- Pass clippy checks with `cargo clippy -- -D warnings`

## License

MIT
