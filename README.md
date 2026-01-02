# DB MCP Server

A Rust-based [MCP](https://modelcontextprotocol.io/) server that enables AI assistants to interact with SQL databases.

## Features

- **Multi-database support**: SQLite, PostgreSQL, MySQL
- **Read-only by default**: Write operations require explicit opt-in
- **Transaction support**: Full ACID transaction management
- **Two transport modes**: stdio (CLI) and HTTP (SSE)
- **Parameterized queries**: SQL injection protection built-in

## Installation

```bash
cargo install --path .
```

Or build from source:

```bash
cargo build --release
```

## Usage

```bash
# SQLite
db-mcp-server --database sqlite:data.db

# PostgreSQL
db-mcp-server --database postgres://user:pass@localhost/mydb

# MySQL with write access
db-mcp-server --database mysql://user:pass@localhost/mydb?writable=true

# Multiple databases
db-mcp-server -d db1=sqlite:one.db -d db2=postgres://localhost/two

# HTTP mode
db-mcp-server --transport http --database sqlite:data.db
```

## MCP Tools

| Tool | Description |
|------|-------------|
| `list_connections` | List available database connections |
| `query` | Execute SELECT queries |
| `execute` | Execute INSERT/UPDATE/DELETE/DDL (requires writable) |
| `list_tables` | List tables and views |
| `describe_table` | Get table schema |
| `list_databases` | List databases (MySQL/PostgreSQL) |
| `begin_transaction` | Start a transaction |
| `commit` | Commit a transaction |
| `rollback` | Rollback a transaction |

## Claude Desktop Configuration

Add to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "database": {
      "command": "db-mcp-server",
      "args": ["--database", "sqlite:/path/to/your.db"]
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
| `MCP_LOG_LEVEL` | Log level | info |

## License

MIT
