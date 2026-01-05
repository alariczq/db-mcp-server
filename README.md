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

### Single Database

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
```

### Multiple Databases

你可以通过以下三种方式配置多个数据库：

**方式 1：命令行重复 `-d` 参数**

```bash
# 使用默认 ID（从数据库名自动推导）
db-mcp-server -d sqlite:one.db -d postgres://localhost/two

# 使用自定义 ID（推荐）
db-mcp-server -d app=sqlite:app.db?writable=true -d analytics=postgres://user:pass@localhost/analytics

# 混合使用
db-mcp-server \
  -d main=sqlite:main.db?writable=true \
  -d reports=mysql://user:pass@localhost/reports \
  -d logs=postgres://user:pass@localhost/logs?writable=true
```

**方式 2：环境变量（逗号分隔）**

```bash
# 单个数据库
export MCP_DATABASE="sqlite:data.db?writable=true"
db-mcp-server

# 多个数据库
export MCP_DATABASE="\
app=sqlite:app.db?writable=true,\
analytics=postgres://user:pass@localhost/analytics,\
logs=mysql://user:pass@localhost/logs?writable=true"
db-mcp-server
```

**方式 3：Claude Desktop 配置**

见下方 [Claude Desktop Configuration](#claude-desktop-configuration) 部分。

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

# 命名连接（多数据库时推荐）
id=<connection_string>
app=sqlite:app.db?writable=true
analytics=postgres://user:pass@localhost/analytics
```

### HTTP Mode

```bash
# HTTP mode (with SSE)
db-mcp-server --transport http --database sqlite:data.db

# HTTP mode with custom host/port
db-mcp-server --transport http --database sqlite:data.db --http-host 0.0.0.0 --http-port 3000
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

Or using environment variables (single database):

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

Or using environment variables (multiple databases, comma-separated):

```json
{
  "mcpServers": {
    "database": {
      "command": "db-mcp-server",
      "env": {
        "MCP_DATABASE": "app=sqlite:app.db?writable=true,analytics=postgres://user:pass@localhost/analytics,logs=mysql://user:pass@localhost/logs"
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

## Development

```bash
# Build from source
cargo build --release

# Run tests
cargo test
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT
