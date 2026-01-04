# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Rust-based MCP (Model Context Protocol) server that enables AI assistants to interact with SQL databases (SQLite, PostgreSQL, MySQL). The server provides two transport modes: stdio for CLI integration and HTTP with Server-Sent Events for web clients.

## Build and Development Commands

```bash
# Build the project
cargo build --release

# Run tests
cargo test

# Run specific test
cargo test <test_name>

# Run tests with MySQL (requires local MySQL server)
TEST_MYSQL_URL="mysql://root:root@localhost:3306?writable=true" cargo test test_query_server_level

# Lint with clippy
cargo clippy -- -D warnings

# Format code
cargo fmt

# Install locally
cargo install --path .

# Run server in stdio mode
db-mcp-server --database sqlite:data.db

# Run server in HTTP mode
db-mcp-server --transport http --database sqlite:data.db
```

## Architecture

### Core Components

**Connection Management (`src/db/pool.rs`)**
- `ConnectionManager`: Central registry for all database connections
- `DbPool`: Database-specific pool enum (MySqlPool, PgPool, SqlitePool) - avoids sqlx::AnyPool limitations
- `ConnectionPool`: Either direct database connection or server-level manager
- Two connection modes:
  - **Direct database**: Single pool for specific database
  - **Server-level**: Lazy per-database pool creation via `DatabasePoolManager`

**Database Pool Manager (`src/db/database_pool.rs`)**
- Manages lazy per-database pools for server-level connections
- `DatabaseTarget` enum: distinguishes Server vs Database(name) targets
- Concurrency-safe design:
  - `OnceCell` per database for single-flight pool creation
  - `AtomicUsize` for lock-free active count tracking
  - Automatic cleanup of idle pools after 10 minutes
  - `PoolGuard` for panic-safe cleanup

**Transaction Registry (`src/db/transaction_registry.rs`)**
- Stateful transaction management across multiple MCP tool calls
- Per-transaction locking with `Arc<Mutex<TxEntry>>` instead of global locking
- `DbTransaction` enum wraps database-specific transactions (MySql, Postgres, SQLite)
- Automatic cleanup of expired transactions (runs every 5 seconds)
- Default timeout: 60s, max: 300s

**MCP Service (`src/mcp/service.rs`)**
- `DbService`: Main service struct using rmcp framework macros
- Exposes all database tools via MCP protocol
- Tool names use simplified format without `db_` prefix
- Uses `ToolRouter` for automatic dispatch

**Transport Layer (`src/transport/`)**
- `StdioTransport`: stdin/stdout for CLI integration
- `HttpTransport`: HTTP with SSE for web clients
- Both implement common `Transport` trait

**Tools (`src/tools/`)**
- Each tool in separate module: query, execute, schema, transaction, explain
- `guard.rs`: SQL AST-based dangerous operation detection (DROP, TRUNCATE, DELETE/UPDATE without WHERE)
- `sql_validator.rs`: Read-only enforcement using sqlparser AST analysis
- `format.rs`: Output formatting (JSON, ASCII table, markdown)

**Error Handling (`src/error.rs`)**
- All errors use `thiserror` with actionable messages
- Each error variant includes suggestions to help AI assistants recover
- Specific error types: Connection, Database, Permission, Schema, Transaction, Timeout, DangerousOperationBlocked

### Key Design Patterns

**Type System (`src/models/connection.rs` and `src/db/types.rs`)**
- `DatabaseType` enum in `models::connection`: Defines supported databases (PostgreSQL, MySQL, SQLite)
- Database-specific type handling in `src/db/types.rs`: Converts database types to JSON with category-based dispatch
- `DbPool` enum pattern: Match on pool type to dispatch to database-specific implementations

**Parameterized Queries**
- SQL injection protection via `QueryParam` enum and database-specific binding
- See `src/db/params.rs` for parameter binding logic

**Read-only by Default**
- Connections require `?writable=true` query parameter for write operations
- Write tools check connection writability before execution
- SQL validator uses AST parsing to prevent read operations in execute tool

**Server-level Connections**
- Detected by absence of database in connection URL
- Require `schema` parameter for `list_tables`/`describe_table`
- Support `database` parameter to target specific database (creates lazy pool)
- Enable queries like `SELECT 1`, `SHOW DATABASES` without database context

**Dangerous Operation Guard**
- AST-based detection prevents bypass via formatting tricks or SQL comments
- Blocked operations: DROP DATABASE/TABLE/INDEX, ALTER TABLE DROP COLUMN, TRUNCATE, DELETE/UPDATE without WHERE
- Requires explicit `dangerous_operation_allowed: true` flag

## Code Style

**Rustfmt Configuration**
- Edition: 2024
- Max width: 100 characters
- Tab spaces: 4
- Newline style: Unix

**Clippy Configuration**
- MSRV: 1.85.0
- Cognitive complexity threshold: 25
- Too many arguments threshold: 7

## Configuration

**Connection String Format**
```
sqlite:path/to/db.db
postgres://user:pass@host:port/database
mysql://user:pass@host:port/database

# With options
mysql://user:pass@host/db?writable=true
postgres://user:pass@host:5432?writable=true

# Server-level (no database)
mysql://user:pass@host:3306
postgres://user:pass@host:5432
```

**Environment Variables**
- `MCP_DATABASE`: Database connection string
- `MCP_TRANSPORT`: Transport mode (stdio/http)
- `MCP_HTTP_HOST`: HTTP bind host (default: 127.0.0.1)
- `MCP_HTTP_PORT`: HTTP bind port (default: 8080)
- `MCP_LOG_LEVEL`: Log level (default: info)

## Testing

**Test Organization**
- Unit tests: Inline in source files or `tests/` directory
- Integration tests: `tests/` directory
- Fuzz tests: `tests/fuzz_test.rs`, `tests/fuzz_with_db_test.rs`

**Key Test Patterns**
- Use `tempfile` for temporary SQLite databases
- Use `tokio-test` for async testing
- MySQL/PostgreSQL tests require environment variables (e.g., `TEST_MYSQL_URL`)
- Guard tests: `tests/dangerous_guard_test.rs`
- Transaction tests: `tests/transaction_test.rs`

## Common Workflows

**Adding a New Tool**
1. Create module in `src/tools/`
2. Define input/output structs with `serde::Serialize` and `schemars::JsonSchema`
3. Implement tool handler logic
4. Add tool to `src/mcp/service.rs` using `#[tool]` macro
5. Add tests in `tests/`

**Adding Database Support**
1. Update `DbPool` enum in `src/db/pool.rs`
2. Add connection logic in `ConnectionManager::connect()`
3. Update `DbTransaction` enum in `src/db/transaction_registry.rs`
4. Add database-specific parameter binding in `src/db/params.rs`
5. Update schema introspection in `src/db/schema.rs`

**Modifying SQL Validation**
- Read-only validation: `src/tools/sql_validator.rs` (uses sqlparser AST)
- Dangerous operation detection: `src/tools/guard.rs` (uses sqlparser AST)
- Both use AST analysis to prevent bypass via formatting or comments
