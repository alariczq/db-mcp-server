# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DB MCP Server is a Rust-based MCP (Model Context Protocol) server that enables AI assistants to interact with SQL databases (SQLite, PostgreSQL, MySQL). It uses the `rmcp` crate for MCP protocol implementation.

## Build & Development Commands

```bash
# Build
cargo build                    # Debug build
cargo build --release          # Release build

# Run (requires at least one database)
cargo run -- --database sqlite:test.db
cargo run -- --database mydb=postgres://user:pass@localhost/db?writable=true
cargo run -- --transport http --database mysql://host/db  # HTTP mode

# Lint & Format
cargo fmt                      # Format code
cargo clippy                   # Run lints (must pass with zero warnings)

# Test
cargo test                     # Run all tests
cargo test <test_name>         # Run specific test
cargo test --test transaction_test  # Run specific integration test
TEST_MYSQL_URL="mysql://root:root@localhost:3306/test?writable=true" cargo test  # With MySQL
```

## Architecture

### Core Components

- **`src/main.rs`**: CLI entry point using `clap`. Parses database URLs from `--database` flags and starts the appropriate transport.

- **`src/mcp/service.rs`**: Central MCP service (`DbService`) with all tool definitions using `rmcp` macros (`#[tool_router]`, `#[tool]`). Tools are named without `db_` prefix (e.g., `query`, `execute`, `list_tables`).

- **`src/db/`**: Database abstraction layer
  - `pool.rs`: `ConnectionManager` manages connection pools via `sqlx`
  - `executor.rs`: Query execution with parameterized queries
  - `transaction_registry.rs`: Stateful transaction management with auto-cleanup
  - `macros.rs`: `db_dispatch!` macro for database-type polymorphism

- **`src/tools/`**: Tool handlers (query, schema, transaction, write) with input/output types

- **`src/transport/`**: Transport implementations
  - `stdio.rs`: Standard input/output (default, for CLI integration)
  - `http.rs`: HTTP with SSE streaming via `axum`

### Key Patterns

1. **Database Dispatch Macro**: Use `db_dispatch!` for code that varies by database type:
   ```rust
   db_dispatch!(pool, conn, {
       // Use `conn` which is the appropriate connection type
   })
   ```

2. **Connection Validation**: All tools call `validate_connection_id()` to ensure connection_id is provided.

3. **Writable Flag**: Database URLs support `?writable=true` to enable write operations. Read-only by default for safety.

4. **Server-Level Connections**: URLs without database path (e.g., `mysql://host:3306`) set `server_level: true`, requiring `schema` parameter for table operations.

## Code Quality Requirements

From project constitution (`.specify/memory/constitution.md`):

- All code must pass `cargo clippy` with zero warnings
- Code must be formatted with `cargo fmt`
- Public APIs require doc comments (`///`)
- No `.unwrap()` in production code (tests excepted)
- Functions should not exceed 50 lines
- Use `Result` types for error handling

### Comment Guidelines

**Avoid trivial inline comments** that merely restate what the code already expresses clearly:

```rust
// BAD - these comments add no value:
// Create the connection pool
let pool = self.create_pool(&config).await?;

// GOOD - no comment needed, code is self-explanatory:
let pool = self.create_pool(&config).await?;
```

**Keep comments that explain WHY**, not WHAT:
- Business logic rationale
- Non-obvious behavior or edge cases
- Database-specific quirks (e.g., `// SQLite's NUMERIC is actually a float`)
- Important constraints or invariants

**Doc comments (`///`) guidelines**:
- **Required**: Public APIs (structs, enums, public functions) - but only at the type level
- **Optional**: Private functions - only add if the function name doesn't convey its purpose
- **Skip**: Simple private helpers like `fn decode_integer()`, `fn format_value()` - the name is enough
- **Skip**: Enum variants and struct fields when the name is self-explanatory
- **Add**: Complex functions that need to explain implementation details or non-obvious behavior

```rust
// BAD - redundant variant/field comments:
pub enum DbPool {
    /// MySQL connection pool
    MySql(MySqlPool),
    /// PostgreSQL connection pool
    Postgres(PgPool),
}

// GOOD - no variant comments needed, names are clear:
pub enum DbPool {
    MySql(MySqlPool),
    Postgres(PgPool),
}

// GOOD - doc comment explains non-obvious behavior:
/// Generate EXPLAIN SQL with database-specific syntax.
/// - SQLite: Uses `EXPLAIN QUERY PLAN` for SELECT, `EXPLAIN` for writes
/// - MySQL/PostgreSQL: Uses `EXPLAIN` directly
fn generate_explain_sql(pool: &DbPool, sql: &str) -> String { ... }
```

## MCP Tools

| Tool | Description |
|------|-------------|
| `list_connections` | List available database connections |
| `query` | Execute SELECT queries (supports parameterized queries) |
| `execute` | Execute write operations (INSERT/UPDATE/DELETE/DDL) |
| `list_tables` | List tables/views in a database |
| `describe_table` | Get table schema details |
| `list_databases` | List databases (MySQL/PostgreSQL only) |
| `begin_transaction` | Start a transaction |
| `commit` | Commit a transaction |
| `rollback` | Rollback a transaction |

## Active Technologies
- Rust 2024 Edition (stable) + rmcp 0.12.0 (MCP framework), sqlx 0.8 (database), tokio (async runtime), schemars (JSON schema) (002-transaction-explain-tools)
- N/A (uses existing connection pools) (002-transaction-explain-tools)
- Rust 2024 Edition + sqlx 0.8, rmcp 0.12.0, schemars, serde (003-code-refactor)
- N/A (refactoring only) (003-code-refactor)
- Rust 2024 Edition (stable) + rmcp 0.12.0 (MCP framework), schemars (JSON Schema generation) (004-mcp-tool-descriptions)
- N/A (documentation-only changes) (004-mcp-tool-descriptions)
- Rust 2024 Edition (stable) + rmcp 0.12.0 (MCP framework), sqlparser 0.60 (SQL parsing), sqlx 0.8 (database), schemars 1.0 (JSON schema) (005-dangerous-operation-guard)
