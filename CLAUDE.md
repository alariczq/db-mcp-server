# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Development Commands

```bash
cargo build                              # Debug build
cargo build --release                    # Release build
cargo test                               # Run all tests
cargo test --test sqlite_writable_test   # Run a single integration test file
cargo test --test clickhouse_test        # Run ClickHouse integration tests
cargo test --test clickhouse_syntax_test # Run ClickHouse syntax validation tests
cargo test --lib                         # Run unit tests only
cargo test test_name                     # Run tests matching a name
cargo clippy                             # Lint
cargo fmt                                # Format
cargo fmt -- --check                     # Check formatting without modifying
```

Some integration tests require external databases via environment variables:
- `TEST_MYSQL_URL` - MySQL connection URL for transaction/MySQL-specific tests
- ClickHouse tests require a running ClickHouse server at `localhost:8123` (no password)

## Architecture

Rust MCP server exposing SQL database operations (SQLite, PostgreSQL, MySQL, ClickHouse) as tools for AI assistants via the rmcp framework.

### Layer Structure

1. **Transport** (`src/transport/`) - stdio or HTTP+SSE entry points. HTTP supports Bearer token auth (`src/auth.rs`).
2. **MCP Service** (`src/mcp/service.rs`) - `DbService` is the central struct. Uses rmcp's `#[tool_router]` and `#[tool_handler]` macros to register 14 tools and implement `ServerHandler`. Each tool method delegates to a handler in `src/tools/`.
3. **Tool Handlers** (`src/tools/`) - Business logic per tool category. Each handler receives shared `Arc<ConnectionManager>` and `Arc<TransactionRegistry>`.
4. **Database Layer** (`src/db/`) - `ConnectionManager` holds named `DbPool` instances (enum over sqlx pool types). `TransactionRegistry` tracks active transactions with timeout cleanup. `database_pool.rs` manages lazy per-database pools for server-level connections.

### Key Design Patterns

- Connections are read-only by default. Write requires `?writable=true` in the connection URL.
- SQL validation uses `sqlparser` AST analysis (`src/tools/sql_validator.rs`, `src/tools/guard.rs`) to block dangerous operations (DROP, TRUNCATE, unqualified DELETE/UPDATE) before execution.
- Tool input/output types live alongside their handler in `src/tools/*.rs`, using `schemars::JsonSchema` for MCP schema generation. All JsonSchema structs MUST have `#[schemars(transform = schemars::transform::RestrictFormats::default())]` to strip `format` fields (e.g. `"format": "uint32"`) that break stdio-based MCP clients.
- `DbPool` enum wraps sqlx's typed pools for SQLite/PostgreSQL/MySQL, and stores a ClickHouse client + address tuple for ClickHouse connections.
- `src/db/executor.rs` dispatches queries across database backends, with special HTTP-based handling for ClickHouse.
- `src/db/types.rs` handles cross-database type mapping and value conversion.
- Config parsing (`src/config.rs`) uses clap with env var fallbacks (`MCP_DATABASE`, `MCP_TRANSPORT`, etc.).
- ClickHouse-specific SQL syntax is preprocessed (`src/tools/sql_validator.rs::preprocess_clickhouse_sql`) to support ANY JOIN, ASOF JOIN, WITH subqueries, and other ClickHouse-only features.

### Adding a New Tool

1. Create input/output types with `JsonSchema` + `Deserialize`/`Serialize` in `src/tools/`.
2. Add `#[schemars(transform = schemars::transform::RestrictFormats::default())]` to every JsonSchema struct.
3. Implement the handler method.
4. Add the tool method to `DbService` in `src/mcp/service.rs` with `#[tool(description = "...")]`.
5. The `#[tool_router]` macro auto-registers it.

## Code Style

- Max line width: 100 (rustfmt.toml)
- Cognitive complexity threshold: 25 (clippy.toml)
- Errors use `DbError` enum (`src/error.rs`) with `thiserror` derive; converted to MCP `ErrorData` via `From` impl.
