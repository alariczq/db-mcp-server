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
- Requires explicit `skip_sql_check: true` flag

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

**Comment Guidelines**

*Clear code is better than extensive comments. No comments is better than outdated, invalid comments.*

**Core Principles**
1. **Clean code first**: Refactor unclear code into well-named functions/types before adding comments
2. **Comments rot**: Every comment is a maintenance burden. Prefer self-documenting code.
3. **No invalid comments**: Outdated comments are worse than no comments. Delete immediately when code changes.
4. **WHY, not WHAT**: If you must comment, explain reasoning/invariants, never mechanics.

**When to Use Comments (Decision Tree)**

```
Is this a public API item? ────YES──→ Use `///` documentation (REQUIRED)
         │
         NO
         ↓
Is the logic complex/non-obvious? ────NO──→ NO COMMENT (use clear naming)
         │
        YES
         ↓
Can you refactor into smaller,     ────YES──→ REFACTOR instead of commenting
well-named functions?
         │
         NO
         ↓
Does it involve:
- Subtle invariants (e.g., lock ordering, field constraints)
- Performance trade-offs (explain why unusual approach)
- Safety requirements (memory ordering, race conditions)
- Bug workarounds (link to issue/explanation)
         │
        YES──→ ADD COMMENT (keep it brief)
```

**Required: Public API Documentation**
Use `///` for all `pub` items: modules, structs, enums, functions, traits, constants, macros.
- Include `# Errors`, `# Panics`, `# Safety` where applicable
- Add examples for non-trivial APIs
- Document each `pub` field

```rust
/// Manages database connection pools with automatic cleanup.
pub struct ConnectionManager { ... }

/// Executes a SQL query with optional parameters.
///
/// # Errors
/// Returns `DbError::Timeout` if query exceeds timeout.
pub async fn query(&self, sql: &str) -> Result<QueryResult> { ... }
```

**Private Items: Default is NO COMMENT**
Private functions, structs, enums, fields should have NO comments by default.
Rely on clear naming and code structure.

```rust
// NO - Redundant comment
/// Checks if connection is expired
fn is_expired(&self) -> bool { ... }

// YES - Clear naming, no comment needed
fn is_expired(&self) -> bool {
    self.last_used.elapsed() > TIMEOUT
}
```

**When to Comment Private Items**
Only add comments for private items in these cases:

1. **Subtle Invariants** - Critical constraints that must be maintained
```rust
struct Registry {
    // INVARIANT: active_count == entries.len()
    entries: Vec<Entry>,
    active_count: AtomicUsize,
}
```

2. **Performance Optimization** - Explain trade-offs for unusual code
```rust
fn cleanup(&mut self) {
    // Use retain() not filter() - runs every 10s, must minimize allocations
    self.pools.retain(|_, e| !e.is_idle());
}
```

3. **Safety Requirements** - Memory ordering, lock ordering, race prevention
```rust
impl Drop for Guard {
    fn drop(&mut self) {
        // Must decrement BEFORE unlock to prevent race with cleanup task
        self.count.fetch_sub(1, Ordering::SeqCst);
    }
}
```

4. **Bug Workarounds** - Code looks wrong but is correct due to external issue
```rust
// sqlx requires explicit close for MySQL pools (issue #1234)
pool.close().await;
```

**Never Comment These**
- Type conversions: `as u64`, `into()`, `from()`
- Simple control flow: `if`, `for`, basic `match`
- Standard patterns: `.map()`, `.filter()`, `?`, `unwrap_or()`
- Obvious operations: assignments, field access, arithmetic
- Variable declarations with clear types/names
- Straightforward function calls

**Inline Comments: Extract Functions Instead**
If you need inline comments to explain code blocks, extract to named functions.

```rust
// NO - Inline comment explaining block
fn process() {
    // Validate input and convert to internal format
    let data = input.trim().to_lowercase();
    if data.is_empty() { return; }
    ...
}

// YES - Extract to self-documenting function
fn process() {
    let data = validate_and_normalize(input);
    if data.is_empty() { return; }
    ...
}
```

**Comment Maintenance**
- Delete outdated comments immediately when code changes
- Never commit commented-out code (use git history)
- During code review: challenge every comment - can code be clearer instead?
- Prefer refactoring over commenting

**Summary: When AI Should Add Comments**
✅ **Always comment**: Public API items (`pub` + `///`)
✅ **Sometimes comment**: Private items with invariants/safety/performance trade-offs
❌ **Never comment**: Standard code, obvious logic, simple private functions
❌ **Never**: Outdated comments, commented-out code

**Code Simplicity Guidelines**

*YAGNI: You Aren't Gonna Need It. Write only what is needed NOW.*

**Avoid Over-Engineering**
- Only implement what the user explicitly requested
- Do NOT add features, abstractions, or flexibility "just in case"
- Do NOT consider future requirements unless user mentions them
- Do NOT add compatibility layers unless specifically requested
- The right amount of code is the MINIMUM needed for the current task

**What NOT to Add (Unless Explicitly Requested)**

1. **Unused Features**
   - Extra function parameters "for future use"
   - Optional behaviors that aren't needed now
   - Configuration options that aren't required
   - Generic/abstract code when specific code works

2. **Unnecessary Compatibility**
   - Backward compatibility for code you just wrote
   - Support for multiple versions/formats when only one is needed
   - Deprecation warnings when removing unused code
   - Migration paths when changing new code

3. **Premature Abstractions**
   - Helper functions used only once
   - Traits/interfaces with single implementation
   - Configuration files for hardcoded values
   - Factories/builders for simple construction

4. **Defensive Programming for Internal Code**
   - Validation of data from trusted internal functions
   - Error handling for "impossible" cases
   - Null/bounds checks when caller guarantees validity
   - Type conversions when types already match

**Examples**

```rust
// BAD - Unused flexibility
fn process_data(data: &[u8], format: OutputFormat, compress: bool, validate: bool) {
    // User only needs basic processing, why add all these options?
}

// GOOD - Only what's needed
fn process_data(data: &[u8]) -> Vec<u8> {
    // Simple, direct, does the job
}

// BAD - Premature abstraction
trait DataProcessor {
    fn process(&self, data: &[u8]) -> Vec<u8>;
}
struct JsonProcessor;
impl DataProcessor for JsonProcessor { ... }

// GOOD - Direct implementation
fn process_json(data: &[u8]) -> Vec<u8> { ... }

// BAD - Unnecessary backward compatibility
fn get_user_name(&self) -> &str { &self.name }
#[deprecated(note = "use get_user_name")]
fn getUserName(&self) -> &str { &self.name }  // Why? Code is new!

// GOOD - Just the new function
fn get_user_name(&self) -> &str { &self.name }

// BAD - Over-validation of internal data
fn calculate_total(items: &[Item]) -> u64 {
    assert!(!items.is_empty(), "items cannot be empty");  // Trust caller
    items.iter().map(|i| i.price).sum()
}

// GOOD - Trust internal callers
fn calculate_total(items: &[Item]) -> u64 {
    items.iter().map(|i| i.price).sum()
}
```

**When Simplicity Rules Don't Apply**
- Public API: Do add proper validation and error handling
- User explicitly requests flexibility/compatibility
- Security-critical code: Validate all inputs
- Code interfacing with external systems/users

**Summary**
✅ Write the simplest code that solves the current problem
✅ Trust internal code and known invariants
✅ Delete unused code completely (no deprecation, no compatibility shims)
❌ Don't add "might need later" features
❌ Don't add compatibility unless explicitly requested
❌ Don't create abstractions for single use cases

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
