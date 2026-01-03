//! MCP service implementation using rmcp.
//!
//! This module defines the DbService struct with all database tools
//! exposed via the MCP protocol using the rmcp framework's macros.
//! Tool names use simplified format without `db_` prefix.

use crate::db::{ConnectionManager, ConnectionSummary, TransactionRegistry};
use crate::tools::explain::{ExplainInput, ExplainOutput, ExplainToolHandler};
use crate::tools::query::{QueryInput, QueryOutput, QueryToolHandler};
use crate::tools::schema::{
    DescribeTableInput, DescribeTableOutput, ListDatabasesInput, ListDatabasesOutput,
    ListTablesInput, ListTablesOutput, SchemaToolHandler,
};
use crate::tools::transaction::{
    BeginTransactionInput, BeginTransactionOutput, CommitInput, CommitOutput,
    ListTransactionsInput, ListTransactionsOutput, RollbackInput, RollbackOutput,
    TransactionToolHandler,
};
use crate::tools::write::{ExecuteInput, ExecuteOutput, WriteToolHandler};
use rmcp::Json;
use rmcp::{
    ErrorData as McpError, ServerHandler,
    handler::server::tool::ToolRouter,
    handler::server::wrapper::Parameters,
    model::{Implementation, ProtocolVersion, ServerCapabilities, ServerInfo},
    schemars::JsonSchema,
    tool, tool_handler, tool_router,
};
use serde::Serialize;
use std::sync::Arc;

/// Output for the list_connections tool.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ListConnectionsOutput {
    /// List of available database connections
    pub connections: Vec<ConnectionSummary>,
    /// Number of connections
    pub count: usize,
}

#[derive(Clone)]
pub struct DbService {
    /// Shared connection manager for all database operations
    connection_manager: Arc<ConnectionManager>,
    /// Shared transaction registry for transaction management
    transaction_registry: Arc<TransactionRegistry>,
    /// Default query timeout in seconds (from config)
    default_query_timeout_secs: u64,
    /// Default row limit for queries (from config)
    default_row_limit: u32,
    /// Tool router for MCP tool dispatch (auto-generated)
    tool_router: ToolRouter<Self>,
}

/// Default query timeout in seconds.
const DEFAULT_QUERY_TIMEOUT_SECS: u64 = 30;
/// Default row limit for queries.
const DEFAULT_ROW_LIMIT: u32 = 100;

impl DbService {
    /// Create a new DbService instance with default timeouts.
    ///
    /// # Arguments
    ///
    /// * `connection_manager` - Shared connection manager for database operations
    /// * `transaction_registry` - Shared transaction registry for transaction management
    pub fn new(
        connection_manager: Arc<ConnectionManager>,
        transaction_registry: Arc<TransactionRegistry>,
    ) -> Self {
        Self {
            connection_manager,
            transaction_registry,
            default_query_timeout_secs: DEFAULT_QUERY_TIMEOUT_SECS,
            default_row_limit: DEFAULT_ROW_LIMIT,
            tool_router: Self::tool_router(),
        }
    }

    /// Create a new DbService instance with custom timeout configuration.
    ///
    /// # Arguments
    ///
    /// * `connection_manager` - Shared connection manager for database operations
    /// * `transaction_registry` - Shared transaction registry for transaction management
    /// * `query_timeout_secs` - Default timeout for queries in seconds
    /// * `row_limit` - Default row limit for queries
    pub fn with_config(
        connection_manager: Arc<ConnectionManager>,
        transaction_registry: Arc<TransactionRegistry>,
        query_timeout_secs: u64,
        row_limit: u32,
    ) -> Self {
        Self {
            connection_manager,
            transaction_registry,
            default_query_timeout_secs: query_timeout_secs,
            default_row_limit: row_limit,
            tool_router: Self::tool_router(),
        }
    }

    /// Validate connection ID - ensure it is provided and non-empty.
    ///
    /// Returns the trimmed connection ID if valid, otherwise returns an error
    /// guiding the user to call list_connections first.
    fn validate_connection_id(&self, provided: &str) -> Result<String, McpError> {
        let trimmed = provided.trim();
        if trimmed.is_empty() {
            Err(McpError::invalid_params(
                "connection_id is required. Call list_connections first to get available database IDs.",
                None,
            ))
        } else {
            Ok(trimmed.to_string())
        }
    }
}

#[tool_router]
impl DbService {
    #[tool(
        description = "List all available database connections.\nReturns connection IDs, types (MySQL/PostgreSQL/SQLite), and read-only status."
    )]
    async fn list_connections(&self) -> Json<ListConnectionsOutput> {
        let connections = self.connection_manager.list_connections_detail().await;
        let count = connections.len();
        Json(ListConnectionsOutput { connections, count })
    }

    #[tool(
        description = "Execute a SELECT query and return results.\nSupports parameterized queries to prevent SQL injection.\nOutput format: json (default), table, or markdown.\nCan run within a transaction using transaction_id.\nFor server-level connections (no default database), specify database in SQL: use `db.table` syntax or `SHOW TABLES FROM db`."
    )]
    async fn query(
        &self,
        Parameters(input): Parameters<QueryInput>,
    ) -> Result<Json<QueryOutput>, McpError> {
        let mut input = input;
        input.connection_id = self.validate_connection_id(&input.connection_id)?;
        let handler = QueryToolHandler::with_defaults(
            self.connection_manager.clone(),
            self.transaction_registry.clone(),
            self.default_query_timeout_secs,
            self.default_row_limit,
        );
        handler.query(input).await.map(Json).map_err(Into::into)
    }

    #[tool(
        description = "List all databases on the server.\nSupported for MySQL and PostgreSQL. SQLite returns an error (file-based)."
    )]
    async fn list_databases(
        &self,
        Parameters(input): Parameters<ListDatabasesInput>,
    ) -> Result<Json<ListDatabasesOutput>, McpError> {
        let mut input = input;
        input.connection_id = self.validate_connection_id(&input.connection_id)?;
        let handler = SchemaToolHandler::new(self.connection_manager.clone());
        handler
            .list_databases(input)
            .await
            .map(Json)
            .map_err(Into::into)
    }

    #[tool(
        description = "List all tables and views in the database.\nCan filter by schema name.\nServer-level connections (without database in URL) require `schema` parameter."
    )]
    async fn list_tables(
        &self,
        Parameters(input): Parameters<ListTablesInput>,
    ) -> Result<Json<ListTablesOutput>, McpError> {
        let mut input = input;
        input.connection_id = self.validate_connection_id(&input.connection_id)?;
        let handler = SchemaToolHandler::new(self.connection_manager.clone());
        handler
            .list_tables(input)
            .await
            .map(Json)
            .map_err(Into::into)
    }

    #[tool(
        description = "Get detailed schema information for a table.\nReturns columns, primary keys, foreign keys, and indexes.\nServer-level connections (without database in URL) require `schema` parameter."
    )]
    async fn describe_table(
        &self,
        Parameters(input): Parameters<DescribeTableInput>,
    ) -> Result<Json<DescribeTableOutput>, McpError> {
        let mut input = input;
        input.connection_id = self.validate_connection_id(&input.connection_id)?;
        let handler = SchemaToolHandler::new(self.connection_manager.clone());
        handler
            .describe_table(input)
            .await
            .map(Json)
            .map_err(Into::into)
    }

    #[tool(
        description = "Execute a write operation (INSERT, UPDATE, DELETE, DDL).\nRequires read-write connection (read_only: false).\nCan run within a transaction using transaction_id.\nDangerous operations return warning instead of executing: DROP, TRUNCATE, DELETE/UPDATE without WHERE.\nFor server-level connections, use `db.table` syntax or `USE database_name` first."
    )]
    async fn execute(
        &self,
        Parameters(input): Parameters<ExecuteInput>,
    ) -> Result<Json<ExecuteOutput>, McpError> {
        let mut input = input;
        input.connection_id = self.validate_connection_id(&input.connection_id)?;
        let handler = WriteToolHandler::with_defaults(
            self.connection_manager.clone(),
            self.transaction_registry.clone(),
            self.default_query_timeout_secs,
        );
        handler.execute(input).await.map(Json).map_err(Into::into)
    }

    #[tool(
        description = "Begin a new database transaction.\nRequires read-write connection. Returns transaction_id for commit/rollback."
    )]
    async fn begin_transaction(
        &self,
        Parameters(input): Parameters<BeginTransactionInput>,
    ) -> Result<Json<BeginTransactionOutput>, McpError> {
        let mut input = input;
        input.connection_id = self.validate_connection_id(&input.connection_id)?;
        let handler = TransactionToolHandler::new(
            self.connection_manager.clone(),
            self.transaction_registry.clone(),
        );
        handler
            .begin_transaction(input)
            .await
            .map(Json)
            .map_err(Into::into)
    }

    #[tool(description = "Commit a transaction.\nUse transaction_id from begin_transaction.")]
    async fn commit(
        &self,
        Parameters(input): Parameters<CommitInput>,
    ) -> Result<Json<CommitOutput>, McpError> {
        let handler = TransactionToolHandler::new(
            self.connection_manager.clone(),
            self.transaction_registry.clone(),
        );
        handler.commit(input).await.map(Json).map_err(Into::into)
    }

    #[tool(description = "Rollback a transaction.\nUse transaction_id from begin_transaction.")]
    async fn rollback(
        &self,
        Parameters(input): Parameters<RollbackInput>,
    ) -> Result<Json<RollbackOutput>, McpError> {
        let handler = TransactionToolHandler::new(
            self.connection_manager.clone(),
            self.transaction_registry.clone(),
        );
        handler.rollback(input).await.map(Json).map_err(Into::into)
    }

    #[tool(
        description = "List all active database transactions.\nReturns transaction IDs, connection IDs, start times, and duration.\nLong-running transactions (>5 minutes) are flagged."
    )]
    async fn list_transactions(
        &self,
        Parameters(input): Parameters<ListTransactionsInput>,
    ) -> Result<Json<ListTransactionsOutput>, McpError> {
        let handler = TransactionToolHandler::new(
            self.connection_manager.clone(),
            self.transaction_registry.clone(),
        );
        handler
            .list_transactions(input)
            .await
            .map(Json)
            .map_err(Into::into)
    }

    #[tool(
        description = "Show query execution plan without executing the query.\nSupports SELECT, INSERT, UPDATE, and DELETE statements.\nUseful for understanding query performance and index usage.\nOutput format: \"json\" returns structured data, \"table\" returns ASCII table, \"markdown\" returns markdown table."
    )]
    async fn explain(
        &self,
        Parameters(input): Parameters<ExplainInput>,
    ) -> Result<Json<ExplainOutput>, McpError> {
        let mut input = input;
        input.connection_id = self.validate_connection_id(&input.connection_id)?;
        let handler = ExplainToolHandler::new(
            self.connection_manager.clone(),
            self.transaction_registry.clone(),
        );
        handler.explain(input).await.map(Json).map_err(Into::into)
    }
}

#[tool_handler]
impl ServerHandler for DbService {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::V_2025_03_26,
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: Implementation {
                name: "db-mcp-server".to_owned(),
                title: Some("DB MCP Server".to_owned()),
                version: env!("CARGO_PKG_VERSION").to_owned(),
                icons: None,
                website_url: None,
            },
            instructions: Some(
                "Database tools for querying and managing SQL databases.\n\
                \n\
                ## Workflow\n\
                1. Call `list_connections` to get available database IDs\n\
                2. Use the `connection_id` from step 1 in all other tool calls\n\
                3. For write operations, ensure the connection is read-write (read_only: false)\n\
                \n\
                ## Transaction Workflow\n\
                1. `begin_transaction` → returns transaction_id\n\
                2. `query`/`execute` with transaction_id → operations within transaction\n\
                3. `commit` or `rollback` with transaction_id → finalize\n\
                \n\
                ## Tools by Category\n\
                - **Read-only**: query, list_tables, describe_table, list_databases, explain\n\
                - **Write** (requires read_only: false): execute, begin_transaction, commit, rollback\n\
                - **Utility**: list_connections, list_transactions\n\
                \n\
                ## Connection Types\n\
                - **Read-only**: read_only: true in list_connections output\n\
                - **Read-write**: read_only: false, can use write tools\n\
                - **Server-level**: server_level: true, no default database; requires `schema` parameter for list_tables/describe_table, or use `db.table` syntax in queries\n\
                \n\
                ## Database-Specific Notes\n\
                - MySQL: Cross-database queries supported (use `db.table` syntax)\n\
                - PostgreSQL: Queries cannot span databases\n\
                - SQLite: list_databases not supported (file-based)"
                    .to_string(),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_service() -> DbService {
        let manager = Arc::new(ConnectionManager::new());
        let registry = Arc::new(TransactionRegistry::new());
        DbService::new(manager, registry)
    }

    #[test]
    fn test_db_service_creation() {
        let _service = create_test_service();
    }

    #[test]
    fn test_validate_connection_id_with_valid() {
        let service = create_test_service();
        assert_eq!(service.validate_connection_id("mydb").unwrap(), "mydb");
    }

    #[test]
    fn test_validate_connection_id_trims_whitespace() {
        let service = create_test_service();
        assert_eq!(service.validate_connection_id("  mydb  ").unwrap(), "mydb");
    }

    #[test]
    fn test_validate_connection_id_rejects_empty() {
        let service = create_test_service();
        let err = service.validate_connection_id("").unwrap_err();
        assert!(err.to_string().contains("connection_id is required"));
    }

    #[test]
    fn test_validate_connection_id_rejects_whitespace_only() {
        let service = create_test_service();
        let err = service.validate_connection_id("   ").unwrap_err();
        assert!(err.to_string().contains("connection_id is required"));
    }

    #[test]
    fn test_server_info() {
        let service = create_test_service();
        let info = service.get_info();
        // from_build_env() uses rmcp's own package name, which is expected
        assert!(!info.server_info.name.is_empty());
        assert!(info.capabilities.tools.is_some());
    }
}
