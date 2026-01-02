//! MCP service implementation using rmcp.
//!
//! This module defines the DbService struct with all database tools
//! exposed via the MCP protocol using the rmcp framework's macros.
//! Tool names use simplified format without `db_` prefix.

use crate::db::{ConnectionManager, ConnectionSummary, TransactionRegistry};
use crate::error::DbError;
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
    /// Tool router for MCP tool dispatch (auto-generated)
    tool_router: ToolRouter<Self>,
}

impl DbService {
    /// Create a new DbService instance.
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
        let handler = QueryToolHandler::with_transaction_registry(
            self.connection_manager.clone(),
            self.transaction_registry.clone(),
        );
        handler
            .query(input)
            .await
            .map(Json)
            .map_err(|e: DbError| McpError::internal_error(e.to_string(), None))
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
            .map_err(|e: DbError| McpError::internal_error(e.to_string(), None))
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
            .map_err(|e: DbError| McpError::internal_error(e.to_string(), None))
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
            .map_err(|e: DbError| McpError::internal_error(e.to_string(), None))
    }

    #[tool(
        description = "Execute a write operation (INSERT, UPDATE, DELETE).\nRequires read-write connection. Also supports DDL (CREATE, DROP, ALTER, TRUNCATE).\nCan run within a transaction using transaction_id. Returns warning for dangerous operations.\nFor server-level connections (no default database), use fully qualified table names (e.g., `db.table`) or run `USE database_name` first."
    )]
    async fn execute(
        &self,
        Parameters(input): Parameters<ExecuteInput>,
    ) -> Result<Json<ExecuteOutput>, McpError> {
        let mut input = input;
        input.connection_id = self.validate_connection_id(&input.connection_id)?;
        let handler = WriteToolHandler::new(
            self.connection_manager.clone(),
            self.transaction_registry.clone(),
        );
        handler
            .execute(input)
            .await
            .map(Json)
            .map_err(|e: DbError| McpError::internal_error(e.to_string(), None))
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
            .map_err(|e: DbError| McpError::internal_error(e.to_string(), None))
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
        handler
            .commit(input)
            .await
            .map(Json)
            .map_err(|e: DbError| McpError::internal_error(e.to_string(), None))
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
        handler
            .rollback(input)
            .await
            .map(Json)
            .map_err(|e: DbError| McpError::internal_error(e.to_string(), None))
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
            .map_err(|e: DbError| McpError::internal_error(e.to_string(), None))
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
        handler
            .explain(input)
            .await
            .map(Json)
            .map_err(|e: DbError| McpError::internal_error(e.to_string(), None))
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
                ## Connection Types\n\
                - **Read-only**: Can use `query`, `list_tables`, `describe_table`, `list_databases`\n\
                - **Read-write**: Can also use `execute`, `begin_transaction`\n\
                - **Server-level**: Connections without a database in the URL (e.g., mysql://host:3306)\n\
                  require `schema` parameter for `list_tables` and `describe_table`\n\
                \n\
                ## Database-Specific Notes\n\
                - MySQL: `list_databases` works on any connection. Cross-database queries supported.\n\
                - PostgreSQL: `list_databases` works, but queries cannot span databases.\n\
                - SQLite: `list_databases` not supported (file-based; each file is a database).\n\
                \n\
                ## Error: Missing connection_id\n\
                If you see \"connection_id is required\", call `list_connections` first.\n\
                \n\
                ## Server-Level Connections\n\
                For connections without a default database (server_level: true in list_connections):\n\
                - `query`: Use `db.table` syntax or `SHOW TABLES FROM db` instead of `SHOW TABLES`\n\
                - `execute`: Use `db.table` syntax or run `USE database_name` first\n\
                - `list_tables`/`describe_table`: Provide the `schema` parameter"
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
