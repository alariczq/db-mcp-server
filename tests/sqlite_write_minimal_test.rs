use db_mcp_server::config::PoolOptions;
use db_mcp_server::db::ConnectionManager;
use db_mcp_server::models::ConnectionConfig;
use std::sync::Arc;
use tempfile::NamedTempFile;

#[tokio::test]
async fn test_sqlite_writable_flag_basic() {
    let temp_file = NamedTempFile::new().unwrap();
    let db_path = temp_file.path().to_str().unwrap().to_string();
    let manager = Arc::new(ConnectionManager::new());

    let conn_url = format!("sqlite:{}?mode=rwc", db_path);
    let config =
        ConnectionConfig::new("test", &conn_url, true, false, None, PoolOptions::default())
            .unwrap();

    manager.connect(config).await.unwrap();

    // Get the pool directly
    let pool = manager.get_pool("test").await.unwrap();

    if let db_mcp_server::db::pool::DbPool::SQLite(pool) = pool {
        // Try to create a table
        sqlx::query("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
            .execute(&pool)
            .await
            .expect("CREATE TABLE should work");

        // Try to insert - this is where the bug shows up
        let result = sqlx::query("INSERT INTO test (id, name) VALUES (1, 'test')")
            .execute(&pool)
            .await;

        assert!(
            result.is_ok(),
            "INSERT should work with writable=true, but got error: {:?}",
            result.err()
        );
    } else {
        panic!("Expected SQLite pool");
    }
}
