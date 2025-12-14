use anyhow::Result;
use rivetdb::datafusion::HotDataEngine;
use tempfile::tempdir;

/// Test that sync_connection handles non-existent connections correctly
#[tokio::test]
#[ignore]
async fn test_sync_connection_not_found() -> Result<()> {
    let dir = tempdir()?;
    let catalog_path = dir.path().join("catalog.db");
    let cache_path = dir.path().join("cache");
    let state_path = dir.path().join("state");

    let engine = HotDataEngine::new_with_paths(
        catalog_path.to_str().unwrap(),
        cache_path.to_str().unwrap(),
        state_path.to_str().unwrap(),
        false,
    )
    .await?;

    // Try to sync a connection that doesn't exist
    let result = engine.sync_connection("nonexistent").await;

    // Should return an error
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not found"));

    Ok(())
}

/// Test that sync_connection handles empty connections (no tables) correctly
#[tokio::test]
#[ignore]
async fn test_sync_connection_no_tables() -> Result<()> {
    let dir = tempdir()?;
    let catalog_path = dir.path().join("catalog.db");
    let cache_path = dir.path().join("cache");
    let state_path = dir.path().join("state");

    let engine = HotDataEngine::new_with_paths(
        catalog_path.to_str().unwrap(),
        cache_path.to_str().unwrap(),
        state_path.to_str().unwrap(),
        false,
    )
    .await?;

    // Add a connection with no tables
    let config = serde_json::json!({
        "type": "postgres",
        "host": "localhost",
        "port": 5432,
        "database": "test",
        "user": "test",
        "password": "test"
    });

    // We can't use engine.connect() because it tries to discover tables from a real DB
    // Instead, manually add a connection with no tables through the catalog
    let catalog = engine.catalog();
    let config_json = serde_json::to_string(&config)?;
    catalog
        .add_connection("empty_conn", "postgres", &config_json)
        .await?;

    // Try to sync - should succeed but do nothing
    let result = engine.sync_connection("empty_conn").await;

    // Should succeed (returns Ok) since there are simply no tables to sync
    assert!(result.is_ok());

    Ok(())
}
