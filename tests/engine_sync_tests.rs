use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use rand::RngCore;
use runtimedb::RuntimeEngine;
use tempfile::tempdir;

/// Generate a test secret key (base64-encoded 32 bytes)
fn generate_test_secret_key() -> String {
    let mut key = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut key);
    STANDARD.encode(key)
}

/// Test that sync_connection handles non-existent connections correctly
#[tokio::test]
#[ignore]
async fn test_sync_connection_not_found() -> Result<()> {
    let dir = tempdir()?;

    let engine = RuntimeEngine::builder()
        .base_dir(dir.path())
        .secret_key(generate_test_secret_key())
        .build()
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

    let engine = RuntimeEngine::builder()
        .base_dir(dir.path())
        .secret_key(generate_test_secret_key())
        .build()
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
