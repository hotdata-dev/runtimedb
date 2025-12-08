use super::catalog_provider::HotDataCatalogProvider;
use crate::catalog::{CatalogManager, ConnectionInfo, DuckdbCatalogManager, TableInfo};
use crate::datafetch::DataFetcher;
use crate::storage::{FilesystemStorage, StorageManager};
use anyhow::Result;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::CatalogProvider;
use datafusion::prelude::*;
use log::{info, warn};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct QueryResponse {
    pub results: Vec<RecordBatch>,
    pub execution_time: Duration,
}

/// Build a connection string from individual config parameters based on source type.
/// Supports both explicit `connection_string` field or building from components.
fn build_connection_string(source_type: &str, config: &serde_json::Value) -> Result<String> {
    // If connection_string is provided directly, use it
    if let Some(conn_str) = config.get("connection_string").and_then(|v| v.as_str()) {
        return Ok(conn_str.to_string());
    }

    // Build from individual parameters based on source type
    match source_type {
        "postgres" => {
            let user = config
                .get("user")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("postgres config requires 'user' field"))?;
            let password = config
                .get("password")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let host = config
                .get("host")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("postgres config requires 'host' field"))?;
            let port = config
                .get("port")
                .and_then(|v| v.as_u64())
                .unwrap_or(5432);
            let database = config
                .get("database")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("postgres config requires 'database' field"))?;

            Ok(format!(
                "postgresql://{}:{}@{}:{}/{}",
                user, password, host, port, database
            ))
        }
        "duckdb" => {
            let path = config
                .get("path")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("duckdb config requires 'path' field"))?;
            Ok(path.to_string())
        }
        "motherduck" => {
            let token = config
                .get("token")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("motherduck config requires 'token' field"))?;
            let database = config
                .get("database")
                .and_then(|v| v.as_str())
                .unwrap_or("my_db");
            Ok(format!("md:{}?motherduck_token={}", database, token))
        }
        "snowflake" => {
            let account = config
                .get("account")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("snowflake config requires 'account' field"))?;
            let user = config
                .get("user")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("snowflake config requires 'user' field"))?;
            let password = config
                .get("password")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("snowflake config requires 'password' field"))?;
            let database = config
                .get("database")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("snowflake config requires 'database' field"))?;
            let warehouse = config
                .get("warehouse")
                .and_then(|v| v.as_str())
                .unwrap_or("COMPUTE_WH");

            Ok(format!(
                "{}:{}@{}/{}/{}",
                user, password, account, database, warehouse
            ))
        }
        _ => anyhow::bail!("Unknown source type: {}", source_type),
    }
}

/// The main query engine that manages connections, catalogs, and query execution.
pub struct HotDataEngine {
    catalog: Arc<dyn CatalogManager>,
    df_ctx: SessionContext,
    storage: Arc<dyn StorageManager>,
}

impl HotDataEngine {
    /// Create a new engine instance and register all existing connections.
    pub fn new(catalog_path: &str) -> Result<Self> {
        Self::new_with_paths(catalog_path, "cache", "state", false)
    }

    /// Create a new engine instance with custom paths for catalog, cache, and state.
    pub fn new_with_paths(
        catalog_path: &str,
        cache_base: &str,
        state_base: &str,
        readonly: bool,
    ) -> Result<Self> {
        // Create filesystem storage manager
        let storage: Arc<dyn StorageManager> =
            Arc::new(FilesystemStorage::new(cache_base, state_base));
        Self::new_with_storage(catalog_path, storage, readonly)
    }

    /// Create a new engine instance with custom storage manager.
    /// This allows using S3 or other storage backends.
    pub fn new_with_storage(
        catalog_path: &str,
        storage: Arc<dyn StorageManager>,
        readonly: bool,
    ) -> Result<Self> {
        let catalog = DuckdbCatalogManager::new_readonly(catalog_path, readonly)?;

        let df_ctx = SessionContext::new();

        // Register storage with DataFusion
        storage.register_with_datafusion(&df_ctx)?;

        let mut engine = Self {
            catalog: Arc::new(catalog),
            df_ctx,
            storage,
        };

        // Register all existing connections as DataFusion catalogs
        engine.register_existing_connections()?;

        Ok(engine)
    }

    /// Delete cache and state directories for a connection.
    async fn delete_connection_files(&self, connection_id: i32) -> Result<()> {
        let cache_prefix = self.storage.cache_prefix(connection_id);
        let state_prefix = self.storage.state_prefix(connection_id);

        let (cache_res, state_res) = tokio::join!(
            self.storage.delete_prefix(&cache_prefix),
            self.storage.delete_prefix(&state_prefix),
        );

        if let Err(e) = cache_res {
            warn!("Failed to delete cache prefix {}: {}", cache_prefix, e);
        }
        if let Err(e) = state_res {
            warn!("Failed to delete state prefix {}: {}", state_prefix, e);
        }

        Ok(())
    }

    /// Delete parquet and state files for a specific table.
    async fn delete_table_files(&self, table_info: &TableInfo) -> Result<()> {
        // Delete parquet file if it exists
        if let Some(parquet_path) = &table_info.parquet_path {
            if let Err(e) = self.storage.delete(parquet_path).await {
                warn!("Failed to delete parquet file {}: {}", parquet_path, e);
            }
        }

        // Delete state path if it exists
        if let Some(state_path) = &table_info.state_path {
            if let Err(e) = self.storage.delete(state_path).await {
                warn!("Failed to delete state file {}: {}", state_path, e);
            }
        }

        Ok(())
    }

    /// Get a reference to the storage manager
    pub fn storage(&self) -> &Arc<dyn StorageManager> {
        &self.storage
    }

    /// Register all connections from the DuckDB catalog as DataFusion catalogs.
    fn register_existing_connections(&mut self) -> Result<()> {
        let connections = self.catalog.list_connections()?;

        for conn in connections {
            let config: serde_json::Value = serde_json::from_str(&conn.config_json)?;

            let catalog_provider = Arc::new(HotDataCatalogProvider::new(
                conn.id,
                conn.name.clone(),
                config,
                self.catalog.clone(),
                self.storage.clone(),
            )) as Arc<dyn CatalogProvider>;

            self.df_ctx.register_catalog(&conn.name, catalog_provider);
        }

        Ok(())
    }

    /// Connect to a new external data source and register it as a catalog.
    pub async fn connect(&self, source_type: &str, name: &str, config: serde_json::Value) -> Result<()> {
        // Validate source type
        if !["postgres", "snowflake", "motherduck", "duckdb"].contains(&source_type) {
            anyhow::bail!(
                "Unsupported source type '{}'. Supported types: postgres, snowflake, motherduck, duckdb",
                source_type
            );
        }

        // Build connection config
        let connection_string = build_connection_string(source_type, &config)?;

        let fetch_config = crate::datafetch::ConnectionConfig {
            source_type: source_type.to_string(),
            connection_string,
        };

        // Discover tables
        info!("Discovering tables for {} source...", source_type);
        let fetcher = crate::datafetch::NativeFetcher::new();
        let tables = fetcher.discover_tables(&fetch_config).await
            .map_err(|e| anyhow::anyhow!("Discovery failed: {}", e))?;

        info!("Discovered {} tables", tables.len());

        // Add connection to catalog
        let config_with_type = {
            let mut obj = match config {
                serde_json::Value::Object(m) => m,
                _ => anyhow::bail!("Configuration must be a JSON object"),
            };
            obj.insert(
                "type".to_string(),
                serde_json::Value::String(source_type.to_string()),
            );
            serde_json::Value::Object(obj)
        };

        let config_json = serde_json::to_string(&config_with_type)?;
        let conn_id = self.catalog.add_connection(name, source_type, &config_json)?;

        // Add discovered tables to catalog with schema in one call
        for table in &tables {
            let schema = table.to_arrow_schema();
            let schema_json = serde_json::to_string(schema.as_ref())
                .map_err(|e| anyhow::anyhow!("Failed to serialize schema: {}", e))?;
            self.catalog.add_table(conn_id, &table.schema_name, &table.table_name, &schema_json)?;
        }

        // Register with DataFusion
        let catalog_provider = Arc::new(HotDataCatalogProvider::new(
            conn_id,
            name.to_string(),
            config_with_type,
            self.catalog.clone(),
            self.storage.clone(),
        )) as Arc<dyn CatalogProvider>;

        self.df_ctx.register_catalog(name, catalog_provider);

        info!("Connection '{}' registered with {} tables", name, tables.len());

        Ok(())
    }

    /// Get a cloned catalog manager that shares the same underlying connection.
    pub fn catalog(&self) -> Arc<dyn CatalogManager> {
        self.catalog.clone()
    }

    /// List all configured connections.
    pub fn list_connections(&self) -> Result<Vec<ConnectionInfo>> {
        self.catalog.list_connections()
    }

    /// List all tables, optionally filtered by connection name.
    pub fn list_tables(&self, connection_name: Option<&str>) -> Result<Vec<TableInfo>> {
        let connection_id = if let Some(name) = connection_name {
            match self.catalog.get_connection(name)? {
                Some(conn) => Some(conn.id),
                None => anyhow::bail!("Connection '{}' not found", name),
            }
        } else {
            None
        };

        self.catalog.list_tables(connection_id)
    }

    /// Execute a SQL query and return the results.
    pub async fn execute_query(&self, sql: &str) -> Result<QueryResponse> {
        let start = Instant::now();
        let df = self.df_ctx.sql(sql).await?;
        let results = df.collect().await?;

        Ok(QueryResponse {
            execution_time: start.elapsed(),
            results,
        })
    }

    /// Purge all cached data for a connection (clears parquet files and resets sync state).
    pub async fn purge_connection(&mut self, name: &str) -> Result<()> {
        // Get connection info (validates it exists and gives us the ID)
        let conn = self
            .catalog
            .get_connection(name)?
            .ok_or_else(|| anyhow::anyhow!("Connection '{}' not found", name))?;

        // Step 1: Clear metadata first (metadata-first ordering)
        self.catalog.clear_connection_cache_metadata(name)?;

        // Step 2: Re-register the connection with fresh state
        // This causes DataFusion to drop any open file handles to the cached files
        let config: serde_json::Value = serde_json::from_str(&conn.config_json)?;

        let catalog_provider = Arc::new(HotDataCatalogProvider::new(
            conn.id,
            conn.name.clone(),
            config,
            self.catalog.clone(),
            self.storage.clone(),
        )) as Arc<dyn CatalogProvider>;

        // register_catalog replaces existing catalog with same name
        self.df_ctx.register_catalog(name, catalog_provider);

        // Step 3: Delete the physical files (now that DataFusion has released file handles)
        self.delete_connection_files(conn.id).await?;

        Ok(())
    }

    /// Purge cached data for a single table (clears parquet file and resets sync state).
    pub async fn purge_table(
        &mut self,
        connection_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<()> {
        // Validate connection exists
        let conn = self
            .catalog
            .get_connection(connection_name)?
            .ok_or_else(|| anyhow::anyhow!("Connection '{}' not found", connection_name))?;

        // Step 1: Clear metadata (set paths to NULL in DB)
        // This returns the table info with the old paths before clearing
        let table_info =
            self.catalog
                .clear_table_cache_metadata(conn.id, schema_name, table_name)?;

        // Step 2: Re-register the connection with fresh state
        // This causes DataFusion to drop any open file handles to the cached files
        let config: serde_json::Value = serde_json::from_str(&conn.config_json)?;

        let catalog_provider = Arc::new(HotDataCatalogProvider::new(
            conn.id,
            conn.name.clone(),
            config,
            self.catalog.clone(),
            self.storage.clone(),
        )) as Arc<dyn CatalogProvider>;

        // register_catalog replaces existing catalog with same name
        self.df_ctx
            .register_catalog(connection_name, catalog_provider);

        // Step 3: Delete the physical files (now that DataFusion has released file handles)
        self.delete_table_files(&table_info).await?;

        Ok(())
    }

    /// Remove a connection entirely (removes from catalog and deletes all data).
    pub async fn remove_connection(&mut self, name: &str) -> Result<()> {
        // Get connection info (validates it exists and gives us the ID)
        let conn = self
            .catalog
            .get_connection(name)?
            .ok_or_else(|| anyhow::anyhow!("Connection '{}' not found", name))?;

        // Step 1: Delete metadata first
        self.catalog.delete_connection(name)?;

        // Step 2: Delete the physical files
        self.delete_connection_files(conn.id).await?;

        // Note: DataFusion doesn't support deregistering catalogs in v50.3.0
        // The catalog will remain registered but will return no schemas/tables
        // since the DuckDB catalog has been cleaned up

        Ok(())
    }

    /// Set the default catalog for queries (allows queries without fully-qualified table names).
    pub async fn set_default_catalog(&self, name: &str) -> Result<()> {
        // Validate connection exists
        if self.catalog.get_connection(name)?.is_none() {
            anyhow::bail!("Connection '{}' not found", name);
        }

        let set_sql = format!("SET datafusion.catalog.default_catalog = '{}'", name);
        self.df_ctx.sql(&set_sql).await?;

        Ok(())
    }

    /// Sync all tables for a connection using DLT.
    /// This forces a sync of all tables, even if they're already cached.
    /// Tables are synced in parallel with a concurrency limit of 10.
    pub async fn sync_connection(&self, _name: &str) -> Result<()> {
        todo!("Implement connections sync")
    }

    /// Shutdown the engine and close all connections.
    /// This should be called before the application exits to ensure proper cleanup.
    pub fn shutdown(&self) -> Result<()> {
        self.catalog.close()
    }
}

impl Drop for HotDataEngine {
    fn drop(&mut self) {
        // Ensure catalog connection is closed when engine is dropped
        let _ = self.catalog.close();
    }
}

/// Builder for HotDataEngine
///
/// # Example
///
/// ```no_run
/// use rivetdb::datafusion::HotDataEngine;
/// use std::path::PathBuf;
///
/// let builder = HotDataEngine::builder()
///     .metadata_dir(PathBuf::from("/tmp/rivet"));
///
/// // let engine = builder.build().unwrap();
/// ```
pub struct HotDataEngineBuilder {
    metadata_dir: Option<PathBuf>,
    catalog: Option<Arc<dyn CatalogManager>>,
    storage: Option<Arc<dyn StorageManager>>,
}

impl Default for HotDataEngineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl HotDataEngineBuilder {
    pub fn new() -> Self {
        Self {
            metadata_dir: None,
            catalog: None,
            storage: None,
        }
    }

    pub fn metadata_dir(mut self, dir: PathBuf) -> Self {
        self.metadata_dir = Some(dir);
        self
    }

    pub fn catalog(mut self, catalog: Arc<dyn CatalogManager>) -> Self {
        self.catalog = Some(catalog);
        self
    }

    pub fn storage(mut self, storage: Arc<dyn StorageManager>) -> Self {
        self.storage = Some(storage);
        self
    }

    pub fn build(self) -> Result<HotDataEngine> {
        let catalog = self
            .catalog
            .ok_or_else(|| anyhow::anyhow!("Catalog manager not set"))?;
        let storage = self
            .storage
            .ok_or_else(|| anyhow::anyhow!("Storage manager not set"))?;

        // Create DataFusion session context
        let df_ctx = SessionContext::new();

        // Register storage with DataFusion
        storage.register_with_datafusion(&df_ctx)?;

        let mut engine = HotDataEngine {
            catalog,
            df_ctx,
            storage,
        };

        // Register all existing connections as DataFusion catalogs
        engine.register_existing_connections()?;

        Ok(engine)
    }
}

impl HotDataEngine {
    pub fn builder() -> HotDataEngineBuilder {
        HotDataEngineBuilder::new()
    }

    /// Create a new engine from application configuration.
    /// This is a convenience method that sets up catalog and storage based on the config.
    pub fn from_config(config: &crate::config::AppConfig) -> Result<Self> {
        // Determine metadata directory
        let metadata_dir = if let Some(cache_dir) = &config.paths.cache_dir {
            PathBuf::from(cache_dir)
                .parent()
                .ok_or_else(|| anyhow::anyhow!("Invalid cache directory path"))?
                .to_path_buf()
        } else if let Some(state_dir) = &config.paths.state_dir {
            PathBuf::from(state_dir)
                .parent()
                .ok_or_else(|| anyhow::anyhow!("Invalid state directory path"))?
                .to_path_buf()
        } else {
            // Default to ~/.hotdata/rivetdb
            let home = std::env::var("HOME")
                .or_else(|_| std::env::var("USERPROFILE"))
                .unwrap_or_else(|_| ".".to_string());
            PathBuf::from(home).join(".hotdata").join("rivetdb")
        };

        // Create metadata directory if it doesn't exist
        std::fs::create_dir_all(&metadata_dir)?;

        // Create catalog manager based on config
        let catalog: Arc<dyn CatalogManager> = match config.catalog.catalog_type.as_str() {
            "duckdb" => {
                let catalog_path = metadata_dir.join("catalog.db");
                Arc::new(DuckdbCatalogManager::new_readonly(
                    catalog_path
                        .to_str()
                        .ok_or_else(|| anyhow::anyhow!("Invalid catalog path"))?,
                    false,
                )?)
            }
            "postgres" => {
                let host = config
                    .catalog
                    .host
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("Postgres catalog requires host"))?;
                let port = config.catalog.port.unwrap_or(5432);
                let database = config
                    .catalog
                    .database
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("Postgres catalog requires database"))?;
                let user = config
                    .catalog
                    .user
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("Postgres catalog requires user"))?;
                let password = config
                    .catalog
                    .password
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("Postgres catalog requires password"))?;

                let connection_string = format!(
                    "postgresql://{}:{}@{}:{}/{}",
                    user, password, host, port, database
                );

                Arc::new(crate::catalog::PostgresCatalogManager::new(
                    &connection_string,
                )?)
            }
            _ => anyhow::bail!("Unsupported catalog type: {}", config.catalog.catalog_type),
        };

        // Create storage manager based on config
        let storage: Arc<dyn StorageManager> = match config.storage.storage_type.as_str() {
            "filesystem" => {
                let cache_dir = config.paths.cache_dir.as_deref().unwrap_or("cache");
                let state_dir = config.paths.state_dir.as_deref().unwrap_or("state");
                Arc::new(FilesystemStorage::new(cache_dir, state_dir))
            }
            "s3" => {
                let bucket = config
                    .storage
                    .bucket
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("S3 storage requires bucket"))?;

                // Check if we have custom endpoint (for MinIO/localstack)
                if let Some(endpoint) = &config.storage.endpoint {
                    // For custom endpoint, we need credentials from environment
                    let access_key = std::env::var("AWS_ACCESS_KEY_ID")
                        .or_else(|_| std::env::var("RIVETDB_STORAGE_ACCESS_KEY_ID"))
                        .unwrap_or_else(|_| "minioadmin".to_string());
                    let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY")
                        .or_else(|_| std::env::var("RIVETDB_STORAGE_SECRET_ACCESS_KEY"))
                        .unwrap_or_else(|_| "minioadmin".to_string());

                    // Allow HTTP for local MinIO
                    let allow_http = endpoint.starts_with("http://");

                    Arc::new(crate::storage::S3Storage::new_with_config(
                        bucket,
                        endpoint,
                        &access_key,
                        &secret_key,
                        allow_http,
                    )?)
                } else {
                    // Use AWS credentials from environment
                    Arc::new(crate::storage::S3Storage::new(bucket)?)
                }
            }
            _ => anyhow::bail!("Unsupported storage type: {}", config.storage.storage_type),
        };

        // Use builder to construct engine
        HotDataEngine::builder()
            .metadata_dir(metadata_dir)
            .catalog(catalog)
            .storage(storage)
            .build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_builder_pattern() {
        let temp_dir = TempDir::new().unwrap();
        let metadata_dir = temp_dir.path().to_path_buf();
        let catalog_path = metadata_dir.join("catalog.db");

        // Create catalog and storage
        let catalog = Arc::new(
            DuckdbCatalogManager::new_readonly(catalog_path.to_str().unwrap(), false).unwrap(),
        );

        let storage = Arc::new(FilesystemStorage::new(
            temp_dir.path().join("cache").to_str().unwrap(),
            temp_dir.path().join("state").to_str().unwrap(),
        ));

        // Build engine using builder pattern
        let engine = HotDataEngine::builder()
            .metadata_dir(metadata_dir.clone())
            .catalog(catalog)
            .storage(storage)
            .build();

        assert!(engine.is_ok(), "Builder should successfully create engine");

        let engine = engine.unwrap();

        // Verify we can list connections (should be empty)
        let connections = engine.list_connections();
        assert!(connections.is_ok(), "Should be able to list connections");
        assert_eq!(
            connections.unwrap().len(),
            0,
            "Should have no connections initially"
        );
    }

    #[test]
    fn test_builder_pattern_missing_fields() {
        // Test that builder fails when required fields are missing
        let temp_dir = TempDir::new().unwrap();
        let result = HotDataEngine::builder()
            .metadata_dir(temp_dir.path().to_path_buf())
            .build();
        assert!(result.is_err(), "Builder should fail without catalog");
        if let Err(e) = result {
            assert!(
                e.to_string().contains("Catalog"),
                "Error should mention catalog: {}",
                e
            );
        }
    }

    #[test]
    fn test_from_config_duckdb_filesystem() {
        use crate::config::{AppConfig, CatalogConfig, PathsConfig, ServerConfig, StorageConfig};

        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().join("cache");
        let state_dir = temp_dir.path().join("state");

        let config = AppConfig {
            server: ServerConfig {
                host: "127.0.0.1".to_string(),
                port: 3000,
            },
            catalog: CatalogConfig {
                catalog_type: "duckdb".to_string(),
                host: None,
                port: None,
                database: None,
                user: None,
                password: None,
            },
            storage: StorageConfig {
                storage_type: "filesystem".to_string(),
                bucket: None,
                region: None,
                endpoint: None,
            },
            paths: PathsConfig {
                cache_dir: Some(cache_dir.to_str().unwrap().to_string()),
                state_dir: Some(state_dir.to_str().unwrap().to_string()),
            },
        };

        let engine = HotDataEngine::from_config(&config);
        assert!(
            engine.is_ok(),
            "from_config should create engine successfully"
        );

        let engine = engine.unwrap();

        // Verify we can list connections
        let connections = engine.list_connections();
        assert!(connections.is_ok(), "Should be able to list connections");
    }
}
