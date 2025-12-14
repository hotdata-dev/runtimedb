use crate::catalog::{CatalogManager, ConnectionInfo, SqliteCatalogManager, TableInfo};
use crate::datafetch::DataFetcher;
use crate::datafusion::{block_on, RivetCatalogProvider};
use crate::source::Source;
use crate::storage::{FilesystemStorage, StorageManager};
use anyhow::Result;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::CatalogProvider;
use datafusion::prelude::*;
use log::{info, warn};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::error;

pub struct QueryResponse {
    pub results: Vec<RecordBatch>,
    pub execution_time: Duration,
}

/// The main query engine that manages connections, catalogs, and query execution.
pub struct RivetEngine {
    catalog: Arc<dyn CatalogManager>,
    df_ctx: SessionContext,
    storage: Arc<dyn StorageManager>,
}

impl RivetEngine {
    /// Create a new engine instance and register all existing connections.
    pub async fn new(catalog_path: &str) -> Result<Self> {
        Self::new_with_paths(catalog_path, "cache", false).await
    }

    /// Create a new engine instance with custom paths for catalog, cache, and state.
    pub async fn new_with_paths(
        catalog_path: &str,
        cache_base: &str,
        readonly: bool,
    ) -> Result<Self> {
        // Create filesystem storage manager
        let storage: Arc<dyn StorageManager> = Arc::new(FilesystemStorage::new(cache_base));
        Self::new_with_storage(catalog_path, storage, readonly).await
    }

    /// Create a new engine instance with custom storage manager.
    /// This allows using S3 or other storage backends.
    pub async fn new_with_storage(
        catalog_path: &str,
        storage: Arc<dyn StorageManager>,
        _readonly: bool,
    ) -> Result<Self> {
        let catalog = SqliteCatalogManager::new(catalog_path).await?;
        catalog.run_migrations().await?;

        let df_ctx = SessionContext::new();

        // Register storage with DataFusion
        storage.register_with_datafusion(&df_ctx)?;

        let mut engine = Self {
            catalog: Arc::new(catalog),
            df_ctx,
            storage,
        };

        // Register all existing connections as DataFusion catalogs
        engine.register_existing_connections().await?;

        Ok(engine)
    }

    /// Delete cache and state directories for a connection.
    async fn delete_connection_files(&self, connection_id: i32) -> Result<()> {
        let cache_prefix = self.storage.cache_prefix(connection_id);

        if let Err(e) = self.storage.delete_prefix(&cache_prefix).await {
            warn!("Failed to delete cache prefix {}: {}", cache_prefix, e);
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

        Ok(())
    }

    /// Get a reference to the storage manager
    pub fn storage(&self) -> &Arc<dyn StorageManager> {
        &self.storage
    }

    /// Register all connections from the catalog store as DataFusion catalogs.
    async fn register_existing_connections(&mut self) -> Result<()> {
        let connections = self.catalog.list_connections().await?;

        for conn in connections {
            let source: Source = serde_json::from_str(&conn.config_json)?;

            let catalog_provider = Arc::new(RivetCatalogProvider::new(
                conn.id,
                conn.name.clone(),
                Arc::new(source),
                self.catalog.clone(),
                self.storage.clone(),
            )) as Arc<dyn CatalogProvider>;

            self.df_ctx.register_catalog(&conn.name, catalog_provider);
        }

        Ok(())
    }

    /// Connect to a new external data source and register it as a catalog.
    pub async fn connect(&self, name: &str, source: Source) -> Result<()> {
        let source_type = source.source_type();

        // Discover tables
        info!("Discovering tables for {} source...", source_type);
        let fetcher = crate::datafetch::NativeFetcher::new();
        let tables = fetcher
            .discover_tables(&source)
            .await
            .map_err(|e| anyhow::anyhow!("Discovery failed: {}", e))?;

        info!("Discovered {} tables", tables.len());

        // Store config as JSON (includes "type" from serde tag)
        let config_json = serde_json::to_string(&source)?;
        let conn_id = self
            .catalog
            .add_connection(name, source_type, &config_json)
            .await?;

        // Add discovered tables to catalog with schema in one call
        for table in &tables {
            let schema = table.to_arrow_schema();
            let schema_json = serde_json::to_string(schema.as_ref())
                .map_err(|e| anyhow::anyhow!("Failed to serialize schema: {}", e))?;
            self.catalog
                .add_table(conn_id, &table.schema_name, &table.table_name, &schema_json)
                .await?;
        }

        // Register with DataFusion
        let catalog_provider = Arc::new(RivetCatalogProvider::new(
            conn_id,
            name.to_string(),
            Arc::new(source),
            self.catalog.clone(),
            self.storage.clone(),
        )) as Arc<dyn CatalogProvider>;

        self.df_ctx.register_catalog(name, catalog_provider);

        info!(
            "Connection '{}' registered with {} tables",
            name,
            tables.len()
        );

        Ok(())
    }

    /// Get a cloned catalog manager that shares the same underlying connection.
    pub fn catalog(&self) -> Arc<dyn CatalogManager> {
        self.catalog.clone()
    }

    /// List all configured connections.
    pub async fn list_connections(&self) -> Result<Vec<ConnectionInfo>> {
        self.catalog.list_connections().await
    }

    /// List all tables, optionally filtered by connection name.
    pub async fn list_tables(&self, connection_name: Option<&str>) -> Result<Vec<TableInfo>> {
        let connection_id = if let Some(name) = connection_name {
            match self.catalog.get_connection(name).await? {
                Some(conn) => Some(conn.id),
                None => anyhow::bail!("Connection '{}' not found", name),
            }
        } else {
            None
        };

        self.catalog.list_tables(connection_id).await
    }

    /// Execute a SQL query and return the results.
    pub async fn execute_query(&self, sql: &str) -> Result<QueryResponse> {
        info!("Executing query: {}", sql);
        let start = Instant::now();
        let df = self.df_ctx.sql(sql).await.map_err(|e| {
            error!("Error executing query: {}", e);
            e
        })?;
        let results = df.collect().await.map_err(|e| {
            error!("Error getting query result: {}", e);
            e
        })?;
        info!("Execution completed in {:?}", start.elapsed());
        info!("Results available");

        Ok(QueryResponse {
            execution_time: start.elapsed(),
            results,
        })
    }

    /// Purge all cached data for a connection (clears parquet files and resets sync state).
    pub async fn purge_connection(&self, name: &str) -> Result<()> {
        // Get connection info (validates it exists and gives us the ID)
        let conn = self
            .catalog
            .get_connection(name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Connection '{}' not found", name))?;

        // Step 1: Clear metadata first (metadata-first ordering)
        self.catalog.clear_connection_cache_metadata(name).await?;

        // Step 2: Re-register the connection with fresh state
        // This causes DataFusion to drop any open file handles to the cached files
        let source: Source = serde_json::from_str(&conn.config_json)?;

        let catalog_provider = Arc::new(RivetCatalogProvider::new(
            conn.id,
            conn.name.clone(),
            Arc::new(source),
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
        &self,
        connection_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<()> {
        // Validate connection exists
        let conn = self
            .catalog
            .get_connection(connection_name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Connection '{}' not found", connection_name))?;

        // Step 1: Clear metadata (set paths to NULL in DB)
        // This returns the table info with the old paths before clearing
        let table_info = self
            .catalog
            .clear_table_cache_metadata(conn.id, schema_name, table_name)
            .await?;

        // Step 2: Re-register the connection with fresh state
        // This causes DataFusion to drop any open file handles to the cached files
        let source: Source = serde_json::from_str(&conn.config_json)?;

        let catalog_provider = Arc::new(RivetCatalogProvider::new(
            conn.id,
            conn.name.clone(),
            Arc::new(source),
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
    pub async fn remove_connection(&self, name: &str) -> Result<()> {
        // Get connection info (validates it exists and gives us the ID)
        let conn = self
            .catalog
            .get_connection(name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Connection '{}' not found", name))?;

        // Step 1: Delete metadata first
        self.catalog.delete_connection(name).await?;

        // Step 2: Delete the physical files
        self.delete_connection_files(conn.id).await?;

        // Note: DataFusion doesn't support deregistering catalogs in v50.3.0
        // The catalog will remain registered but will return no schemas/tables
        // since the metadata catalog has been cleaned up

        Ok(())
    }

    /// Set the default catalog for queries (allows queries without fully-qualified table names).
    pub async fn set_default_catalog(&self, name: &str) -> Result<()> {
        // Validate connection exists
        if self.catalog.get_connection(name).await?.is_none() {
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
    pub async fn shutdown(&self) -> Result<()> {
        self.catalog.close().await
    }
}

impl Drop for RivetEngine {
    fn drop(&mut self) {
        // Ensure catalog connection is closed when engine is dropped
        let _ = block_on(self.catalog.close());
    }
}

/// Builder for RivetEngine
///
/// # Example
///
/// ```no_run
/// use rivetdb::RivetEngine;
/// use std::path::PathBuf;
///
/// let builder = RivetEngine::builder()
///     .metadata_dir(PathBuf::from("/tmp/rivet"));
///
/// // let engine = builder.build().unwrap();
/// ```
pub struct RivetEngineBuilder {
    metadata_dir: Option<PathBuf>,
    catalog: Option<Arc<dyn CatalogManager>>,
    storage: Option<Arc<dyn StorageManager>>,
}

impl Default for RivetEngineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl RivetEngineBuilder {
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

    pub async fn build(self) -> Result<RivetEngine> {
        let catalog = self
            .catalog
            .ok_or_else(|| anyhow::anyhow!("Catalog manager not set"))?;
        let storage = self
            .storage
            .ok_or_else(|| anyhow::anyhow!("Storage manager not set"))?;

        catalog.run_migrations().await?;

        // Create DataFusion session context
        let df_ctx = SessionContext::new();

        // Register storage with DataFusion
        storage.register_with_datafusion(&df_ctx)?;

        let mut engine = RivetEngine {
            catalog,
            df_ctx,
            storage,
        };

        // Register all existing connections as DataFusion catalogs
        engine.register_existing_connections().await?;

        Ok(engine)
    }
}

impl RivetEngine {
    pub fn builder() -> RivetEngineBuilder {
        RivetEngineBuilder::new()
    }

    /// Create a new engine from application configuration.
    /// This is a convenience method that sets up catalog and storage based on the config.
    pub async fn from_config(config: &crate::config::AppConfig) -> Result<Self> {
        // Determine metadata directory root (defaults to ~/.hotdata/rivetdb)
        let metadata_dir = if let Some(cache_dir) = &config.paths.cache_dir {
            PathBuf::from(cache_dir)
                .parent()
                .ok_or_else(|| anyhow::anyhow!("Invalid cache directory path"))?
                .to_path_buf()
        } else {
            // Default to ~/.hotdata/rivetdb
            let home = std::env::var("HOME")
                .or_else(|_| std::env::var("USERPROFILE"))
                .unwrap_or_else(|_| ".".to_string());
            PathBuf::from(home).join(".hotdata").join("rivetdb")
        };

        // Default cache/state directories live under metadata dir unless explicitly set
        let cache_dir_path = config
            .paths
            .cache_dir
            .as_ref()
            .map(PathBuf::from)
            .unwrap_or_else(|| metadata_dir.join("cache"));

        // Ensure directories exist before using them
        std::fs::create_dir_all(&metadata_dir)?;
        std::fs::create_dir_all(&cache_dir_path)?;

        // Create catalog manager based on config
        let catalog: Arc<dyn CatalogManager> = match config.catalog.catalog_type.as_str() {
            "sqlite" => {
                let catalog_path = metadata_dir.join("catalog.db");
                Arc::new(
                    SqliteCatalogManager::new(
                        catalog_path
                            .to_str()
                            .ok_or_else(|| anyhow::anyhow!("Invalid catalog path"))?,
                    )
                    .await?,
                )
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

                Arc::new(crate::catalog::PostgresCatalogManager::new(&connection_string).await?)
            }
            _ => anyhow::bail!("Unsupported catalog type: {}", config.catalog.catalog_type),
        };

        // Create storage manager based on config
        let storage: Arc<dyn StorageManager> = match config.storage.storage_type.as_str() {
            "filesystem" => {
                let cache_dir_str = cache_dir_path
                    .to_str()
                    .ok_or_else(|| anyhow::anyhow!("Invalid cache directory path"))?;
                Arc::new(FilesystemStorage::new(cache_dir_str))
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
        RivetEngine::builder()
            .metadata_dir(metadata_dir)
            .catalog(catalog)
            .storage(storage)
            .build()
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_builder_pattern() {
        let temp_dir = TempDir::new().unwrap();
        let metadata_dir = temp_dir.path().to_path_buf();
        let catalog_path = metadata_dir.join("catalog.db");

        // Create catalog and storage
        let catalog = Arc::new(
            SqliteCatalogManager::new(catalog_path.to_str().unwrap())
                .await
                .unwrap(),
        );

        let storage = Arc::new(FilesystemStorage::new(
            temp_dir.path().join("cache").to_str().unwrap(),
        ));

        // Build engine using builder pattern
        let engine = RivetEngine::builder()
            .metadata_dir(metadata_dir.clone())
            .catalog(catalog)
            .storage(storage)
            .build()
            .await;

        assert!(engine.is_ok(), "Builder should successfully create engine");

        let engine = engine.unwrap();

        // Verify we can list connections (should be empty)
        let connections = engine.list_connections().await;
        assert!(connections.is_ok(), "Should be able to list connections");
        assert_eq!(
            connections.unwrap().len(),
            0,
            "Should have no connections initially"
        );
    }

    #[tokio::test]
    async fn test_builder_pattern_missing_fields() {
        // Test that builder fails when required fields are missing
        let temp_dir = TempDir::new().unwrap();
        let result = RivetEngine::builder()
            .metadata_dir(temp_dir.path().to_path_buf())
            .build()
            .await;
        assert!(result.is_err(), "Builder should fail without catalog");
        if let Err(e) = result {
            assert!(
                e.to_string().contains("Catalog"),
                "Error should mention catalog: {}",
                e
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_from_config_sqlite_filesystem() {
        use crate::config::{AppConfig, CatalogConfig, PathsConfig, ServerConfig, StorageConfig};

        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().join("cache");

        let config = AppConfig {
            server: ServerConfig {
                host: "127.0.0.1".to_string(),
                port: 3000,
            },
            catalog: CatalogConfig {
                catalog_type: "sqlite".to_string(),
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
            },
        };

        let engine = RivetEngine::from_config(&config).await;
        assert!(
            engine.is_ok(),
            "from_config should create engine successfully"
        );

        let engine = engine.unwrap();

        // Verify we can list connections
        let connections = engine.list_connections().await;
        assert!(connections.is_ok(), "Should be able to list connections");
    }
}
