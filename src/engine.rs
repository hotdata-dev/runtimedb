use crate::catalog::{CatalogManager, ConnectionInfo, SqliteCatalogManager, TableInfo};
use crate::datafetch::{DataFetcher, FetchOrchestrator, NativeFetcher};
use crate::datafusion::{
    block_on, InformationSchemaProvider, RuntimeCatalogProvider, RuntimeDbCatalogProvider,
};
use crate::secrets::{EncryptedCatalogBackend, SecretManager, ENCRYPTED_PROVIDER_TYPE};
use crate::source::Source;
use crate::storage::{FilesystemStorage, StorageManager};
use anyhow::Result;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::CatalogProvider;
use datafusion::prelude::*;
use log::{info, warn};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::error;

/// Default insecure encryption key for development use only.
/// This key is publicly known and provides NO security.
/// It is a base64-encoded 32-byte key: "INSECURE_DEFAULT_KEY_RUNTIMEDB!!"
const DEFAULT_INSECURE_KEY: &str = "SU5TRUNVUkVfREVGQVVMVF9LRVlfUlVOVElNRURCISE=";

pub struct QueryResponse {
    pub results: Vec<RecordBatch>,
    pub execution_time: Duration,
}

/// The main query engine that manages connections, catalogs, and query execution.
pub struct RuntimeEngine {
    catalog: Arc<dyn CatalogManager>,
    df_ctx: SessionContext,
    storage: Arc<dyn StorageManager>,
    orchestrator: Arc<FetchOrchestrator>,
    secret_manager: Arc<SecretManager>,
}

impl RuntimeEngine {
    // =========================================================================
    // Constructors
    // =========================================================================

    /// Create an engine with default settings at the given base directory.
    ///
    /// Uses SQLite catalog at {base_dir}/catalog.db and filesystem storage at {base_dir}/cache.
    pub async fn defaults(base_dir: impl Into<PathBuf>) -> Result<Self> {
        Self::builder().base_dir(base_dir).build().await
    }

    /// Create a builder for more control over engine configuration.
    pub fn builder() -> RuntimeEngineBuilder {
        RuntimeEngineBuilder::new()
    }

    /// Create a new engine from application configuration.
    ///
    /// For sqlite catalog + filesystem storage, the builder handles defaults.
    /// This method only creates explicit catalog/storage for non-default backends (postgres, s3).
    pub async fn from_config(config: &crate::config::AppConfig) -> Result<Self> {
        let mut builder = RuntimeEngine::builder();

        // Set base_dir if explicitly configured
        if let Some(base) = &config.paths.base_dir {
            builder = builder.base_dir(PathBuf::from(base));
        }

        // Set cache_dir if explicitly configured
        if let Some(cache) = &config.paths.cache_dir {
            builder = builder.cache_dir(PathBuf::from(cache));
        }

        // Set secret key if explicitly configured
        if let Some(key) = &config.secrets.encryption_key {
            builder = builder.secret_key(key);
        }

        // Only create explicit catalog for non-sqlite backends
        if config.catalog.catalog_type != "sqlite" {
            let catalog = Self::create_catalog_from_config(config).await?;
            builder = builder.catalog(catalog);
        }

        // Only create explicit storage for non-filesystem backends
        if config.storage.storage_type != "filesystem" {
            let storage = Self::create_storage_from_config(config).await?;
            builder = builder.storage(storage);
        }

        builder.build().await
    }

    /// Create a catalog manager from config (for non-sqlite backends).
    async fn create_catalog_from_config(
        config: &crate::config::AppConfig,
    ) -> Result<Arc<dyn CatalogManager>> {
        match config.catalog.catalog_type.as_str() {
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

                Ok(Arc::new(
                    crate::catalog::PostgresCatalogManager::new(&connection_string).await?,
                ))
            }
            _ => anyhow::bail!("Unsupported catalog type: {}", config.catalog.catalog_type),
        }
    }

    /// Create a storage manager from config (for non-filesystem backends).
    async fn create_storage_from_config(
        config: &crate::config::AppConfig,
    ) -> Result<Arc<dyn StorageManager>> {
        match config.storage.storage_type.as_str() {
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
                        .or_else(|_| std::env::var("RUNTIMEDB_STORAGE_ACCESS_KEY_ID"))
                        .unwrap_or_else(|_| "minioadmin".to_string());
                    let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY")
                        .or_else(|_| std::env::var("RUNTIMEDB_STORAGE_SECRET_ACCESS_KEY"))
                        .unwrap_or_else(|_| "minioadmin".to_string());

                    // Allow HTTP for local MinIO
                    let allow_http = endpoint.starts_with("http://");

                    Ok(Arc::new(crate::storage::S3Storage::new_with_config(
                        bucket,
                        endpoint,
                        &access_key,
                        &secret_key,
                        allow_http,
                    )?))
                } else {
                    // Use AWS credentials from environment
                    Ok(Arc::new(crate::storage::S3Storage::new(bucket)?))
                }
            }
            _ => anyhow::bail!("Unsupported storage type: {}", config.storage.storage_type),
        }
    }

    // =========================================================================
    // Private helpers
    // =========================================================================

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

    /// Get a reference to the secret manager.
    pub fn secret_manager(&self) -> &Arc<SecretManager> {
        &self.secret_manager
    }

    /// Register all connections from the catalog store as DataFusion catalogs.
    async fn register_existing_connections(&mut self) -> Result<()> {
        let connections = self.catalog.list_connections().await?;

        for conn in connections {
            let source: Source = serde_json::from_str(&conn.config_json)?;

            let catalog_provider = Arc::new(RuntimeCatalogProvider::new(
                conn.id,
                conn.name.clone(),
                Arc::new(source),
                self.catalog.clone(),
                self.orchestrator.clone(),
            )) as Arc<dyn CatalogProvider>;

            self.df_ctx.register_catalog(&conn.name, catalog_provider);
        }

        Ok(())
    }

    /// Register a connection without discovering tables.
    ///
    /// This persists the connection config to the catalog and registers it with DataFusion,
    /// but does not attempt to connect to the remote database or discover tables.
    /// Use `discover_connection()` to discover tables after registration.
    pub async fn register_connection(&self, name: &str, source: Source) -> Result<i32> {
        let source_type = source.source_type();

        // Store config as JSON (includes "type" from serde tag)
        let config_json = serde_json::to_string(&source)?;
        let conn_id = self
            .catalog
            .add_connection(name, source_type, &config_json)
            .await?;

        // Register with DataFusion (empty catalog - no tables yet)
        let catalog_provider = Arc::new(RuntimeCatalogProvider::new(
            conn_id,
            name.to_string(),
            Arc::new(source),
            self.catalog.clone(),
            self.orchestrator.clone(),
        )) as Arc<dyn CatalogProvider>;

        self.df_ctx.register_catalog(name, catalog_provider);

        info!("Connection '{}' registered (discovery pending)", name);

        Ok(conn_id)
    }

    /// Discover tables for an existing connection.
    ///
    /// Connects to the remote database, discovers available tables, and stores
    /// their metadata in the catalog. Returns the number of tables discovered.
    pub async fn discover_connection(&self, name: &str) -> Result<usize> {
        // Get connection info
        let conn = self
            .catalog
            .get_connection(name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Connection '{}' not found", name))?;

        let source: Source = serde_json::from_str(&conn.config_json)?;
        let source_type = source.source_type();

        // Discover tables
        info!("Discovering tables for {} source...", source_type);
        let fetcher = crate::datafetch::NativeFetcher::new();
        let tables = fetcher
            .discover_tables(&source, &self.secret_manager)
            .await
            .map_err(|e| anyhow::anyhow!("Discovery failed: {}", e))?;

        info!("Discovered {} tables", tables.len());

        // Add discovered tables to catalog with schema
        for table in &tables {
            let schema = table.to_arrow_schema();
            let schema_json = serde_json::to_string(schema.as_ref())
                .map_err(|e| anyhow::anyhow!("Failed to serialize schema: {}", e))?;
            self.catalog
                .add_table(conn.id, &table.schema_name, &table.table_name, &schema_json)
                .await?;
        }

        info!(
            "Connection '{}' discovery complete: {} tables",
            name,
            tables.len()
        );

        Ok(tables.len())
    }

    /// Connect to a new external data source and register it as a catalog.
    ///
    /// This is a convenience method that combines `register_connection()` and
    /// `discover_connection()`. For more control, use those methods separately.
    pub async fn connect(&self, name: &str, source: Source) -> Result<()> {
        self.register_connection(name, source).await?;
        self.discover_connection(name).await?;
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

        let catalog_provider = Arc::new(RuntimeCatalogProvider::new(
            conn.id,
            conn.name.clone(),
            Arc::new(source),
            self.catalog.clone(),
            self.orchestrator.clone(),
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

        let catalog_provider = Arc::new(RuntimeCatalogProvider::new(
            conn.id,
            conn.name.clone(),
            Arc::new(source),
            self.catalog.clone(),
            self.orchestrator.clone(),
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

impl Drop for RuntimeEngine {
    fn drop(&mut self) {
        // Ensure catalog connection is closed when engine is dropped
        let _ = block_on(self.catalog.close());
    }
}

/// Builder for RuntimeEngine
///
/// The builder is responsible for:
/// - Resolving the base directory (defaults to ~/.hotdata/runtimedb)
/// - Creating default catalog (SQLite) and storage (filesystem) when not explicitly provided
/// - Ensuring directories exist before use
///
/// # Example
///
/// ```no_run
/// use runtimedb::RuntimeEngine;
/// use std::path::PathBuf;
///
/// // Minimal: uses ~/.hotdata/runtimedb with SQLite + filesystem
/// // let engine = RuntimeEngine::builder().build().await.unwrap();
///
/// // Custom base dir with defaults
/// // let engine = RuntimeEngine::builder()
/// //     .base_dir(PathBuf::from("/tmp/runtime"))
/// //     .build().await.unwrap();
///
/// // Explicit catalog/storage (skips defaults)
/// // let engine = RuntimeEngine::builder()
/// //     .catalog(my_catalog)
/// //     .storage(my_storage)
/// //     .build().await.unwrap();
/// ```
pub struct RuntimeEngineBuilder {
    base_dir: Option<PathBuf>,
    cache_dir: Option<PathBuf>,
    catalog: Option<Arc<dyn CatalogManager>>,
    storage: Option<Arc<dyn StorageManager>>,
    secret_key: Option<String>,
}

impl Default for RuntimeEngineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl RuntimeEngineBuilder {
    pub fn new() -> Self {
        Self {
            base_dir: None,
            cache_dir: None,
            catalog: None,
            storage: None,
            secret_key: std::env::var("RUNTIMEDB_SECRET_KEY").ok(),
        }
    }

    /// Set the base directory for all RuntimeDB data.
    /// Defaults to ~/.hotdata/runtimedb if not set.
    pub fn base_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.base_dir = Some(dir.into());
        self
    }

    /// Set a custom cache directory for Parquet files.
    /// Defaults to {base_dir}/cache if not set.
    pub fn cache_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.cache_dir = Some(dir.into());
        self
    }

    /// Set a custom catalog manager.
    /// If not set, creates a SQLite catalog at {base_dir}/catalog.db
    pub fn catalog(mut self, catalog: Arc<dyn CatalogManager>) -> Self {
        self.catalog = Some(catalog);
        self
    }

    /// Set a custom storage manager.
    /// If not set, creates filesystem storage at {cache_dir}
    pub fn storage(mut self, storage: Arc<dyn StorageManager>) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Set the encryption key for the secret manager (base64-encoded 32-byte key).
    /// If not set, falls back to RUNTIMEDB_SECRET_KEY environment variable.
    /// If neither is set, uses a default insecure key (with loud warnings).
    pub fn secret_key(mut self, key: impl Into<String>) -> Self {
        self.secret_key = Some(key.into());
        self
    }

    /// Resolve the base directory, using default if not set.
    fn resolve_base_dir(&self) -> PathBuf {
        self.base_dir.clone().unwrap_or_else(|| {
            let home = std::env::var("HOME")
                .or_else(|_| std::env::var("USERPROFILE"))
                .unwrap_or_else(|_| ".".to_string());
            PathBuf::from(home).join(".hotdata").join("runtimedb")
        })
    }

    /// Resolve the cache directory, using default if not set.
    fn resolve_cache_dir(&self, base_dir: &Path) -> PathBuf {
        self.cache_dir
            .clone()
            .unwrap_or_else(|| base_dir.join("cache"))
    }

    pub async fn build(self) -> Result<RuntimeEngine> {
        // Step 1: Resolve directories
        let base_dir = self.resolve_base_dir();
        let cache_dir = self.resolve_cache_dir(&base_dir);

        // Step 2: Ensure directories exist
        std::fs::create_dir_all(&base_dir)?;
        std::fs::create_dir_all(&cache_dir)?;

        // Step 3: Create catalog if not provided
        let catalog: Arc<dyn CatalogManager> = match self.catalog {
            Some(c) => c,
            None => {
                let catalog_path = base_dir.join("catalog.db");
                Arc::new(
                    SqliteCatalogManager::new(
                        catalog_path
                            .to_str()
                            .ok_or_else(|| anyhow::anyhow!("Invalid catalog path"))?,
                    )
                    .await?,
                )
            }
        };

        // Step 4: Create storage if not provided
        let storage: Arc<dyn StorageManager> = match self.storage {
            Some(s) => s,
            None => {
                let cache_dir_str = cache_dir
                    .to_str()
                    .ok_or_else(|| anyhow::anyhow!("Invalid cache directory path"))?;
                Arc::new(FilesystemStorage::new(cache_dir_str))
            }
        };

        // Step 5: Run migrations and set up DataFusion
        catalog.run_migrations().await?;

        let df_ctx = SessionContext::new();
        storage.register_with_datafusion(&df_ctx)?;

        // Step 6: Initialize secret manager
        let (secret_key, using_default_key) = match self.secret_key {
            Some(key) => (key, false),
            None => {
                // Use insecure default key for development convenience
                (DEFAULT_INSECURE_KEY.to_string(), true)
            }
        };

        if using_default_key {
            warn!("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            warn!("!!!                    SECURITY WARNING                       !!!");
            warn!("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            warn!("!!! Using DEFAULT INSECURE encryption key for secrets.        !!!");
            warn!("!!! This key is PUBLICLY KNOWN and provides NO SECURITY.      !!!");
            warn!("!!!                                                           !!!");
            warn!("!!! DO NOT USE IN PRODUCTION!                                 !!!");
            warn!("!!!                                                           !!!");
            warn!("!!! To fix: Set RUNTIMEDB_SECRET_KEY environment variable       !!!");
            warn!("!!! Generate a key with: openssl rand -base64 32              !!!");
            warn!("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        }

        let backend = Arc::new(
            EncryptedCatalogBackend::from_base64_key(&secret_key, catalog.clone())
                .map_err(|e| anyhow::anyhow!("Invalid secret key: {}", e))?,
        );
        let secret_manager = Arc::new(SecretManager::new(
            backend,
            catalog.clone(),
            ENCRYPTED_PROVIDER_TYPE,
        ));
        info!("Secret manager initialized");

        // Step 7: Create fetch orchestrator (needs secret_manager)
        let fetcher = Arc::new(NativeFetcher::new());
        let orchestrator = Arc::new(FetchOrchestrator::new(
            fetcher,
            storage.clone(),
            catalog.clone(),
            secret_manager.clone(),
        ));

        let mut engine = RuntimeEngine {
            catalog,
            df_ctx,
            storage,
            orchestrator,
            secret_manager,
        };

        // Register all existing connections as DataFusion catalogs
        engine.register_existing_connections().await?;

        // Register the runtimedb virtual catalog for system metadata
        let runtimedb_catalog = Arc::new(RuntimeDbCatalogProvider::new());
        runtimedb_catalog.register_schema(
            "information_schema",
            Arc::new(InformationSchemaProvider::new(engine.catalog.clone())),
        )?;
        engine
            .df_ctx
            .register_catalog("runtimedb", runtimedb_catalog);

        Ok(engine)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Generate a test secret key (base64-encoded 32 bytes)
    fn test_secret_key() -> String {
        use base64::{engine::general_purpose::STANDARD, Engine};
        use rand::RngCore;
        let mut key = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut key);
        STANDARD.encode(key)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_builder_pattern() {
        let temp_dir = TempDir::new().unwrap();
        let base_dir = temp_dir.path().to_path_buf();
        let catalog_path = base_dir.join("catalog.db");

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
        let engine = RuntimeEngine::builder()
            .base_dir(base_dir.clone())
            .catalog(catalog)
            .storage(storage)
            .secret_key(test_secret_key())
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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_builder_with_defaults() {
        // Test that builder creates defaults when only base_dir is provided
        let temp_dir = TempDir::new().unwrap();
        let result = RuntimeEngine::builder()
            .base_dir(temp_dir.path().to_path_buf())
            .secret_key(test_secret_key())
            .build()
            .await;
        assert!(
            result.is_ok(),
            "Builder should create engine with defaults: {:?}",
            result.err()
        );

        let engine = result.unwrap();
        let connections = engine.list_connections().await;
        assert!(connections.is_ok(), "Should be able to list connections");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_builder_uses_default_key_without_secret_key() {
        // Test that builder uses default insecure key when no secret key is provided
        let temp_dir = TempDir::new().unwrap();

        // Temporarily clear the env var if set
        let old_key = std::env::var("RUNTIMEDB_SECRET_KEY").ok();
        std::env::remove_var("RUNTIMEDB_SECRET_KEY");

        let result = RuntimeEngine::builder()
            .base_dir(temp_dir.path().to_path_buf())
            .build()
            .await;

        // Restore env var if it was set
        if let Some(key) = old_key {
            std::env::set_var("RUNTIMEDB_SECRET_KEY", key);
        }

        // Should succeed with default insecure key (with warnings logged)
        assert!(
            result.is_ok(),
            "Builder should succeed with default key: {:?}",
            result.err()
        );

        let engine = result.unwrap();
        let connections = engine.list_connections().await;
        assert!(connections.is_ok(), "Should be able to list connections");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_from_config_sqlite_filesystem() {
        use crate::config::{
            AppConfig, CatalogConfig, PathsConfig, SecretsConfig, ServerConfig, StorageConfig,
        };

        let temp_dir = TempDir::new().unwrap();
        let base_dir = temp_dir.path().to_path_buf();

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
                base_dir: Some(base_dir.to_str().unwrap().to_string()),
                cache_dir: None,
            },
            secrets: SecretsConfig {
                encryption_key: Some(test_secret_key()),
            },
        };

        let engine = RuntimeEngine::from_config(&config).await;
        assert!(
            engine.is_ok(),
            "from_config should create engine successfully: {:?}",
            engine.err()
        );

        let engine = engine.unwrap();

        // Verify we can list connections
        let connections = engine.list_connections().await;
        assert!(connections.is_ok(), "Should be able to list connections");
    }
}
