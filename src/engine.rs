use crate::catalog::{
    CatalogManager, ConnectionInfo, QueryResult, SqliteCatalogManager, TableInfo,
};
use crate::datafetch::native::StreamingParquetWriter;
use crate::datafetch::{BatchWriter, FetchOrchestrator, NativeFetcher};
use crate::datafusion::{
    block_on, InformationSchemaProvider, RuntimeCatalogProvider, RuntimeDbCatalogProvider,
};
use crate::http::models::{
    ConnectionRefreshResult, ConnectionSchemaError, RefreshWarning, SchemaRefreshResult,
    TableRefreshError, TableRefreshResult,
};
use crate::secrets::{EncryptedCatalogBackend, SecretManager, ENCRYPTED_PROVIDER_TYPE};
use crate::source::Source;
use crate::storage::{FilesystemStorage, StorageManager};
use anyhow::Result;
use chrono::Utc;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::CatalogProvider;
use datafusion::prelude::*;
use log::{info, warn};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::error;

/// Default insecure encryption key for development use only.
/// This key is publicly known and provides NO security.
/// It is a base64-encoded 32-byte key: "INSECURE_DEFAULT_KEY_RUNTIMEDB!!"
const DEFAULT_INSECURE_KEY: &str = "SU5TRUNVUkVfREVGQVVMVF9LRVlfUlVOVElNRURCISE=";

/// Maximum number of retries for file deletion before giving up.
/// After this many failures, the pending deletion record is removed
/// and a warning is logged. This prevents indefinite accumulation
/// of stuck deletion records.
const MAX_DELETION_RETRIES: i32 = 5;

/// Default number of parallel table refreshes for connection-wide data refresh.
const DEFAULT_PARALLEL_REFRESH_COUNT: usize = 4;

/// Default interval (in seconds) between deletion worker runs.
const DEFAULT_DELETION_WORKER_INTERVAL_SECS: u64 = 30;

/// Connection ID used for internal runtimedb storage (results, etc.)
const INTERNAL_CONNECTION_ID: i32 = 0;

/// Result of a query execution with optional persistence.
pub struct QueryResponse {
    pub schema: Arc<Schema>,
    pub results: Vec<RecordBatch>,
    pub execution_time: Duration,
}

/// Internal result from writing parquet file, containing the handle for finalization.
struct ParquetWriteResult {
    handle: crate::storage::CacheWriteHandle,
}

/// The main query engine that manages connections, catalogs, and query execution.
pub struct RuntimeEngine {
    catalog: Arc<dyn CatalogManager>,
    df_ctx: SessionContext,
    storage: Arc<dyn StorageManager>,
    orchestrator: Arc<FetchOrchestrator>,
    secret_manager: Arc<SecretManager>,
    shutdown_token: CancellationToken,
    deletion_worker_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    deletion_grace_period: Duration,
    /// Stored for potential future restart capability; interval is passed to worker at start
    #[allow(dead_code)]
    deletion_worker_interval: Duration,
    parallel_refresh_count: usize,
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

    /// Delete cache directory for a specific table.
    async fn delete_table_files(&self, table_info: &TableInfo) -> Result<()> {
        // Delete versioned cache directory if it exists
        if let Some(parquet_path) = &table_info.parquet_path {
            if let Err(e) = self.storage.delete_prefix(parquet_path).await {
                warn!("Failed to delete cache directory {}: {}", parquet_path, e);
            }
        }

        Ok(())
    }

    /// Get a reference to the storage manager
    pub fn storage(&self) -> &Arc<dyn StorageManager> {
        &self.storage
    }

    /// Get a reference to the DataFusion session context.
    pub fn session_context(&self) -> &SessionContext {
        &self.df_ctx
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
    /// Use `refresh_schema()` to discover tables after registration.
    ///
    /// The Source should already contain a Credential with the secret ID if authentication
    /// is required. The secret_id is extracted from the Source for DB queryability.
    pub async fn register_connection(&self, name: &str, source: Source) -> Result<String> {
        let source_type = source.source_type();
        // Extract secret_id from the Source's credential for DB storage (denormalized for queries)
        let secret_id = source.secret_id().map(|s| s.to_string());

        // Store config as JSON (includes "type" from serde tag)
        let config_json = serde_json::to_string(&source)?;
        let external_id = self
            .catalog
            .add_connection(name, source_type, &config_json, secret_id.as_deref())
            .await?;

        // Get the connection to retrieve internal id for RuntimeCatalogProvider
        let conn = self
            .catalog
            .get_connection_by_external_id(&external_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Connection not found after creation"))?;

        // Register with DataFusion (empty catalog - no tables yet)
        let catalog_provider = Arc::new(RuntimeCatalogProvider::new(
            conn.id,
            name.to_string(),
            Arc::new(source),
            self.catalog.clone(),
            self.orchestrator.clone(),
        )) as Arc<dyn CatalogProvider>;

        self.df_ctx.register_catalog(name, catalog_provider);

        info!("Connection '{}' registered (discovery pending)", name);

        Ok(external_id)
    }

    /// Connect to a new external data source and register it as a catalog.
    ///
    /// This is a convenience method that combines `register_connection()` and
    /// `refresh_schema()`. For more control, use those methods separately.
    pub async fn connect(&self, name: &str, source: Source) -> Result<String> {
        let connection_id = self.register_connection(name, source).await?;
        self.refresh_schema(&connection_id).await?;
        Ok(connection_id)
    }

    /// Get a cloned catalog manager that shares the same underlying connection.
    pub fn catalog(&self) -> Arc<dyn CatalogManager> {
        self.catalog.clone()
    }

    /// List all configured connections.
    pub async fn list_connections(&self) -> Result<Vec<ConnectionInfo>> {
        self.catalog.list_connections().await
    }

    /// List all tables, optionally filtered by connection external ID.
    pub async fn list_tables(&self, connection_id: Option<&str>) -> Result<Vec<TableInfo>> {
        let internal_id = if let Some(ext_id) = connection_id {
            match self.catalog.get_connection_by_external_id(ext_id).await? {
                Some(conn) => Some(conn.id),
                None => anyhow::bail!("Connection '{}' not found", ext_id),
            }
        } else {
            None
        };

        self.catalog.list_tables(internal_id).await
    }

    /// Execute a SQL query and return the results.
    pub async fn execute_query(&self, sql: &str) -> Result<QueryResponse> {
        info!("Executing query: {}", sql);
        let start = Instant::now();
        let df = self.df_ctx.sql(sql).await.map_err(|e| {
            error!("Error executing query: {}", e);
            e
        })?;
        let schema = Arc::new(Schema::from(df.schema()));
        let results = df.collect().await.map_err(|e| {
            error!("Error getting query result: {}", e);
            e
        })?;
        info!("Execution completed in {:?}", start.elapsed());
        info!("Results available");

        Ok(QueryResponse {
            schema,
            execution_time: start.elapsed(),
            results,
        })
    }

    /// Persist query results to storage and catalog.
    ///
    /// Returns the result ID on success, or an error if persistence fails.
    /// This is a best-effort operation - callers should handle errors gracefully
    /// since the query results are still valid even if persistence fails.
    pub async fn persist_result(
        &self,
        schema: &Arc<Schema>,
        batches: &[RecordBatch],
    ) -> Result<String> {
        let result_id = crate::id::generate_result_id();

        // Write to parquet
        let parquet_path = self.write_results_to_parquet(&result_id, schema, batches)?;

        // Finalize (uploads to S3 if needed) and get directory URL
        let dir_url = self
            .storage
            .finalize_cache_write(&parquet_path.handle)
            .await?;

        // Directory URL already includes the version subdirectory, and the file is data.parquet
        let file_url = format!("{}/data.parquet", dir_url);

        // Store in catalog
        let query_result = QueryResult {
            id: result_id.clone(),
            parquet_path: file_url,
            created_at: Utc::now(),
        };

        self.catalog.store_result(&query_result).await?;

        Ok(result_id)
    }

    /// Retrieve a persisted query result by ID.
    ///
    /// Returns the schema and record batches for the result, or None if not found.
    pub async fn get_result(&self, id: &str) -> Result<Option<(Arc<Schema>, Vec<RecordBatch>)>> {
        // Look up result in catalog
        let result = match self.catalog.get_result(id).await? {
            Some(r) => r,
            None => return Ok(None),
        };

        // Load results from parquet
        let df = self
            .df_ctx
            .read_parquet(
                &result.parquet_path,
                datafusion::prelude::ParquetReadOptions::default(),
            )
            .await?;

        let schema = Arc::new(Schema::from(df.schema()));
        let batches = df.collect().await?;

        Ok(Some((schema, batches)))
    }

    /// List persisted query results with pagination.
    ///
    /// Returns results ordered by creation time (newest first).
    /// The `has_more` flag indicates if there are additional results beyond this page.
    pub async fn list_results(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<crate::catalog::QueryResult>, bool)> {
        self.catalog.list_results(limit, offset).await
    }

    /// Write result batches to a parquet file.
    ///
    /// Note: Empty results are persisted with schema only. This ensures GET /results/{id}
    /// works for all result IDs returned by POST /query. A future optimization could avoid
    /// persisting empty results entirely if storage space becomes a concern.
    fn write_results_to_parquet(
        &self,
        result_id: &str,
        schema: &Arc<Schema>,
        batches: &[RecordBatch],
    ) -> Result<ParquetWriteResult> {
        // Prepare write location - using result_id as both schema and table
        // This creates path: {base}/0/runtimedb_results/{result_id}/{version}/data.parquet
        let handle = self.storage.prepare_cache_write(
            INTERNAL_CONNECTION_ID,
            "runtimedb_results",
            result_id,
        );

        // Write parquet file
        let mut writer: Box<dyn BatchWriter> =
            Box::new(StreamingParquetWriter::new(handle.local_path.clone()));
        writer.init(schema)?;

        for batch in batches {
            writer.write_batch(batch)?;
        }

        writer.close()?;

        Ok(ParquetWriteResult { handle })
    }

    /// Purge all cached data for a connection (clears parquet files and resets sync state).
    pub async fn purge_connection(&self, connection_id: &str) -> Result<()> {
        // Get connection info (validates it exists and gives us the ID)
        let conn = self
            .catalog
            .get_connection_by_external_id(connection_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Connection '{}' not found", connection_id))?;

        // Step 1: Clear metadata first (metadata-first ordering)
        self.catalog
            .clear_connection_cache_metadata(&conn.name)
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
        self.df_ctx.register_catalog(&conn.name, catalog_provider);

        // Step 3: Delete the physical files (now that DataFusion has released file handles)
        self.delete_connection_files(conn.id).await?;

        Ok(())
    }

    /// Purge cached data for a single table (clears parquet file and resets sync state).
    pub async fn purge_table(
        &self,
        connection_id: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<()> {
        // Validate connection exists
        let conn = self
            .catalog
            .get_connection_by_external_id(connection_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Connection '{}' not found", connection_id))?;

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
        self.df_ctx.register_catalog(&conn.name, catalog_provider);

        // Step 3: Delete the physical files (now that DataFusion has released file handles)
        self.delete_table_files(&table_info).await?;

        Ok(())
    }

    /// Remove a connection entirely (removes from catalog and deletes all data).
    ///
    /// If the connection has an associated secret that is not used by any other
    /// connections, the secret will also be deleted.
    pub async fn remove_connection(&self, external_id: &str) -> Result<()> {
        // Get connection info (validates it exists and gives us the ID)
        let conn = self
            .catalog
            .get_connection_by_external_id(external_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Connection '{}' not found", external_id))?;

        // Capture secret_id before deleting the connection
        let secret_id = conn.secret_id.clone();

        // Step 1: Delete metadata first
        self.catalog.delete_connection(&conn.name).await?;

        // Step 2: Delete the physical files
        self.delete_connection_files(conn.id).await?;

        // Step 3: Clean up orphaned secret if applicable
        if let Some(ref secret_id) = secret_id {
            let remaining = self
                .catalog
                .count_connections_by_secret_id(secret_id)
                .await?;
            if remaining == 0 {
                // Get secret metadata to find the name for deletion
                if let Ok(metadata) = self.secret_manager.get_metadata_by_id(secret_id).await {
                    if let Err(e) = self.secret_manager.delete(&metadata.name).await {
                        // Log but don't fail - the connection is already deleted
                        tracing::warn!(
                            secret_id = %secret_id,
                            secret_name = %metadata.name,
                            error = %e,
                            "Failed to delete orphaned secret"
                        );
                    }
                }
            }
        }

        // Note: DataFusion doesn't support deregistering catalogs in v50.3.0
        // The catalog will remain registered but will return no schemas/tables
        // since the metadata catalog has been cleaned up

        Ok(())
    }

    /// Set the default catalog for queries (allows queries without fully-qualified table names).
    pub async fn set_default_catalog(&self, connection_id: &str) -> Result<()> {
        // Validate connection exists and get its name for DataFusion
        let conn = self
            .catalog
            .get_connection_by_external_id(connection_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Connection '{}' not found", connection_id))?;

        let set_sql = format!("SET datafusion.catalog.default_catalog = '{}'", conn.name);
        self.df_ctx.sql(&set_sql).await?;

        Ok(())
    }

    /// Sync all tables for a connection using DLT.
    /// This forces a sync of all tables, even if they're already cached.
    /// Tables are synced in parallel with a concurrency limit of 10.
    pub async fn sync_connection(&self, _connection_id: &str) -> Result<()> {
        todo!("Implement connections sync")
    }

    /// Shutdown the engine and close all connections.
    /// This should be called before the application exits to ensure proper cleanup.
    pub async fn shutdown(&self) -> Result<()> {
        // Signal the deletion worker to stop
        self.shutdown_token.cancel();

        // Wait for the deletion worker to finish
        if let Some(handle) = self.deletion_worker_handle.lock().await.take() {
            // We use a timeout to avoid blocking forever if the worker is stuck
            let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
        }

        self.catalog.close().await
    }

    /// Schedule file deletion after grace period (persisted to database).
    async fn schedule_file_deletion(&self, path: &str) -> Result<()> {
        let grace_period = chrono::Duration::from_std(self.deletion_grace_period)
            .map_err(|e| anyhow::anyhow!("Invalid grace period duration: {}", e))?;
        let delete_after = Utc::now() + grace_period;
        self.catalog
            .schedule_file_deletion(path, delete_after)
            .await
    }

    /// Refresh schema for a connection. Re-discovers tables from remote,
    /// preserving cached data for existing tables.
    ///
    /// Tables that no longer exist in the remote source will remain in the catalog
    /// (they are not automatically deleted).
    ///
    /// Returns counts of (added, modified).
    pub async fn refresh_schema(&self, connection_id: &str) -> Result<(usize, usize)> {
        let conn = self
            .catalog
            .get_connection_by_external_id(connection_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Connection '{}' not found", connection_id))?;

        let existing_tables = self.catalog.list_tables(Some(conn.id)).await?;
        let existing_set: HashSet<(String, String)> = existing_tables
            .iter()
            .map(|t| (t.schema_name.clone(), t.table_name.clone()))
            .collect();

        let source: Source = serde_json::from_str(&conn.config_json)?;
        let discovered = self.orchestrator.discover_tables(&source).await?;

        let current_set: HashSet<(String, String)> = discovered
            .iter()
            .map(|t| (t.schema_name.clone(), t.table_name.clone()))
            .collect();

        let added_count = current_set.difference(&existing_set).count();

        let mut modified = 0;
        for table in &discovered {
            let schema_json = serde_json::to_string(&table.to_arrow_schema())?;

            if let Some(existing) = existing_tables
                .iter()
                .find(|t| t.schema_name == table.schema_name && t.table_name == table.table_name)
            {
                if existing.arrow_schema_json.as_ref() != Some(&schema_json) {
                    modified += 1;
                }
            }

            self.catalog
                .add_table(conn.id, &table.schema_name, &table.table_name, &schema_json)
                .await?;
        }

        Ok((added_count, modified))
    }

    /// Refresh schema for all connections.
    /// Continues processing remaining connections if one fails.
    pub async fn refresh_all_schemas(&self) -> Result<SchemaRefreshResult> {
        let connections = self.catalog.list_connections().await?;
        let mut result = SchemaRefreshResult {
            connections_refreshed: 0,
            connections_failed: 0,
            tables_discovered: 0,
            tables_added: 0,
            tables_modified: 0,
            errors: Vec::new(),
        };

        for conn in connections {
            match self.refresh_schema(&conn.external_id).await {
                Ok((added, modified)) => {
                    result.connections_refreshed += 1;
                    result.tables_added += added;
                    result.tables_modified += modified;
                }
                Err(e) => {
                    tracing::warn!(
                        connection_id = %conn.external_id,
                        error = %e,
                        "Failed to refresh schema for connection"
                    );
                    result.connections_failed += 1;
                    result.errors.push(ConnectionSchemaError {
                        connection_id: conn.external_id.clone(),
                        error: e.to_string(),
                    });
                }
            }
        }

        result.tables_discovered = self.catalog.list_tables(None).await?.len();
        Ok(result)
    }

    /// Refresh data for a single table using atomic swap.
    pub async fn refresh_table_data(
        &self,
        connection_id: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<TableRefreshResult> {
        let start = std::time::Instant::now();
        let mut warnings = Vec::new();

        let conn = self
            .catalog
            .get_connection_by_external_id(connection_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Connection '{}' not found", connection_id))?;
        let source: Source = serde_json::from_str(&conn.config_json)?;

        let (_, old_path, rows_synced) = self
            .orchestrator
            .refresh_table(&source, conn.id, schema_name, table_name)
            .await?;

        if let Some(path) = old_path {
            if let Err(e) = self.schedule_file_deletion(&path).await {
                tracing::warn!(
                    schema = schema_name,
                    table = table_name,
                    path = %path,
                    error = %e,
                    "Failed to schedule deletion of old cache file"
                );
                warnings.push(RefreshWarning {
                    schema_name: Some(schema_name.to_string()),
                    table_name: Some(table_name.to_string()),
                    message: format!("Failed to schedule deletion of old cache: {}", e),
                });
            }
        }

        Ok(TableRefreshResult {
            connection_id: connection_id.to_string(),
            schema_name: schema_name.to_string(),
            table_name: table_name.to_string(),
            rows_synced,
            duration_ms: start.elapsed().as_millis() as u64,
            warnings,
        })
    }

    /// Refresh data for tables in a connection.
    ///
    /// By default, only refreshes tables that already have cached data (parquet_path is set).
    /// Set `include_uncached` to true to also sync tables that haven't been cached yet.
    pub async fn refresh_connection_data(
        &self,
        connection_id: &str,
        include_uncached: bool,
    ) -> Result<ConnectionRefreshResult> {
        let start = std::time::Instant::now();

        let conn = self
            .catalog
            .get_connection_by_external_id(connection_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Connection '{}' not found", connection_id))?;
        let internal_id = conn.id;

        let all_tables = self.catalog.list_tables(Some(internal_id)).await?;

        // By default, only refresh tables that already have cached data
        let tables: Vec<_> = if include_uncached {
            all_tables
        } else {
            all_tables
                .into_iter()
                .filter(|t| t.parquet_path.is_some())
                .collect()
        };

        let source: Source = serde_json::from_str(&conn.config_json)?;

        let mut result = ConnectionRefreshResult {
            connection_id: connection_id.to_string(),
            tables_refreshed: 0,
            tables_failed: 0,
            total_rows: 0,
            duration_ms: 0,
            errors: vec![],
            warnings: vec![],
        };

        let semaphore = Arc::new(Semaphore::new(self.parallel_refresh_count));
        let mut handles = vec![];

        for table in tables {
            let permit = semaphore.clone().acquire_owned().await?;
            let orchestrator = self.orchestrator.clone();
            let source = source.clone();
            let schema_name = table.schema_name.clone();
            let table_name = table.table_name.clone();

            let handle = tokio::spawn(async move {
                let result = orchestrator
                    .refresh_table(&source, internal_id, &schema_name, &table_name)
                    .await;
                drop(permit);
                (schema_name, table_name, result)
            });
            handles.push(handle);
        }

        for handle in handles {
            let (schema_name, table_name, refresh_result) = handle.await?;
            match refresh_result {
                Ok((_, old_path, rows_synced)) => {
                    result.tables_refreshed += 1;
                    result.total_rows += rows_synced;
                    if let Some(path) = old_path {
                        if let Err(e) = self.schedule_file_deletion(&path).await {
                            tracing::warn!(
                                schema = %schema_name,
                                table = %table_name,
                                path = %path,
                                error = %e,
                                "Failed to schedule deletion of old cache file"
                            );
                            result.warnings.push(RefreshWarning {
                                schema_name: Some(schema_name.clone()),
                                table_name: Some(table_name.clone()),
                                message: format!("Failed to schedule deletion of old cache: {}", e),
                            });
                        }
                    }
                }
                Err(e) => {
                    result.tables_failed += 1;
                    result.errors.push(TableRefreshError {
                        schema_name,
                        table_name,
                        error: e.to_string(),
                    });
                }
            }
        }

        result.duration_ms = start.elapsed().as_millis() as u64;
        Ok(result)
    }

    /// Process any pending directory deletions that are due.
    pub async fn process_pending_deletions(&self) -> Result<usize> {
        let pending = self.catalog.get_pending_deletions().await?;
        let mut deleted = 0;

        for deletion in pending {
            match self.storage.delete_prefix(&deletion.path).await {
                Ok(_) => {
                    self.catalog.remove_pending_deletion(deletion.id).await?;
                    deleted += 1;
                }
                Err(e) => {
                    warn!("Failed to delete {}: {}", deletion.path, e);
                }
            }
        }

        Ok(deleted)
    }

    /// Start background task that processes pending deletions periodically.
    /// Returns the JoinHandle for the spawned task.
    fn start_deletion_worker(
        catalog: Arc<dyn CatalogManager>,
        storage: Arc<dyn StorageManager>,
        shutdown_token: CancellationToken,
        interval_duration: Duration,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval_duration);
            loop {
                tokio::select! {
                    _ = shutdown_token.cancelled() => {
                        info!("Deletion worker received shutdown signal");
                        break;
                    }
                    _ = interval.tick() => {
                        let pending = match catalog.get_pending_deletions().await {
                            Ok(p) => p,
                            Err(e) => {
                                warn!("Failed to get pending deletions: {}", e);
                                continue;
                            }
                        };

                        for deletion in pending {
                            match storage.delete_prefix(&deletion.path).await {
                                Ok(_) => {
                                    // Successfully deleted - remove the record
                                    if let Err(e) = catalog.remove_pending_deletion(deletion.id).await {
                                        warn!("Failed to remove deletion record {}: {}", deletion.id, e);
                                    }
                                }
                                Err(e) => {
                                    // Failed to delete - increment retry count
                                    warn!(
                                        "Failed to delete {} (attempt {}): {}",
                                        deletion.path,
                                        deletion.retry_count + 1,
                                        e
                                    );

                                    match catalog.increment_deletion_retry(deletion.id).await {
                                        Ok(new_count) if new_count >= MAX_DELETION_RETRIES => {
                                            // Max retries reached - give up and remove record
                                            warn!(
                                                "Giving up on deleting {} after {} failed attempts",
                                                deletion.path, new_count
                                            );
                                            if let Err(e) = catalog.remove_pending_deletion(deletion.id).await {
                                                warn!("Failed to remove stuck deletion record {}: {}", deletion.id, e);
                                            }
                                        }
                                        Ok(_) => {
                                            // Will retry on next interval
                                        }
                                        Err(e) => {
                                            warn!("Failed to increment retry count for {}: {}", deletion.id, e);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        })
    }
}

impl Drop for RuntimeEngine {
    fn drop(&mut self) {
        // Signal the deletion worker to stop
        self.shutdown_token.cancel();

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
    deletion_grace_period: Duration,
    deletion_worker_interval: Duration,
    parallel_refresh_count: usize,
}

impl Default for RuntimeEngineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Default grace period for file deletion (60 seconds).
const DEFAULT_DELETION_GRACE_PERIOD: Duration = Duration::from_secs(60);

impl RuntimeEngineBuilder {
    pub fn new() -> Self {
        Self {
            base_dir: None,
            cache_dir: None,
            catalog: None,
            storage: None,
            secret_key: std::env::var("RUNTIMEDB_SECRET_KEY").ok(),
            deletion_grace_period: DEFAULT_DELETION_GRACE_PERIOD,
            deletion_worker_interval: Duration::from_secs(DEFAULT_DELETION_WORKER_INTERVAL_SECS),
            parallel_refresh_count: DEFAULT_PARALLEL_REFRESH_COUNT,
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

    /// Set the grace period for file deletion.
    /// When cached files are replaced during refresh operations, the old files
    /// are scheduled for deletion after this grace period to allow in-flight
    /// queries to complete. Defaults to 60 seconds.
    pub fn deletion_grace_period(mut self, duration: Duration) -> Self {
        self.deletion_grace_period = duration;
        self
    }

    /// Set the interval between deletion worker runs.
    /// The deletion worker processes pending file deletions periodically.
    /// Defaults to 30 seconds.
    pub fn deletion_worker_interval(mut self, duration: Duration) -> Self {
        self.deletion_worker_interval = duration;
        self
    }

    /// Set the number of parallel table refreshes for connection-wide data refresh.
    /// Higher values may speed up refresh for connections with many small tables,
    /// but could overwhelm the source database. Defaults to 4. Minimum value is 1
    /// (values less than 1 are clamped to 1 to prevent deadlock).
    pub fn parallel_refresh_count(mut self, count: usize) -> Self {
        self.parallel_refresh_count = count.max(1);
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

        // Create shutdown token for graceful shutdown
        let shutdown_token = CancellationToken::new();

        // Start background deletion worker
        let deletion_worker_handle = RuntimeEngine::start_deletion_worker(
            catalog.clone(),
            storage.clone(),
            shutdown_token.clone(),
            self.deletion_worker_interval,
        );

        let mut engine = RuntimeEngine {
            catalog,
            df_ctx,
            storage,
            orchestrator,
            secret_manager,
            shutdown_token,
            deletion_worker_handle: Mutex::new(Some(deletion_worker_handle)),
            deletion_grace_period: self.deletion_grace_period,
            deletion_worker_interval: self.deletion_worker_interval,
            parallel_refresh_count: self.parallel_refresh_count,
        };

        // Register all existing connections as DataFusion catalogs
        engine.register_existing_connections().await?;

        // Register the runtimedb virtual catalog for system metadata and results
        // Pass df_ctx so the results schema can access registered object stores (S3, etc.)
        let runtimedb_catalog = Arc::new(RuntimeDbCatalogProvider::new(
            engine.catalog.clone(),
            &engine.df_ctx,
        ));
        runtimedb_catalog.register_schema(
            "information_schema",
            Arc::new(InformationSchemaProvider::new(engine.catalog.clone())),
        )?;
        engine
            .df_ctx
            .register_catalog("runtimedb", runtimedb_catalog);

        // Process any pending deletions from previous runs
        if let Err(e) = engine.process_pending_deletions().await {
            warn!("Failed to process pending deletions on startup: {}", e);
        }

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
    async fn test_deletion_worker_stops_on_shutdown() {
        let temp_dir = TempDir::new().unwrap();
        let base_dir = temp_dir.path().to_path_buf();

        // Create engine
        let engine = RuntimeEngine::builder()
            .base_dir(base_dir)
            .secret_key(test_secret_key())
            .build()
            .await
            .unwrap();

        // Verify the worker handle exists before shutdown
        assert!(
            engine.deletion_worker_handle.lock().await.is_some(),
            "Worker handle should exist after engine creation"
        );

        // Trigger shutdown
        engine.shutdown().await.unwrap();

        // After shutdown, we should be able to verify the worker task completed
        // The handle should have been awaited during shutdown
        // Note: We can't check the handle directly after shutdown since it's consumed,
        // but we can verify the shutdown token was cancelled

        // Schedule a deletion in the past
        let past = Utc::now() - chrono::Duration::seconds(10);
        engine
            .catalog
            .schedule_file_deletion("/tmp/test.parquet", past)
            .await
            .unwrap();

        // Wait a bit - if worker was still running, it would process this
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // The deletion should still be pending (worker stopped)
        let due = engine.catalog.get_pending_deletions().await.unwrap();
        assert_eq!(
            due.len(),
            1,
            "Worker should have stopped and not processed deletion"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_configurable_deletion_grace_period() {
        let temp_dir = TempDir::new().unwrap();
        let base_dir = temp_dir.path().to_path_buf();

        // Create engine with a custom grace period of 300 seconds (5 minutes)
        let custom_grace_period = Duration::from_secs(300);
        let engine = RuntimeEngine::builder()
            .base_dir(base_dir)
            .secret_key(test_secret_key())
            .deletion_grace_period(custom_grace_period)
            .build()
            .await
            .unwrap();

        // Schedule a file deletion
        engine
            .schedule_file_deletion("/tmp/test_grace_period.parquet")
            .await
            .unwrap();

        // Get the pending deletions - should not be due yet since grace period is 5 minutes
        let pending = engine.catalog.get_pending_deletions().await.unwrap();
        assert!(
            pending.is_empty(),
            "With 300s grace period, deletion should not be due immediately"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_default_deletion_grace_period() {
        let temp_dir = TempDir::new().unwrap();
        let base_dir = temp_dir.path().to_path_buf();

        // Create engine with default grace period (should be 60 seconds)
        let engine = RuntimeEngine::builder()
            .base_dir(base_dir)
            .secret_key(test_secret_key())
            .build()
            .await
            .unwrap();

        // Schedule a file deletion
        engine
            .schedule_file_deletion("/tmp/test_default_grace.parquet")
            .await
            .unwrap();

        // Verify it's not immediately due (within the grace period)
        let pending = engine.catalog.get_pending_deletions().await.unwrap();
        assert!(
            pending.is_empty(),
            "With default 60s grace period, deletion should not be due immediately"
        );
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

    #[test]
    fn test_parallel_refresh_count_clamps_to_minimum_one() {
        // Test that parallel_refresh_count is clamped to at least 1
        // to prevent deadlock on semaphore acquisition
        let builder = RuntimeEngineBuilder::new().parallel_refresh_count(0);
        assert_eq!(
            builder.parallel_refresh_count, 1,
            "parallel_refresh_count should be clamped to 1 when set to 0"
        );

        // Values >= 1 should be preserved
        let builder = RuntimeEngineBuilder::new().parallel_refresh_count(5);
        assert_eq!(builder.parallel_refresh_count, 5);

        let builder = RuntimeEngineBuilder::new().parallel_refresh_count(1);
        assert_eq!(builder.parallel_refresh_count, 1);
    }
}
