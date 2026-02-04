use crate::catalog::{
    is_dataset_table_name_conflict, CachingCatalogManager, CatalogManager, ConnectionInfo,
    ResultStatus, ResultUpdate, SqliteCatalogManager, TableInfo,
};
use crate::datafetch::native::StreamingParquetWriter;
use crate::datafetch::{BatchWriter, FetchOrchestrator, NativeFetcher};
use crate::datafusion::{InMemoryTableCache, ParquetCacheManager, UnifiedCatalogList};
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
use datafusion::catalog::AsyncCatalogProviderList;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::*;
use datafusion_tracing::{instrument_with_info_spans, InstrumentationOptions};
use instrumented_object_store::instrument_object_store;
use liquid_cache_client::PushdownOptimizer;
use object_store::ObjectStore;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::error;
use tracing::{info, warn, Instrument};

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

/// Default maximum number of concurrent result persistence tasks.
/// This limits how many background tasks can write parquet files simultaneously.
const DEFAULT_MAX_CONCURRENT_PERSISTENCE: usize = 32;

/// Default interval (in seconds) between deletion worker runs.
const DEFAULT_DELETION_WORKER_INTERVAL_SECS: u64 = 30;

/// Default interval (in seconds) between stale result cleanup runs.
const DEFAULT_STALE_RESULT_CLEANUP_INTERVAL_SECS: u64 = 60;

/// Default timeout (in seconds) after which pending/processing results are considered stale.
const DEFAULT_STALE_RESULT_TIMEOUT_SECS: u64 = 300;

/// Connection ID used for internal runtimedb storage (results, etc.)
/// This uses a leading underscore to ensure it cannot conflict with user-created
/// connection IDs, which use the "conn_" prefix from nanoid generation.
const INTERNAL_CONNECTION_ID: &str = "_runtimedb_internal";

/// Schema name for storing query results.
const RESULTS_SCHEMA_NAME: &str = "runtimedb_results";

/// Result of a query execution with optional persistence.
pub struct QueryResponse {
    pub schema: Arc<Schema>,
    pub results: Vec<RecordBatch>,
    pub execution_time: Duration,
}

/// Result of a query execution with async persistence.
///
/// This struct is returned by [`RuntimeEngine::execute_query_with_persistence`].
/// Unlike [`QueryResponse`], this includes a `result_id` that can be used to
/// retrieve the persisted result later via the `/results/{id}` API endpoint.
///
/// The persistence happens asynchronously in a background task after this
/// struct is returned. The result status can be checked via `GET /results/{id}`:
/// - `"processing"` - Persistence is still in progress
/// - `"ready"` - Result is fully persisted and available
/// - `"failed"` - Persistence failed (check `error_message` for details)
pub struct QueryResponseWithId {
    /// Unique identifier for the persisted result.
    pub result_id: String,
    /// Arrow schema describing the result columns.
    pub schema: Arc<Schema>,
    /// The query result data (available immediately, regardless of persistence status).
    pub results: Vec<RecordBatch>,
    /// Time taken to execute the query (excluding persistence).
    pub execution_time: Duration,
}

/// The main query engine that manages connections, catalogs, and query execution.
pub struct RuntimeEngine {
    catalog: Arc<dyn CatalogManager>,
    df_ctx: SessionContext,
    storage: Arc<dyn StorageManager>,
    orchestrator: Arc<FetchOrchestrator>,
    secret_manager: Arc<SecretManager>,
    /// Unified async catalog list for all catalogs (connections, datasets, runtimedb).
    unified_catalog_list: Arc<UnifiedCatalogList>,
    shutdown_token: CancellationToken,
    deletion_worker_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    deletion_grace_period: Duration,
    /// Stored for potential future restart capability; interval is passed to worker at start
    #[allow(dead_code)]
    deletion_worker_interval: Duration,
    parallel_refresh_count: usize,
    /// Semaphore to limit concurrent result persistence tasks.
    /// This prevents unbounded task spawning under burst load.
    persistence_semaphore: Arc<Semaphore>,
    /// Set of in-flight persistence tasks. JoinSet automatically removes completed tasks
    /// when polled, preventing unbounded memory growth during high load.
    persistence_tasks: Mutex<JoinSet<()>>,
    /// Handle for the stale result cleanup worker task.
    stale_result_cleanup_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    parquet_cache: Option<Arc<ParquetCacheManager>>,
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

        if config.liquid_cache.enabled {
            let server = config.liquid_cache.server_address.as_ref().ok_or_else(|| {
                anyhow::anyhow!("liquid cache enabled but server_address not set")
            })?;
            info!("Enabling liquid cache with server: {}", server);
            builder = builder.liquid_cache_server(server.clone());
        }

        // Pass cache config to builder
        builder = builder.cache_config(config.cache.clone());

        // Set engine config
        builder = builder.max_concurrent_persistence(config.engine.max_concurrent_persistence);
        builder = builder.stale_result_cleanup_interval(Duration::from_secs(
            config.engine.stale_result_cleanup_interval_secs,
        ));
        builder = builder
            .stale_result_timeout(Duration::from_secs(config.engine.stale_result_timeout_secs));
        builder = builder
            .in_memory_cache_total_max_bytes(config.engine.in_memory_cache_total_max_bytes)
            .in_memory_cache_table_max_bytes(config.engine.in_memory_cache_table_max_bytes)
            .in_memory_cache_ttl(Duration::from_secs(config.engine.in_memory_cache_ttl_secs))
            .in_memory_cache_max_concurrent_loads(
                config.engine.in_memory_cache_max_concurrent_loads,
            );

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

                let region = config.storage.region.as_deref().unwrap_or("us-east-1");

                // Get credentials from config first, then fall back to environment
                let access_key = config
                    .storage
                    .access_key
                    .clone()
                    .or_else(|| std::env::var("AWS_ACCESS_KEY_ID").ok())
                    .or_else(|| std::env::var("RUNTIMEDB_STORAGE_ACCESS_KEY_ID").ok());
                let secret_key = config
                    .storage
                    .secret_key
                    .clone()
                    .or_else(|| std::env::var("AWS_SECRET_ACCESS_KEY").ok())
                    .or_else(|| std::env::var("RUNTIMEDB_STORAGE_SECRET_ACCESS_KEY").ok());

                // Custom endpoint (for MinIO/localstack) - uses path-style URLs
                if let Some(endpoint) = &config.storage.endpoint {
                    let access_key = access_key.unwrap_or_else(|| "minioadmin".to_string());
                    let secret_key = secret_key.unwrap_or_else(|| "minioadmin".to_string());
                    let allow_http = endpoint.starts_with("http://");

                    Ok(Arc::new(crate::storage::S3Storage::new_with_endpoint(
                        bucket,
                        endpoint,
                        &access_key,
                        &secret_key,
                        region,
                        allow_http,
                    )?))
                } else if let (Some(access_key), Some(secret_key)) = (access_key, secret_key) {
                    // AWS S3 with explicit credentials from config/env
                    Ok(Arc::new(crate::storage::S3Storage::new_with_credentials(
                        bucket,
                        &access_key,
                        &secret_key,
                        region,
                    )?))
                } else {
                    // Production: use IAM/automatic credential discovery
                    Ok(Arc::new(crate::storage::S3Storage::new_with_iam(
                        bucket, region,
                    )?))
                }
            }
            _ => anyhow::bail!("Unsupported storage type: {}", config.storage.storage_type),
        }
    }

    // =========================================================================
    // Private helpers
    // =========================================================================

    /// Delete cache and state directories for a connection.
    async fn delete_connection_files(&self, connection_id: &str) -> Result<()> {
        let cache_prefix = self.storage.cache_prefix(connection_id);

        if let Some(cache) = &self.parquet_cache {
            cache.cache().invalidate_prefix(&cache_prefix);
        }

        if let Err(e) = self.storage.delete_prefix(&cache_prefix).await {
            warn!("Failed to delete cache prefix {}: {}", cache_prefix, e);
        }

        Ok(())
    }

    /// Delete cache directory for a specific table.
    async fn delete_table_files(&self, table_info: &TableInfo) -> Result<()> {
        // Delete versioned cache directory if it exists
        if let Some(parquet_path) = &table_info.parquet_path {
            if let Some(cache) = &self.parquet_cache {
                cache.cache().invalidate_url(parquet_path);
            }
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

    /// Register a connection without discovering tables.
    ///
    /// This persists the connection config to the catalog metadata store.
    /// The connection catalog will be resolved on-demand during query execution.
    /// Use `refresh_schema()` to discover tables after registration.
    ///
    /// The Source should already contain a Credential with the secret ID if authentication
    /// is required. The secret_id is extracted from the Source for DB queryability.
    #[tracing::instrument(
        name = "register_connection",
        skip(self, source),
        fields(
            runtimedb.connection_name = %name,
            runtimedb.source_type = %source.source_type(),
            runtimedb.connection_id = tracing::field::Empty,
        )
    )]
    pub async fn register_connection(&self, name: &str, source: Source) -> Result<String> {
        let source_type = source.source_type();
        // Extract secret_id from the Source's credential for DB storage (denormalized for queries)
        let secret_id = source.secret_id().map(|s| s.to_string());

        // Store config as JSON (includes "type" from serde tag)
        let config_json = serde_json::to_string(&source)?;
        let connection_id = self
            .catalog
            .add_connection(name, source_type, &config_json, secret_id.as_deref())
            .await?;

        // Note: Connection catalogs are now resolved on-demand during query execution
        // via the async ConnectionsCatalogList. No pre-registration with DataFusion needed.

        tracing::Span::current().record("runtimedb.connection_id", &connection_id);
        info!("Connection '{}' registered (discovery pending)", name);

        Ok(connection_id)
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
        // Validate connection exists if provided
        if let Some(ext_id) = connection_id {
            if self.catalog.get_connection(ext_id).await?.is_none() {
                anyhow::bail!("Connection '{}' not found", ext_id);
            }
        }

        self.catalog.list_tables(connection_id).await
    }

    /// Execute a SQL query and return the results.
    /// This follows the official datafusion remote_catalog example
    /// https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/data_io/remote_catalog.rs
    #[tracing::instrument(
        name = "execute_query",
        skip(self, sql),
        fields(
            runtimedb.sql = tracing::field::Empty,
            runtimedb.rows_returned = tracing::field::Empty,
        )
    )]
    pub async fn execute_query(&self, sql: &str) -> Result<QueryResponse> {
        info!("Executing query: {}", sql);
        let start = Instant::now();
        if crate::telemetry::include_sql_in_traces() {
            tracing::Span::current().record("runtimedb.sql", sql);
        }

        let session_state = self.df_ctx.state();

        // Step 1: Parse SQL into a statement
        let dialect = session_state.config().options().sql_parser.dialect;
        let statement = session_state.sql_to_statement(sql, &dialect).map_err(|e| {
            error!("SQL parse error: {}", e);
            e
        })?;

        // Step 2: Extract table references from the parsed SQL
        let references = session_state
            .resolve_table_references(&statement)
            .map_err(|e| {
                error!("Failed to resolve table references: {}", e);
                e
            })?;

        // Step 3: Resolve connection catalogs asynchronously
        // This uses the async catalog providers to look up catalogs on-demand
        // without blocking. The resolve() method returns a sync CatalogProviderList
        // that has all needed catalogs pre-resolved.
        let resolved_catalog_list = self
            .unified_catalog_list
            .resolve(&references, session_state.config())
            .instrument(tracing::info_span!("resolve_catalogs"))
            .await
            .map_err(|e| {
                error!("Failed to resolve catalogs: {}", e);
                e
            })?;

        // Step 4: Clone context and register resolved catalogs
        // Each query gets an isolated context to avoid concurrent queries
        // interfering with each other's catalog registrations.
        let query_ctx = self.df_ctx.clone();
        query_ctx.register_catalog_list(resolved_catalog_list);
        let session_state = query_ctx.state();

        // Step 5: Plan and execute using the query-specific session state
        let plan = session_state
            .statement_to_plan(statement)
            .instrument(tracing::info_span!("statement_to_plan"))
            .await
            .map_err(|e| {
                error!("Planning error: {}", e);
                e
            })?;

        let df = DataFrame::new(session_state, plan);
        let schema: Arc<Schema> = Arc::clone(df.schema().inner());

        // Execute physical plan and collect results
        let results = async {
            let results = df.collect().await.map_err(|e| {
                error!("Error getting query result: {}", e);
                e
            })?;
            tracing::Span::current().record("runtimedb.batches_collected", results.len());
            Ok::<_, datafusion::error::DataFusionError>(results)
        }
        .instrument(tracing::info_span!(
            "collect_results",
            runtimedb.batches_collected = tracing::field::Empty
        ))
        .await?;

        info!("Execution completed in {:?}", start.elapsed());
        info!("Results available");

        let row_count: usize = results.iter().map(|b| b.num_rows()).sum();
        tracing::Span::current().record("runtimedb.rows_returned", row_count);

        Ok(QueryResponse {
            schema,
            execution_time: start.elapsed(),
            results,
        })
    }

    /// Execute a SQL query and persist results asynchronously.
    ///
    /// This method:
    /// 1. Executes the query
    /// 2. Generates a result ID
    /// 3. Inserts a catalog entry with status "processing"
    /// 4. Spawns a background task to write parquet and update status
    /// 5. Returns immediately with the result ID
    ///
    /// The result can be retrieved via GET /results/{id}. If status is "processing",
    /// the client should poll again later.
    #[tracing::instrument(
        name = "execute_query_with_persistence",
        skip(self, sql),
        fields(
            runtimedb.result_id = tracing::field::Empty,
        )
    )]
    pub async fn execute_query_with_persistence(
        &self,
        sql: &str,
    ) -> Result<(QueryResponseWithId, Option<String>)> {
        // Execute the query first
        let query_response = self.execute_query(sql).await?;

        // Create result in catalog - if this fails, return result with warning
        // Note: create_result generates the ID and records it in the span
        let result_id = match self.catalog.create_result(ResultStatus::Processing).await {
            Ok(id) => id,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "Failed to register result in catalog, returning without persistence"
                );
                // Return results with no persistence (empty result_id signals no persistence)
                return Ok((
                    QueryResponseWithId {
                        result_id: String::new(), // Empty signals no persistence available
                        schema: query_response.schema,
                        results: query_response.results,
                        execution_time: query_response.execution_time,
                    },
                    Some(format!("Result persistence unavailable: {}", e)),
                ));
            }
        };

        // Record result_id in span for traceability
        tracing::Span::current().record("runtimedb.result_id", &result_id);

        // Clone what we need for the background task
        let storage = Arc::clone(&self.storage);
        let catalog = Arc::clone(&self.catalog);
        let result_id_clone = result_id.clone();
        let schema = Arc::clone(&query_response.schema);
        let batches = query_response.results.clone();
        let semaphore = Arc::clone(&self.persistence_semaphore);

        // Acquire a persistence permit before spawning the background task.
        // This limits the number of concurrent persistence tasks to prevent unbounded
        // task spawning under burst load. If all permits are in use, this will block
        // until one becomes available. The "schedule_result_persistence" span makes
        // this wait time visible in traces - it should typically be near-zero, but
        // will show contention if persistence tasks are backing up.
        //
        // If the semaphore is closed (e.g., during shutdown), we gracefully degrade
        // by marking the result as failed rather than panicking.
        let permit = match semaphore
            .acquire_owned()
            .instrument(tracing::info_span!(
                "schedule_result_persistence",
                runtimedb.result_id = %result_id,
            ))
            .await
        {
            Ok(p) => p,
            Err(_) => {
                // Semaphore closed (shutdown in progress) - mark result as failed
                tracing::warn!(
                    result_id = %result_id,
                    "Persistence semaphore closed during shutdown, marking result as failed"
                );
                match self
                    .catalog
                    .update_result(
                        &result_id,
                        ResultUpdate::Failed {
                            error_message: Some("Server shutting down"),
                        },
                    )
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => {
                        tracing::warn!(
                            result_id = %result_id,
                            "Result row not found when marking failed during shutdown"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            result_id = %result_id,
                            error = %e,
                            "Failed to mark result as failed during shutdown"
                        );
                    }
                }
                return Ok((
                    QueryResponseWithId {
                        result_id: String::new(),
                        schema: query_response.schema,
                        results: query_response.results,
                        execution_time: query_response.execution_time,
                    },
                    Some("Result persistence unavailable: server shutting down".to_string()),
                ));
            }
        };

        // Spawn background persistence task via JoinSet, which automatically removes
        // completed tasks when polled, preventing unbounded memory growth.
        {
            let mut tasks = self.persistence_tasks.lock().await;
            // Poll JoinSet to clean up any completed tasks before spawning new one
            while tasks.try_join_next().is_some() {}
            tasks.spawn(async move {
                // Hold the permit for the duration of the persistence task
                let _permit = permit;

                let span = tracing::info_span!(
                    "persist_result_async",
                    runtimedb.result_id = %result_id_clone,
                );

                async {
                    if let Err(e) = persist_result_background(
                        &result_id_clone,
                        &schema,
                        &batches,
                        storage,
                        catalog,
                    )
                    .await
                    {
                        tracing::warn!(
                            result_id = %result_id_clone,
                            error = %e,
                            "Background result persistence failed"
                        );
                    }
                }
                .instrument(span)
                .await
            });
        }

        Ok((
            QueryResponseWithId {
                result_id,
                schema: query_response.schema,
                results: query_response.results,
                execution_time: query_response.execution_time,
            },
            None, // No warning, persistence initiated successfully
        ))
    }

    /// Get result metadata (status, etc.) without loading parquet data.
    pub async fn get_result_metadata(
        &self,
        id: &str,
    ) -> Result<Option<crate::catalog::QueryResult>> {
        self.catalog.get_result(id).await
    }

    /// Retrieve a persisted query result by ID.
    ///
    /// Returns the schema and record batches for the result, or None if not found.
    /// Returns None if the result is still processing or has failed.
    pub async fn get_result(&self, id: &str) -> Result<Option<(Arc<Schema>, Vec<RecordBatch>)>> {
        // Look up result in catalog
        let result = match self.catalog.get_result(id).await? {
            Some(r) => r,
            None => return Ok(None),
        };

        // Only load if result is ready (has parquet path)
        let parquet_path = match &result.parquet_path {
            Some(path) => path,
            None => return Ok(None), // Still processing or failed
        };

        // Load results from parquet. Handle the case where the file was deleted
        // between the catalog lookup and the file read (e.g., concurrent cleanup).
        let df = match self
            .df_ctx
            .read_parquet(
                parquet_path,
                datafusion::prelude::ParquetReadOptions::default(),
            )
            .await
        {
            Ok(df) => df,
            Err(e) => {
                // Check if this is a file-not-found error
                let err_str = e.to_string();
                if err_str.contains("No such file")
                    || err_str.contains("not found")
                    || err_str.contains("does not exist")
                {
                    tracing::warn!(
                        result_id = %id,
                        parquet_path = %parquet_path,
                        "Result parquet file not found, possibly deleted concurrently"
                    );
                    return Ok(None);
                }
                // Other errors should be propagated
                return Err(e.into());
            }
        };

        let schema: Arc<Schema> = Arc::clone(df.schema().inner());

        // collect() is when the actual file read happens - handle file-not-found errors
        let batches = match df.collect().await {
            Ok(b) => b,
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("No such file")
                    || err_str.contains("not found")
                    || err_str.contains("does not exist")
                    || err_str.contains("Object at location")
                {
                    tracing::warn!(
                        result_id = %id,
                        parquet_path = %parquet_path,
                        error = %e,
                        "Result parquet file not found during read"
                    );
                    return Ok(None);
                }
                return Err(e.into());
            }
        };

        // DataFusion may return empty results when the parquet file is missing (instead of an error).
        // Check if the result schema is unexpectedly empty, which indicates the file wasn't found.
        // A valid result should have at least one column in its schema.
        if schema.fields().is_empty() {
            tracing::warn!(
                result_id = %id,
                parquet_path = %parquet_path,
                "Result returned empty schema, parquet file may be missing or corrupted"
            );
            return Ok(None);
        }

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

    /// Mark a result as failed with the given error message.
    ///
    /// Used when the result data is no longer available (e.g., parquet file deleted).
    /// Returns true if the result was found and updated, false if not found.
    pub async fn mark_result_failed(&self, id: &str, error_message: &str) -> Result<bool> {
        self.catalog
            .update_result(
                id,
                ResultUpdate::Failed {
                    error_message: Some(error_message),
                },
            )
            .await
    }

    /// Purge all cached data for a connection (clears parquet files and resets sync state).
    #[tracing::instrument(
        name = "purge_connection",
        skip(self),
        fields(runtimedb.connection_id = %connection_id)
    )]
    pub async fn purge_connection(&self, connection_id: &str) -> Result<()> {
        // Get connection info (validates it exists)
        let _conn = self
            .catalog
            .get_connection(connection_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Connection '{}' not found", connection_id))?;

        // Step 1: Clear metadata first (metadata-first ordering)
        self.catalog
            .clear_connection_cache_metadata(connection_id)
            .await?;

        // Note: Connection catalogs are resolved on-demand during query execution.
        // The next query will resolve a fresh catalog without cached file references.

        // Step 2: Delete the physical files
        self.delete_connection_files(connection_id).await?;

        Ok(())
    }

    /// Purge cached data for a single table (clears parquet file and resets sync state).
    #[tracing::instrument(
        name = "purge_table",
        skip(self),
        fields(
            runtimedb.connection_id = %connection_id,
            runtimedb.schema = %schema_name,
            runtimedb.table = %table_name,
        )
    )]
    pub async fn purge_table(
        &self,
        connection_id: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<()> {
        // Validate connection exists
        let _conn = self
            .catalog
            .get_connection(connection_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Connection '{}' not found", connection_id))?;

        // Step 1: Clear metadata (set paths to NULL in DB)
        // This returns the table info with the old paths before clearing
        let table_info = self
            .catalog
            .clear_table_cache_metadata(connection_id, schema_name, table_name)
            .await?;

        // Note: Connection catalogs are resolved on-demand during query execution.
        // The next query will resolve a fresh catalog without cached file references.

        // Step 2: Delete the physical files
        self.delete_table_files(&table_info).await?;

        Ok(())
    }

    /// Remove a connection entirely (removes from catalog and deletes all data).
    ///
    /// If the connection has an associated secret that is not used by any other
    /// connections, the secret will also be deleted.
    #[tracing::instrument(
        name = "remove_connection",
        skip(self),
        fields(runtimedb.connection_id = %connection_id)
    )]
    pub async fn remove_connection(&self, connection_id: &str) -> Result<()> {
        // Get connection info (validates it exists)
        let conn = self
            .catalog
            .get_connection(connection_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Connection '{}' not found", connection_id))?;

        // Capture secret_id before deleting the connection
        let secret_id = conn.secret_id.clone();

        // Step 1: Delete metadata first
        self.catalog.delete_connection(connection_id).await?;

        // Step 2: Delete the physical files
        self.delete_connection_files(connection_id).await?;

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
            .get_connection(connection_id)
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
        // Signal background workers to stop
        self.shutdown_token.cancel();

        // Wait for the deletion worker to finish
        if let Some(handle) = self.deletion_worker_handle.lock().await.take() {
            // We use a timeout to avoid blocking forever if the worker is stuck
            let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
        }

        // Wait for the stale result cleanup worker to finish
        if let Some(handle) = self.stale_result_cleanup_handle.lock().await.take() {
            let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
        }

        // Wait for in-flight persistence tasks to complete
        let mut tasks = self.persistence_tasks.lock().await;
        let task_count = tasks.len();
        if task_count > 0 {
            tracing::info!(
                count = task_count,
                "Waiting for in-flight persistence tasks to complete"
            );
            // Use a timeout to avoid blocking forever
            let join_future = async {
                while let Some(result) = tasks.join_next().await {
                    if let Err(e) = result {
                        tracing::warn!(error = %e, "Persistence task failed during shutdown");
                    }
                }
            };
            if tokio::time::timeout(Duration::from_secs(30), join_future)
                .await
                .is_err()
            {
                tracing::warn!("Timeout waiting for persistence tasks during shutdown");
                // Abort remaining tasks
                tasks.abort_all();
            }
        }
        drop(tasks); // Release the lock before closing catalog

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
    #[tracing::instrument(
        name = "refresh_schema",
        skip(self),
        fields(
            runtimedb.connection_id = %connection_id,
            runtimedb.tables_added = tracing::field::Empty,
            runtimedb.tables_modified = tracing::field::Empty,
        )
    )]
    pub async fn refresh_schema(&self, connection_id: &str) -> Result<(usize, usize)> {
        let conn = self
            .catalog
            .get_connection(connection_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Connection '{}' not found", connection_id))?;

        let existing_tables = self.catalog.list_tables(Some(connection_id)).await?;
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
                .add_table(
                    connection_id,
                    &table.schema_name,
                    &table.table_name,
                    &schema_json,
                )
                .await?;
        }

        tracing::Span::current()
            .record("runtimedb.tables_added", added_count)
            .record("runtimedb.tables_modified", modified);
        Ok((added_count, modified))
    }

    /// Refresh schema for all connections.
    /// Continues processing remaining connections if one fails.
    #[tracing::instrument(
        name = "refresh_all_schemas",
        skip(self),
        fields(
            runtimedb.connections_refreshed = tracing::field::Empty,
            runtimedb.connections_failed = tracing::field::Empty,
        )
    )]
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
            match self.refresh_schema(&conn.id).await {
                Ok((added, modified)) => {
                    result.connections_refreshed += 1;
                    result.tables_added += added;
                    result.tables_modified += modified;
                }
                Err(e) => {
                    tracing::warn!(
                        connection_id = %conn.id,
                        error = %e,
                        "Failed to refresh schema for connection"
                    );
                    result.connections_failed += 1;
                    result.errors.push(ConnectionSchemaError {
                        connection_id: conn.id.clone(),
                        error: e.to_string(),
                    });
                }
            }
        }

        result.tables_discovered = self.catalog.list_tables(None).await?.len();
        tracing::Span::current()
            .record(
                "runtimedb.connections_refreshed",
                result.connections_refreshed,
            )
            .record("runtimedb.connections_failed", result.connections_failed);
        Ok(result)
    }

    /// Refresh data for a single table using atomic swap.
    #[tracing::instrument(
        name = "refresh_table",
        skip(self),
        fields(
            runtimedb.connection_id = %connection_id,
            runtimedb.schema = %schema_name,
            runtimedb.table = %table_name,
            runtimedb.rows_synced = tracing::field::Empty,
            runtimedb.warnings_count = tracing::field::Empty,
        )
    )]
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
            .get_connection(connection_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Connection '{}' not found", connection_id))?;
        let source: Source = serde_json::from_str(&conn.config_json)?;

        let (_, old_path, rows_synced) = self
            .orchestrator
            .refresh_table(&source, connection_id, schema_name, table_name)
            .await?;

        if let Some(path) = old_path {
            if let Some(cache) = &self.parquet_cache {
                cache.cache().invalidate_url(&path);
            }
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

        tracing::Span::current()
            .record("runtimedb.rows_synced", rows_synced)
            .record("runtimedb.warnings_count", warnings.len());

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
    #[tracing::instrument(
        name = "refresh_connection",
        skip(self),
        fields(
            runtimedb.connection_id = %connection_id,
            runtimedb.parallelism = self.parallel_refresh_count,
            runtimedb.tables_refreshed = tracing::field::Empty,
            runtimedb.tables_failed = tracing::field::Empty,
            runtimedb.rows_synced = tracing::field::Empty,
        )
    )]
    pub async fn refresh_connection_data(
        &self,
        connection_id: &str,
        include_uncached: bool,
    ) -> Result<ConnectionRefreshResult> {
        let start = std::time::Instant::now();

        let conn = self
            .catalog
            .get_connection(connection_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Connection '{}' not found", connection_id))?;

        let all_tables = self.catalog.list_tables(Some(connection_id)).await?;

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

        // Clone connection_id for use in spawned tasks
        let conn_id = connection_id.to_string();

        for table in tables {
            let permit = semaphore.clone().acquire_owned().await?;
            let orchestrator = self.orchestrator.clone();
            let source = source.clone();
            let schema_name = table.schema_name.clone();
            let table_name = table.table_name.clone();
            let conn_id = conn_id.clone();

            let handle = tokio::spawn(async move {
                let result = orchestrator
                    .refresh_table(&source, &conn_id, &schema_name, &table_name)
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
                        if let Some(cache) = &self.parquet_cache {
                            cache.cache().invalidate_url(&path);
                        }
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

        tracing::Span::current()
            .record("runtimedb.tables_refreshed", result.tables_refreshed)
            .record("runtimedb.tables_failed", result.tables_failed)
            .record("runtimedb.rows_synced", result.total_rows);

        Ok(result)
    }

    /// Store an uploaded file and create an upload record.
    pub async fn store_upload(
        &self,
        data: Vec<u8>,
        content_type: Option<String>,
    ) -> Result<crate::catalog::UploadInfo> {
        let upload_id = crate::id::generate_upload_id();
        let size_bytes = data.len() as i64;

        // Write to storage
        let local_path = self.storage.prepare_upload_write(&upload_id);
        if let Some(parent) = local_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&local_path, &data).await?;
        let storage_url = self.storage.finalize_upload_write(&upload_id).await?;

        // Create catalog record
        let now = Utc::now();
        let upload = crate::catalog::UploadInfo {
            id: upload_id,
            status: crate::datasets::upload_status::PENDING.to_string(),
            storage_url,
            content_type,
            content_encoding: None,
            size_bytes,
            created_at: now,
            consumed_at: None,
        };
        self.catalog.create_upload(&upload).await?;

        Ok(upload)
    }

    /// Create a dataset from an upload or inline data.
    #[tracing::instrument(
        name = "create_dataset",
        skip(self, source),
        fields(
            runtimedb.dataset_id = tracing::field::Empty,
            runtimedb.table_name = tracing::field::Empty,
            runtimedb.source_type = tracing::field::Empty,
        )
    )]
    pub async fn create_dataset(
        &self,
        label: &str,
        table_name: Option<&str>,
        source: crate::http::models::DatasetSource,
    ) -> Result<crate::catalog::DatasetInfo, crate::datasets::DatasetError> {
        use crate::datafetch::native::StreamingParquetWriter;
        use crate::datasets::DatasetError;
        use crate::http::models::DatasetSource;
        use datafusion::arrow::csv::{reader::Format, ReaderBuilder};
        use std::fs::File;
        use std::io::BufReader;

        // Validate label is not empty
        if label.trim().is_empty() {
            return Err(DatasetError::InvalidLabel);
        }

        // Generate or validate table_name
        let table_name = match table_name {
            Some(name) => {
                crate::datasets::validate_table_name(name)
                    .map_err(DatasetError::InvalidTableName)?;
                name.to_string()
            }
            None => {
                let generated = crate::datasets::table_name_from_label(label);
                // Validate the auto-generated name (labels like "select" can produce invalid names)
                crate::datasets::validate_table_name(&generated).map_err(|e| {
                    DatasetError::InvalidGeneratedTableName {
                        label: label.to_string(),
                        error: e,
                    }
                })?;
                generated
            }
        };

        // Check if table_name is already used
        if self
            .catalog
            .get_dataset_by_table_name(crate::datasets::DEFAULT_SCHEMA, &table_name)
            .await
            .map_err(DatasetError::Catalog)?
            .is_some()
        {
            return Err(DatasetError::TableNameInUse(table_name));
        }

        // Track claimed upload ID for rollback on failure
        let mut claimed_upload_id: Option<String> = None;
        // Track temp file path for cleanup (used when downloading from S3)
        let mut temp_file_path: Option<std::path::PathBuf> = None;

        // Helper to cleanup temp file
        let cleanup_temp = |path: &Option<std::path::PathBuf>| {
            if let Some(ref p) = path {
                let _ = std::fs::remove_file(p);
            }
        };

        // Data source for streaming: either a file path (for uploads) or in-memory data (for inline)
        enum DataSource {
            FilePath(std::path::PathBuf),
            InMemory(Vec<u8>),
        }

        // Get format, data source, and optional columns based on input
        // For uploads, we use streaming from local file path to avoid loading entire file into memory
        // For inline, we keep the data in memory (typically small)
        let (data_source, format, source_type, source_config, explicit_columns) = match &source {
            DatasetSource::Upload {
                upload_id,
                format,
                columns,
            } => {
                // Atomically claim the upload to prevent concurrent dataset creation
                // This must happen before reading data to avoid race conditions
                let claimed = self
                    .catalog
                    .claim_upload(upload_id)
                    .await
                    .map_err(DatasetError::Catalog)?;
                if !claimed {
                    // Either upload doesn't exist, was already claimed, or consumed
                    let upload = self
                        .catalog
                        .get_upload(upload_id)
                        .await
                        .map_err(DatasetError::Catalog)?;
                    use crate::datasets::upload_status;
                    return Err(match upload {
                        None => DatasetError::UploadNotFound(upload_id.clone()),
                        Some(u) if u.status == upload_status::CONSUMED => {
                            DatasetError::UploadConsumed(upload_id.clone())
                        }
                        Some(u) if u.status == upload_status::PROCESSING => {
                            DatasetError::UploadProcessing(upload_id.clone())
                        }
                        Some(_) => DatasetError::UploadNotAvailable(upload_id.clone()),
                    });
                }

                // Track that we claimed this upload for potential rollback
                claimed_upload_id = Some(upload_id.clone());

                // Get upload info for format detection (we know it exists since claim succeeded)
                let upload = self
                    .catalog
                    .get_upload(upload_id)
                    .await
                    .map_err(DatasetError::Catalog)?
                    .expect("Upload should exist after successful claim");

                // Get local path for streaming read (downloads from S3 if needed)
                let upload_url = self.storage.upload_url(upload_id);
                let (local_path, is_temp) = match self.storage.get_local_path(&upload_url).await {
                    Ok(result) => result,
                    Err(e) => {
                        // Release the upload back to pending state
                        let _ = self.catalog.release_upload(upload_id).await;
                        return Err(DatasetError::Storage(e));
                    }
                };

                // Track temp file for cleanup (only for S3 downloads)
                if is_temp {
                    temp_file_path = Some(local_path.clone());
                }

                // Determine format
                let format = format
                    .clone()
                    .or_else(|| {
                        upload.content_type.as_ref().and_then(|ct| {
                            if ct.contains("csv") {
                                Some("csv".to_string())
                            } else if ct.contains("json") {
                                Some("json".to_string())
                            } else if ct.contains("parquet") {
                                Some("parquet".to_string())
                            } else {
                                None
                            }
                        })
                    })
                    .unwrap_or_else(|| "csv".to_string());

                let source_config = serde_json::json!({
                    "upload_id": upload_id,
                    "format": format
                })
                .to_string();

                (
                    DataSource::FilePath(local_path),
                    format,
                    "upload",
                    source_config,
                    columns.clone(),
                )
            }
            DatasetSource::Inline { inline } => {
                let source_config = serde_json::json!({
                    "format": inline.format
                })
                .to_string();
                (
                    DataSource::InMemory(inline.content.as_bytes().to_vec()),
                    inline.format.clone(),
                    "inline",
                    source_config,
                    inline.columns.clone(),
                )
            }
        };

        // Normalize format to lowercase for case-insensitive matching
        let format = format.to_lowercase();

        // Prepare the parquet writer first so we can stream directly to it
        let dataset_id = crate::id::generate_dataset_id();
        let handle = self.storage.prepare_dataset_write(&dataset_id);

        // Helper to cleanup partial parquet files on error
        let cleanup_parquet = |handle: &crate::storage::DatasetWriteHandle| {
            if let Some(parent) = handle.local_path.parent() {
                let _ = std::fs::remove_dir_all(parent);
            }
        };

        // Helper macro to release upload, cleanup temp/parquet, and return ParseError
        macro_rules! release_on_parse_error {
            ($result:expr) => {
                match $result {
                    Ok(v) => v,
                    Err(e) => {
                        if let Some(ref upload_id) = claimed_upload_id {
                            let _ = self.catalog.release_upload(upload_id).await;
                        }
                        cleanup_temp(&temp_file_path);
                        cleanup_parquet(&handle);
                        return Err(DatasetError::ParseError(e.into()));
                    }
                }
            };
        }

        // Helper macro to release upload, cleanup temp/parquet, and return storage error
        macro_rules! release_on_storage_error {
            ($result:expr) => {
                match $result {
                    Ok(v) => v,
                    Err(e) => {
                        if let Some(ref upload_id) = claimed_upload_id {
                            let _ = self.catalog.release_upload(upload_id).await;
                        }
                        cleanup_temp(&temp_file_path);
                        cleanup_parquet(&handle);
                        return Err(DatasetError::Storage(e.into()));
                    }
                }
            };
        }

        // Helper macro to release upload, cleanup temp/parquet, and return schema error
        macro_rules! release_on_schema_error {
            ($result:expr) => {
                match $result {
                    Ok(v) => v,
                    Err(e) => {
                        if let Some(ref upload_id) = claimed_upload_id {
                            let _ = self.catalog.release_upload(upload_id).await;
                        }
                        cleanup_temp(&temp_file_path);
                        cleanup_parquet(&handle);
                        return Err(DatasetError::InvalidSchema(e));
                    }
                }
            };
        }

        // Process data in streaming fashion based on format
        // For file paths, we read from disk; for inline, from memory
        // We stream batches directly to the parquet writer to minimize memory usage
        let (schema, row_count) = match format.as_str() {
            "csv" => {
                match data_source {
                    DataSource::FilePath(path) => {
                        // Determine schema: use explicit columns if provided, otherwise infer
                        let schema = if let Some(ref cols) = explicit_columns {
                            // Read header to get column names from data
                            let file = release_on_parse_error!(File::open(&path));
                            let mut reader = BufReader::new(file);
                            let (inferred, _) = release_on_parse_error!(Format::default()
                                .with_header(true)
                                .infer_schema(&mut reader, Some(1)));
                            let data_columns: Vec<String> =
                                inferred.fields().iter().map(|f| f.name().clone()).collect();
                            // Build schema from explicit column definitions
                            release_on_schema_error!(crate::datasets::build_schema_from_columns(
                                cols,
                                &data_columns
                            ))
                        } else {
                            // Infer schema from file (reads first 10000 rows)
                            let file = release_on_parse_error!(File::open(&path));
                            let mut reader = BufReader::new(file);
                            let (schema, _) = release_on_parse_error!(Format::default()
                                .with_header(true)
                                .infer_schema(&mut reader, Some(10_000)));
                            Arc::new(schema)
                        };

                        // Reopen file for streaming read
                        let file = release_on_parse_error!(File::open(&path));
                        let csv_reader =
                            release_on_parse_error!(ReaderBuilder::new(schema.clone())
                                .with_header(true)
                                .with_batch_size(8192)
                                .build(BufReader::new(file)));

                        // Initialize writer with schema
                        let mut writer: Box<dyn crate::datafetch::BatchWriter> =
                            Box::new(StreamingParquetWriter::new(handle.local_path.clone()));
                        release_on_storage_error!(writer.init(&schema));

                        // Stream batches directly to writer
                        let mut row_count = 0usize;
                        for batch_result in csv_reader {
                            let batch = release_on_parse_error!(batch_result);
                            row_count += batch.num_rows();
                            release_on_storage_error!(writer.write_batch(&batch));
                        }

                        release_on_storage_error!(writer.close());
                        (schema, row_count)
                    }
                    DataSource::InMemory(data) => {
                        // Determine schema: use explicit columns if provided, otherwise infer
                        let schema = if let Some(ref cols) = explicit_columns {
                            // Read header to get column names from data
                            let mut cursor = std::io::Cursor::new(&data);
                            let (inferred, _) = release_on_parse_error!(Format::default()
                                .with_header(true)
                                .infer_schema(&mut cursor, Some(1)));
                            let data_columns: Vec<String> =
                                inferred.fields().iter().map(|f| f.name().clone()).collect();
                            // Build schema from explicit column definitions
                            release_on_schema_error!(crate::datasets::build_schema_from_columns(
                                cols,
                                &data_columns
                            ))
                        } else {
                            // Infer schema (reads first 10000 rows)
                            let mut cursor = std::io::Cursor::new(&data);
                            let (schema, _) = release_on_parse_error!(Format::default()
                                .with_header(true)
                                .infer_schema(&mut cursor, Some(10_000)));
                            Arc::new(schema)
                        };

                        // Reset cursor for reading
                        let cursor = std::io::Cursor::new(&data);
                        let csv_reader =
                            release_on_parse_error!(ReaderBuilder::new(schema.clone())
                                .with_header(true)
                                .with_batch_size(8192)
                                .build(cursor));

                        let mut writer: Box<dyn crate::datafetch::BatchWriter> =
                            Box::new(StreamingParquetWriter::new(handle.local_path.clone()));
                        release_on_storage_error!(writer.init(&schema));

                        let mut row_count = 0usize;
                        for batch_result in csv_reader {
                            let batch = release_on_parse_error!(batch_result);
                            row_count += batch.num_rows();
                            release_on_storage_error!(writer.write_batch(&batch));
                        }

                        release_on_storage_error!(writer.close());
                        (schema, row_count)
                    }
                }
            }
            "json" => {
                use arrow_json::reader::infer_json_schema_from_seekable;

                match data_source {
                    DataSource::FilePath(path) => {
                        // Always infer schema first to get observed field names
                        let file = release_on_parse_error!(File::open(&path));
                        let mut reader = BufReader::new(file);
                        let (inferred_schema, _) = release_on_parse_error!(
                            infer_json_schema_from_seekable(&mut reader, Some(10_000))
                        );

                        // Determine final schema: use explicit columns validated against observed fields,
                        // or use inferred schema if no explicit columns provided
                        let schema = if let Some(ref cols) = explicit_columns {
                            // Get observed field names from inferred schema
                            let observed_fields: Vec<String> = inferred_schema
                                .fields()
                                .iter()
                                .map(|f| f.name().clone())
                                .collect();
                            release_on_schema_error!(
                                crate::datasets::build_schema_from_columns_for_json(
                                    cols,
                                    &observed_fields
                                )
                            )
                        } else {
                            Arc::new(inferred_schema)
                        };

                        // Open file for streaming read
                        let file = release_on_parse_error!(File::open(&path));
                        let json_reader =
                            release_on_parse_error!(arrow_json::ReaderBuilder::new(schema.clone())
                                .with_batch_size(8192)
                                .build(BufReader::new(file)));

                        let mut writer: Box<dyn crate::datafetch::BatchWriter> =
                            Box::new(StreamingParquetWriter::new(handle.local_path.clone()));
                        release_on_storage_error!(writer.init(&schema));

                        let mut row_count = 0usize;
                        for batch_result in json_reader {
                            let batch = release_on_parse_error!(batch_result);
                            row_count += batch.num_rows();
                            release_on_storage_error!(writer.write_batch(&batch));
                        }

                        release_on_storage_error!(writer.close());
                        (schema, row_count)
                    }
                    DataSource::InMemory(data) => {
                        // Always infer schema first to get observed field names
                        let mut cursor = std::io::Cursor::new(&data);
                        let (inferred_schema, _) = release_on_parse_error!(
                            infer_json_schema_from_seekable(&mut cursor, Some(10_000))
                        );

                        // Determine final schema: use explicit columns validated against observed fields,
                        // or use inferred schema if no explicit columns provided
                        let schema = if let Some(ref cols) = explicit_columns {
                            // Get observed field names from inferred schema
                            let observed_fields: Vec<String> = inferred_schema
                                .fields()
                                .iter()
                                .map(|f| f.name().clone())
                                .collect();
                            release_on_schema_error!(
                                crate::datasets::build_schema_from_columns_for_json(
                                    cols,
                                    &observed_fields
                                )
                            )
                        } else {
                            Arc::new(inferred_schema)
                        };

                        // Open cursor for reading
                        let cursor = std::io::Cursor::new(&data);
                        let json_reader =
                            release_on_parse_error!(arrow_json::ReaderBuilder::new(schema.clone())
                                .with_batch_size(8192)
                                .build(cursor));

                        let mut writer: Box<dyn crate::datafetch::BatchWriter> =
                            Box::new(StreamingParquetWriter::new(handle.local_path.clone()));
                        release_on_storage_error!(writer.init(&schema));

                        let mut row_count = 0usize;
                        for batch_result in json_reader {
                            let batch = release_on_parse_error!(batch_result);
                            row_count += batch.num_rows();
                            release_on_storage_error!(writer.write_batch(&batch));
                        }

                        release_on_storage_error!(writer.close());
                        (schema, row_count)
                    }
                }
            }
            "parquet" => {
                use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

                // Log warning if columns were provided - parquet has embedded schema
                if explicit_columns.is_some() {
                    warn!("Ignoring explicit columns for parquet format - parquet files have embedded schema");
                }

                match data_source {
                    DataSource::FilePath(path) => {
                        // Read parquet from file - streaming batch by batch
                        let file = release_on_parse_error!(File::open(&path));
                        let builder =
                            release_on_parse_error!(ParquetRecordBatchReaderBuilder::try_new(file));
                        let schema = builder.schema().clone();
                        let parquet_reader =
                            release_on_parse_error!(builder.with_batch_size(8192).build());

                        let mut writer: Box<dyn crate::datafetch::BatchWriter> =
                            Box::new(StreamingParquetWriter::new(handle.local_path.clone()));
                        release_on_storage_error!(writer.init(&schema));

                        let mut row_count = 0usize;
                        for batch_result in parquet_reader {
                            let batch = release_on_parse_error!(batch_result);
                            row_count += batch.num_rows();
                            release_on_storage_error!(writer.write_batch(&batch));
                        }

                        release_on_storage_error!(writer.close());
                        (schema, row_count)
                    }
                    DataSource::InMemory(data) => {
                        // For inline parquet (unusual but supported)
                        let cursor = axum::body::Bytes::from(data);
                        let builder = release_on_parse_error!(
                            ParquetRecordBatchReaderBuilder::try_new(cursor)
                        );
                        let schema = builder.schema().clone();
                        let parquet_reader =
                            release_on_parse_error!(builder.with_batch_size(8192).build());

                        let mut writer: Box<dyn crate::datafetch::BatchWriter> =
                            Box::new(StreamingParquetWriter::new(handle.local_path.clone()));
                        release_on_storage_error!(writer.init(&schema));

                        let mut row_count = 0usize;
                        for batch_result in parquet_reader {
                            let batch = release_on_parse_error!(batch_result);
                            row_count += batch.num_rows();
                            release_on_storage_error!(writer.write_batch(&batch));
                        }

                        release_on_storage_error!(writer.close());
                        (schema, row_count)
                    }
                }
            }
            _ => {
                if let Some(ref upload_id) = claimed_upload_id {
                    let _ = self.catalog.release_upload(upload_id).await;
                }
                cleanup_temp(&temp_file_path);
                cleanup_parquet(&handle);
                return Err(DatasetError::UnsupportedFormat(format));
            }
        };

        // Cleanup temp file now that we're done reading
        cleanup_temp(&temp_file_path);

        if row_count == 0 {
            cleanup_parquet(&handle);
            if let Some(ref upload_id) = claimed_upload_id {
                let _ = self.catalog.release_upload(upload_id).await;
            }
            return Err(DatasetError::EmptyData);
        }

        let schema_json = release_on_parse_error!(serde_json::to_string(schema.as_ref()));

        // Finalize storage (upload to S3 if needed)
        // Note: finalize_dataset_write returns the full file URL including data.parquet
        let parquet_url =
            release_on_storage_error!(self.storage.finalize_dataset_write(&handle).await);

        // From this point on, if we fail we need to clean up the parquet file too
        // Extract the directory URL from the file URL (remove /data.parquet)
        let parquet_dir_url = parquet_url
            .strip_suffix("/data.parquet")
            .unwrap_or(&parquet_url)
            .to_string();

        // Create catalog record
        let now = Utc::now();
        let table_name_for_error = table_name.clone();
        let dataset = crate::catalog::DatasetInfo {
            id: dataset_id,
            label: label.to_string(),
            schema_name: crate::datasets::DEFAULT_SCHEMA.to_string(),
            table_name,
            parquet_url,
            arrow_schema_json: schema_json,
            source_type: source_type.to_string(),
            source_config,
            created_at: now,
            updated_at: now,
        };

        if let Err(e) = self.catalog.create_dataset(&dataset).await {
            // Clean up the parquet file we wrote
            if let Err(cleanup_err) = self.storage.delete_prefix(&parquet_dir_url).await {
                tracing::warn!(
                    dataset_id = %dataset.id,
                    path = %parquet_dir_url,
                    error = %cleanup_err,
                    "Failed to clean up orphaned parquet directory after catalog insert failure"
                );
            }
            // Release the upload back to pending state
            if let Some(ref upload_id) = claimed_upload_id {
                let _ = self.catalog.release_upload(upload_id).await;
            }
            // Check if this is a unique constraint violation on table_name (race condition)
            if is_dataset_table_name_conflict(
                self.catalog.as_ref(),
                &e,
                crate::datasets::DEFAULT_SCHEMA,
                &table_name_for_error,
                None, // creating new dataset
            )
            .await
            {
                return Err(DatasetError::TableNameInUse(table_name_for_error));
            }
            return Err(DatasetError::Catalog(e));
        }

        // Only consume the upload after everything succeeded
        if let Some(ref upload_id) = claimed_upload_id {
            if let Err(e) = self.catalog.consume_upload(upload_id).await {
                // Dataset was created successfully, but we couldn't mark upload as consumed.
                // This is not fatal - the upload will remain in "processing" state but the
                // dataset is valid. Log a warning but don't fail.
                tracing::warn!(
                    upload_id = %upload_id,
                    error = %e,
                    "Failed to mark upload as consumed after successful dataset creation"
                );
            }
        }

        tracing::Span::current()
            .record("runtimedb.dataset_id", &dataset.id)
            .record("runtimedb.table_name", &dataset.table_name)
            .record("runtimedb.source_type", &dataset.source_type);

        Ok(dataset)
    }

    /// Process any pending directory deletions that are due.
    #[tracing::instrument(
        name = "process_pending_deletions",
        skip(self),
        fields(runtimedb.deletions_processed = tracing::field::Empty)
    )]
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

        tracing::Span::current().record("runtimedb.deletions_processed", deleted);
        Ok(deleted)
    }

    /// Schedule deletion of a dataset's parquet file after the grace period.
    /// Get a dataset by ID.
    #[tracing::instrument(
        name = "get_dataset",
        skip(self),
        fields(runtimedb.dataset_id = %id)
    )]
    pub async fn get_dataset(
        &self,
        id: &str,
    ) -> Result<crate::catalog::DatasetInfo, crate::datasets::DatasetError> {
        use crate::datasets::DatasetError;

        self.catalog
            .get_dataset(id)
            .await
            .map_err(DatasetError::Catalog)?
            .ok_or_else(|| DatasetError::NotFound(id.to_string()))
    }

    /// Delete a dataset by ID.
    /// Handles cache invalidation and schedules parquet file cleanup.
    #[tracing::instrument(
        name = "delete_dataset",
        skip(self),
        fields(runtimedb.dataset_id = %id)
    )]
    pub async fn delete_dataset(&self, id: &str) -> Result<(), crate::datasets::DatasetError> {
        use crate::datasets::DatasetError;

        let deleted = self
            .catalog
            .delete_dataset(id)
            .await
            .map_err(DatasetError::Catalog)?
            .ok_or_else(|| DatasetError::NotFound(id.to_string()))?;

        if let Some(cache) = &self.parquet_cache {
            cache.cache().invalidate_url(&deleted.parquet_url);
        }

        // Schedule parquet directory deletion after grace period
        // parquet_url is the full file path (e.g., .../version/data.parquet)
        // but delete_prefix expects the directory, so strip the filename
        let dir_url = deleted
            .parquet_url
            .strip_suffix("/data.parquet")
            .unwrap_or(&deleted.parquet_url);

        if let Err(e) = self.schedule_file_deletion(dir_url).await {
            tracing::warn!(
                dataset_id = %id,
                dir_url = %dir_url,
                error = %e,
                "Failed to schedule parquet directory deletion"
            );
        }

        Ok(())
    }

    /// Update a dataset's label and/or table name.
    #[tracing::instrument(
        name = "update_dataset",
        skip(self),
        fields(runtimedb.dataset_id = %id)
    )]
    pub async fn update_dataset(
        &self,
        id: &str,
        new_label: Option<&str>,
        new_table_name: Option<&str>,
    ) -> Result<crate::catalog::DatasetInfo, crate::datasets::DatasetError> {
        use crate::datasets::DatasetError;

        // Get existing dataset
        let existing = self
            .catalog
            .get_dataset(id)
            .await
            .map_err(DatasetError::Catalog)?
            .ok_or_else(|| DatasetError::NotFound(id.to_string()))?;

        // Use existing values if not provided
        let old_table_name = existing.table_name.clone();
        let label = new_label.unwrap_or(&existing.label);
        let table_name = new_table_name.unwrap_or(&existing.table_name);

        // Validate label is not empty
        if label.trim().is_empty() {
            return Err(DatasetError::InvalidLabel);
        }

        // Validate table_name
        crate::datasets::validate_table_name(table_name).map_err(DatasetError::InvalidTableName)?;

        // Check table_name uniqueness if it's changing
        if table_name != old_table_name {
            if let Some(existing_dataset) = self
                .catalog
                .get_dataset_by_table_name(crate::datasets::DEFAULT_SCHEMA, table_name)
                .await
                .map_err(DatasetError::Catalog)?
            {
                // Another dataset already uses this table_name
                if existing_dataset.id != id {
                    return Err(DatasetError::TableNameInUse(table_name.to_string()));
                }
            }
        }

        // Update in catalog
        let updated = match self.catalog.update_dataset(id, label, table_name).await {
            Ok(updated) => updated,
            Err(e) => {
                // Check if this is a unique constraint violation on table_name (race condition)
                if is_dataset_table_name_conflict(
                    self.catalog.as_ref(),
                    &e,
                    crate::datasets::DEFAULT_SCHEMA,
                    table_name,
                    Some(id), // exclude current dataset from conflict check
                )
                .await
                {
                    return Err(DatasetError::TableNameInUse(table_name.to_string()));
                }
                return Err(DatasetError::Catalog(e));
            }
        };

        if !updated {
            return Err(DatasetError::NotFound(id.to_string()));
        }

        // Fetch and return updated dataset
        self.catalog
            .get_dataset(id)
            .await
            .map_err(DatasetError::Catalog)?
            .ok_or_else(|| DatasetError::NotFound(id.to_string()))
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

    /// Start background task that cleans up stale results (stuck in pending/processing) periodically.
    /// Returns the JoinHandle for the spawned task, or None if cleanup is disabled (interval is zero).
    fn start_stale_result_cleanup_worker(
        catalog: Arc<dyn CatalogManager>,
        shutdown_token: CancellationToken,
        cleanup_interval: Duration,
        stale_timeout: Duration,
    ) -> Option<tokio::task::JoinHandle<()>> {
        // Disable cleanup if interval is zero
        if cleanup_interval.is_zero() {
            info!("Stale result cleanup worker disabled (interval is zero)");
            return None;
        }

        Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(cleanup_interval);
            loop {
                tokio::select! {
                    _ = shutdown_token.cancelled() => {
                        info!("Stale result cleanup worker received shutdown signal");
                        break;
                    }
                    _ = interval.tick() => {
                        let cutoff = Utc::now() - chrono::Duration::from_std(stale_timeout).unwrap_or(chrono::Duration::seconds(300));
                        match catalog.cleanup_stale_results(cutoff).await {
                            Ok(0) => {
                                // No stale results - this is the normal case, don't log
                            }
                            Ok(count) => {
                                info!(
                                    count = count,
                                    cutoff = %cutoff,
                                    "Cleaned up stale results"
                                );
                            }
                            Err(e) => {
                                warn!(error = %e, "Failed to cleanup stale results");
                            }
                        }
                    }
                }
            }
        }))
    }
}

impl Drop for RuntimeEngine {
    fn drop(&mut self) {
        // Signal workers to stop (deletion worker, stale result cleanup worker)
        self.shutdown_token.cancel();

        // Note: Call engine.shutdown().await explicitly before dropping for proper cleanup.
        // The catalog connection will be cleaned up when its Arc is dropped.
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
    max_concurrent_persistence: usize,
    liquid_cache_server: Option<String>,
    cache_config: Option<crate::config::CacheConfig>,
    stale_result_cleanup_interval: Duration,
    stale_result_timeout: Duration,
    in_memory_cache_total_max_bytes: u64,
    in_memory_cache_table_max_bytes: u64,
    in_memory_cache_ttl: Duration,
    in_memory_cache_max_concurrent_loads: usize,
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
            max_concurrent_persistence: DEFAULT_MAX_CONCURRENT_PERSISTENCE,
            liquid_cache_server: None,
            cache_config: None,
            stale_result_cleanup_interval: Duration::from_secs(
                DEFAULT_STALE_RESULT_CLEANUP_INTERVAL_SECS,
            ),
            stale_result_timeout: Duration::from_secs(DEFAULT_STALE_RESULT_TIMEOUT_SECS),
            in_memory_cache_total_max_bytes: 4 * 1024 * 1024 * 1024,
            in_memory_cache_table_max_bytes: 1024 * 1024 * 1024,
            in_memory_cache_ttl: Duration::from_secs(0),
            in_memory_cache_max_concurrent_loads: 10,
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

    /// Set the maximum number of concurrent result persistence tasks.
    /// When a query is executed with persistence, a background task is spawned
    /// to write the results to parquet. This semaphore limits how many of these
    /// tasks can run concurrently. If all permits are in use, the query will
    /// block until a permit becomes available. Defaults to 32.
    pub fn max_concurrent_persistence(mut self, count: usize) -> Self {
        self.max_concurrent_persistence = count.max(1);
        self
    }

    /// Configure liquid cache for distributed query caching.
    /// When enabled, queries are offloaded to a liquid-cache server for caching.
    pub fn liquid_cache_server(mut self, server_address: String) -> Self {
        self.liquid_cache_server = Some(server_address);
        self
    }

    /// Set the cache configuration for Redis metadata caching.
    pub fn cache_config(mut self, config: crate::config::CacheConfig) -> Self {
        self.cache_config = Some(config);
        self
    }

    /// Set the interval for stale result cleanup.
    /// Results stuck in pending/processing longer than the timeout are marked as failed.
    /// Set to Duration::ZERO to disable cleanup. Defaults to 60 seconds.
    pub fn stale_result_cleanup_interval(mut self, interval: Duration) -> Self {
        self.stale_result_cleanup_interval = interval;
        self
    }

    /// Set the timeout after which pending/processing results are considered stale.
    /// Results older than this are marked as failed during cleanup.
    /// Defaults to 300 seconds (5 minutes).
    pub fn stale_result_timeout(mut self, timeout: Duration) -> Self {
        self.stale_result_timeout = timeout;
        self
    }

    pub fn in_memory_cache_total_max_bytes(mut self, bytes: u64) -> Self {
        self.in_memory_cache_total_max_bytes = bytes;
        self
    }

    pub fn in_memory_cache_table_max_bytes(mut self, bytes: u64) -> Self {
        self.in_memory_cache_table_max_bytes = bytes;
        self
    }

    pub fn in_memory_cache_ttl(mut self, ttl: Duration) -> Self {
        self.in_memory_cache_ttl = ttl;
        self
    }

    pub fn in_memory_cache_max_concurrent_loads(mut self, max: usize) -> Self {
        self.in_memory_cache_max_concurrent_loads = max.max(1);
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

        // Wrap catalog with caching layer if Redis is configured
        let catalog: Arc<dyn CatalogManager> = if let Some(cache_config) = &self.cache_config {
            if let Some(ref redis_url) = cache_config.redis_url {
                Arc::new(
                    CachingCatalogManager::new(catalog, redis_url, cache_config.clone())
                        .await
                        .map_err(|e| anyhow::anyhow!("Failed to create caching catalog: {}", e))?,
                )
            } else {
                catalog
            }
        } else {
            catalog
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

        let df_ctx = {
            // Get actual object stores for instrumentation (preserves full config like MinIO path-style)
            let object_stores: Vec<_> = storage.get_object_store().into_iter().collect();

            // Build liquid-cache config if server is configured
            let liquid_cache_config = self.liquid_cache_server.map(|server| {
                let store_configs: Vec<_> = storage.get_object_store_config().into_iter().collect();
                (server, store_configs)
            });

            // Build instrumented context - all object stores get tracing,
            // liquid-cache adds the PushdownOptimizer if configured
            build_instrumented_context(object_stores, liquid_cache_config)?
        };

        let parquet_cache = if self.in_memory_cache_total_max_bytes > 0
            && self.in_memory_cache_table_max_bytes > 0
        {
            let ttl = if self.in_memory_cache_ttl.is_zero() {
                None
            } else {
                Some(self.in_memory_cache_ttl)
            };
            let cache = Arc::new(InMemoryTableCache::new(
                self.in_memory_cache_total_max_bytes,
                self.in_memory_cache_table_max_bytes,
                ttl,
            ));
            let warm_semaphore =
                Arc::new(Semaphore::new(self.in_memory_cache_max_concurrent_loads));
            Some(Arc::new(ParquetCacheManager::new(
                cache,
                storage.clone(),
                warm_semaphore,
            )))
        } else {
            None
        };

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

        // Start background stale result cleanup worker
        let stale_result_cleanup_handle = RuntimeEngine::start_stale_result_cleanup_worker(
            catalog.clone(),
            shutdown_token.clone(),
            self.stale_result_cleanup_interval,
            self.stale_result_timeout,
        );

        // Initialize catalog (starts warmup loop if configured)
        catalog.init().await?;

        // Create the unified async catalog list for all catalogs
        // This handles connections, datasets, and runtimedb catalogs
        let unified_catalog_list = Arc::new(UnifiedCatalogList::new(
            catalog.clone(),
            orchestrator.clone(),
            &df_ctx,
            parquet_cache.clone(),
        ));

        let engine = RuntimeEngine {
            catalog,
            df_ctx,
            storage,
            orchestrator,
            secret_manager,
            unified_catalog_list,
            shutdown_token,
            deletion_worker_handle: Mutex::new(Some(deletion_worker_handle)),
            deletion_grace_period: self.deletion_grace_period,
            deletion_worker_interval: self.deletion_worker_interval,
            parallel_refresh_count: self.parallel_refresh_count,
            persistence_semaphore: Arc::new(Semaphore::new(self.max_concurrent_persistence)),
            persistence_tasks: Mutex::new(JoinSet::new()),
            stale_result_cleanup_handle: Mutex::new(stale_result_cleanup_handle),
            parquet_cache,
        };

        // Note: All catalogs (connections, datasets, runtimedb) are now resolved on-demand
        // during query execution via the unified AsyncCatalogProviderList.

        // Process any pending deletions from previous runs
        if let Err(e) = engine.process_pending_deletions().await {
            warn!("Failed to process pending deletions on startup: {}", e);
        }

        Ok(engine)
    }
}

/// Background task to persist query results to parquet.
/// Updates catalog status to "ready" on success or "failed" on error.
async fn persist_result_background(
    result_id: &str,
    schema: &Arc<Schema>,
    batches: &[RecordBatch],
    storage: Arc<dyn StorageManager>,
    catalog: Arc<dyn CatalogManager>,
) -> Result<()> {
    // Helper to mark result as failed with error message
    async fn mark_failed(catalog: &dyn CatalogManager, result_id: &str, error: &anyhow::Error) {
        let error_msg = error.to_string();
        match catalog
            .update_result(
                result_id,
                ResultUpdate::Failed {
                    error_message: Some(&error_msg),
                },
            )
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                tracing::warn!(
                    result_id = %result_id,
                    "Result row not found when marking as failed, may have been deleted"
                );
            }
            Err(e) => {
                tracing::warn!(
                    result_id = %result_id,
                    error = %e,
                    "Failed to mark result as failed in catalog"
                );
            }
        }
    }

    // Prepare write location
    let handle =
        storage.prepare_cache_write(INTERNAL_CONNECTION_ID, RESULTS_SCHEMA_NAME, result_id);

    // Write parquet file (sync I/O in blocking task to avoid blocking the runtime)
    let local_path = handle.local_path.clone();
    let schema_clone = Arc::clone(schema);
    let batches_clone: Vec<RecordBatch> = batches.to_vec();

    let write_result = tokio::task::spawn_blocking(move || {
        let mut writer: Box<dyn BatchWriter> = Box::new(StreamingParquetWriter::new(local_path));
        writer.init(&schema_clone)?;
        for batch in &batches_clone {
            writer.write_batch(batch)?;
        }
        writer.close()
    })
    .await
    .map_err(|e| anyhow::anyhow!("persist_result: task join failed: {}", e))?;

    if let Err(e) = write_result {
        let error = anyhow::anyhow!("persist_result: parquet write failed: {}", e);
        mark_failed(catalog.as_ref(), result_id, &error).await;
        return Err(error);
    }

    // Finalize storage (uploads to S3 if needed)
    let dir_url = match storage.finalize_cache_write(&handle).await {
        Ok(url) => url,
        Err(e) => {
            let error = anyhow::anyhow!("persist_result: storage finalization failed: {}", e);
            mark_failed(catalog.as_ref(), result_id, &error).await;
            return Err(error);
        }
    };

    let file_url = format!("{}/data.parquet", dir_url);

    // Update catalog to ready
    match catalog
        .update_result(
            result_id,
            ResultUpdate::Ready {
                parquet_path: &file_url,
            },
        )
        .await
    {
        Ok(true) => Ok(()),
        Ok(false) => {
            // Result row was not found - it may have been deleted concurrently
            tracing::warn!(
                result_id = %result_id,
                "Result row not found when finalizing, may have been deleted concurrently"
            );
            Err(anyhow::anyhow!(
                "persist_result: result row not found when finalizing"
            ))
        }
        Err(e) => {
            let error = anyhow::anyhow!("persist_result: catalog update failed: {}", e);
            mark_failed(catalog.as_ref(), result_id, &error).await;
            Err(error)
        }
    }
}

// =========================================================================
// Instrumented SessionContext Builder
// =========================================================================

/// Object store configuration for liquid-cache: URL and options (credentials, etc.)
type ObjectStoreConfig = (ObjectStoreUrl, HashMap<String, String>);

/// Liquid-cache configuration: server address and store configs for PushdownOptimizer
type LiquidCacheConfig = (String, Vec<ObjectStoreConfig>);

/// Builds an instrumented SessionContext with optional liquid-cache support.
///
/// This function creates a SessionContext with full tracing instrumentation:
///
/// 1. **Object stores**: All provided stores are wrapped with `instrument_object_store()`
///    for storage I/O tracing (uses actual store instances, not rebuilt from config)
/// 2. **DataFusion operators**: Each operator gets a span via `datafusion-tracing`
/// 3. **Liquid-cache optimizer**: When configured, adds the `PushdownOptimizer`
///
/// ## Arguments
///
/// * `object_stores` - Pre-built object stores to wrap with instrumentation. These are
///   the actual store instances (e.g., from S3Storage), ensuring configuration like
///   path-style URLs for MinIO is preserved.
/// * `liquid_cache_config` - Optional (server_address, store_configs) for liquid-cache.
///   The store configs are sent to the liquid-cache server so it can build matching stores.
fn build_instrumented_context(
    object_stores: Vec<(ObjectStoreUrl, Arc<dyn ObjectStore>)>,
    liquid_cache_config: Option<LiquidCacheConfig>,
) -> Result<SessionContext> {
    let runtime_env = Arc::new(RuntimeEnv::default());

    // Register instrumented object stores (using actual pre-built stores)
    for (url, store) in object_stores {
        let url_ref: &url::Url = url.as_ref();
        let prefix = url_ref.scheme();
        let instrumented_store = instrument_object_store(store, prefix);

        info!(
            url = %url.as_str(),
            prefix = %prefix,
            "Registering instrumented object store"
        );
        runtime_env.register_object_store(url.as_ref(), instrumented_store);
    }

    // Configure session (liquid-cache has specific recommended settings)
    let session_config = if liquid_cache_config.is_some() {
        SessionConfig::new()
            .with_target_partitions(1)
            .with_round_robin_repartition(false)
    } else {
        SessionConfig::new()
    };

    // Build SessionState with tracing instrumentation
    let mut state_builder = SessionStateBuilder::new()
        .with_config(session_config)
        .with_runtime_env(runtime_env)
        .with_default_features()
        .with_physical_optimizer_rule(
            instrument_with_info_spans!(options: InstrumentationOptions::default()),
        );

    // Add liquid-cache pushdown optimizer if configured
    if let Some((server_address, store_configs)) = liquid_cache_config {
        info!(server = %server_address, "Adding liquid-cache pushdown optimizer");
        let pushdown_optimizer = PushdownOptimizer::new(server_address, store_configs);
        state_builder = state_builder.with_physical_optimizer_rule(Arc::new(pushdown_optimizer));
    }

    Ok(SessionContext::from(state_builder.build()))
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
            AppConfig, CacheConfig, CatalogConfig, LiquidCacheConfig, PathsConfig, SecretsConfig,
            ServerConfig, StorageConfig,
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
                access_key: None,
                secret_key: None,
            },
            paths: PathsConfig {
                base_dir: Some(base_dir.to_str().unwrap().to_string()),
                cache_dir: None,
            },
            secrets: SecretsConfig {
                encryption_key: Some(test_secret_key()),
            },
            liquid_cache: LiquidCacheConfig {
                enabled: false,
                server_address: None,
            },
            cache: CacheConfig::default(),
            engine: crate::config::EngineConfig::default(),
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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stale_result_cleanup_worker_cleans_stuck_results() {
        use crate::catalog::ResultUpdate;

        let temp_dir = TempDir::new().unwrap();
        let base_dir = temp_dir.path().to_path_buf();

        // Create engine with short cleanup interval and very short timeout for testing
        let engine = RuntimeEngine::builder()
            .base_dir(base_dir)
            .secret_key(test_secret_key())
            .stale_result_cleanup_interval(Duration::from_millis(50))
            .stale_result_timeout(Duration::from_millis(100))
            .build()
            .await
            .unwrap();

        // Verify cleanup worker is running
        assert!(
            engine.stale_result_cleanup_handle.lock().await.is_some(),
            "Cleanup worker handle should exist"
        );

        // Create a result and leave it in Processing state
        let result_id = engine
            .catalog
            .create_result(ResultStatus::Pending)
            .await
            .unwrap();
        engine
            .catalog
            .update_result(&result_id, ResultUpdate::Processing)
            .await
            .unwrap();

        // Verify it's in processing state
        let result = engine
            .catalog
            .get_result(&result_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.status, ResultStatus::Processing);

        // Wait for the cleanup interval + timeout to pass
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Now the worker should have cleaned it up
        let result = engine
            .catalog
            .get_result(&result_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            result.status,
            ResultStatus::Failed,
            "Stale result should be marked as failed"
        );
        assert!(
            result.error_message.is_some(),
            "Failed result should have error message"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stale_result_cleanup_worker_disabled_when_interval_zero() {
        let temp_dir = TempDir::new().unwrap();
        let base_dir = temp_dir.path().to_path_buf();

        // Create engine with cleanup disabled (interval = 0)
        let engine = RuntimeEngine::builder()
            .base_dir(base_dir)
            .secret_key(test_secret_key())
            .stale_result_cleanup_interval(Duration::ZERO)
            .build()
            .await
            .unwrap();

        // Verify cleanup worker is NOT running
        assert!(
            engine.stale_result_cleanup_handle.lock().await.is_none(),
            "Cleanup worker handle should be None when disabled"
        );
    }

    #[tokio::test]
    async fn test_instrumented_context_basic() {
        let ctx = build_instrumented_context(vec![], None).unwrap();
        assert!(ctx.state().config().target_partitions() > 0);
    }

    #[tokio::test]
    async fn test_instrumented_context_executes_sql() {
        use datafusion::arrow::array::{Int64Array, StringArray};

        let ctx = build_instrumented_context(vec![], None).unwrap();
        let result = ctx
            .sql("SELECT * FROM (VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')) AS t(id, name)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 3);

        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 1);

        let name_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "Alice");
    }
}
