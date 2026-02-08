use crate::secrets::{SecretMetadata, SecretStatus};
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::FromRow;
use std::fmt::Debug;

/// Compute SHA-256 hex hash of SQL text.
pub fn sql_hash(sql: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(sql.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Status of a persisted query result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ResultStatus {
    /// Result is reserved but query hasn't started yet (for future async query API)
    Pending,
    /// Result is being processed (query executing or persistence in progress)
    Processing,
    /// Result is ready for retrieval
    Ready,
    /// Result failed (query or persistence error)
    Failed,
}

impl ResultStatus {
    /// Convert from database string representation.
    /// Unknown values are treated as Failed and logged as a warning.
    pub fn parse(s: &str) -> Self {
        match s {
            "pending" => Self::Pending,
            "processing" => Self::Processing,
            "ready" => Self::Ready,
            "failed" => Self::Failed,
            unknown => {
                tracing::warn!(
                    status = %unknown,
                    "Unknown result status in database, treating as failed"
                );
                Self::Failed
            }
        }
    }

    /// Convert to database string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Processing => "processing",
            Self::Ready => "ready",
            Self::Failed => "failed",
        }
    }
}

impl std::fmt::Display for ResultStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// State update for an existing query result.
/// Used with [`CatalogManager::update_result`] to transition result state.
#[derive(Debug, Clone)]
pub enum ResultUpdate<'a> {
    /// Transition to processing state (query executing or persistence in progress)
    Processing,
    /// Transition to ready state with the parquet file path
    Ready { parquet_path: &'a str },
    /// Transition to failed state with an optional error message
    Failed { error_message: Option<&'a str> },
}

/// Used to conditionally update a secret only if it hasn't been modified.
#[derive(Debug, Clone, Copy)]
pub struct OptimisticLock {
    pub created_at: DateTime<Utc>,
}

impl From<DateTime<Utc>> for OptimisticLock {
    fn from(created_at: DateTime<Utc>) -> Self {
        Self { created_at }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ConnectionInfo {
    pub id: String,
    pub name: String,
    pub source_type: String,
    pub config_json: String,
    /// ID of the secret in the secret manager (if any).
    pub secret_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct TableInfo {
    pub id: i32,
    pub connection_id: String,
    pub schema_name: String,
    pub table_name: String,
    pub parquet_path: Option<String>,
    pub last_sync: Option<String>,
    pub arrow_schema_json: Option<String>,
}

/// Record for deferred file deletion (survives restarts)
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct PendingDeletion {
    pub id: i32,
    pub path: String,
    pub delete_after: DateTime<Utc>,
    /// Number of failed deletion attempts. After MAX_DELETION_RETRIES,
    /// the record is removed to prevent indefinite accumulation.
    pub retry_count: i32,
}

/// A persisted query result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub id: String,
    pub parquet_path: Option<String>,
    pub status: ResultStatus,
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
}

/// Raw database row for query results (used by sqlx).
#[derive(Debug, Clone, FromRow)]
pub struct QueryResultRow {
    pub id: String,
    pub parquet_path: Option<String>,
    pub status: String,
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl From<QueryResultRow> for QueryResult {
    fn from(row: QueryResultRow) -> Self {
        Self {
            id: row.id,
            parquet_path: row.parquet_path,
            status: ResultStatus::parse(&row.status),
            error_message: row.error_message,
            created_at: row.created_at,
        }
    }
}

/// Status of a query run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryRunStatus {
    Running,
    Succeeded,
    Failed,
}

impl QueryRunStatus {
    pub fn parse(s: &str) -> Self {
        match s {
            "running" => Self::Running,
            "succeeded" => Self::Succeeded,
            "failed" => Self::Failed,
            unknown => {
                tracing::warn!(
                    status = %unknown,
                    "Unknown query run status in database, treating as failed"
                );
                Self::Failed
            }
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }
}

impl std::fmt::Display for QueryRunStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// A query run record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRun {
    pub id: String,
    /// SQL text, resolved from sql_snapshots via JOIN.
    pub sql_text: String,
    /// SHA-256 hash of the SQL text, resolved from sql_snapshots via JOIN.
    pub sql_hash: String,
    /// The sql_snapshot that contains the SQL text for this run.
    pub snapshot_id: String,
    pub trace_id: Option<String>,
    pub status: QueryRunStatus,
    pub result_id: Option<String>,
    pub error_message: Option<String>,
    pub warning_message: Option<String>,
    pub row_count: Option<i64>,
    pub execution_time_ms: Option<i64>,
    pub saved_query_id: Option<String>,
    pub saved_query_version: Option<i32>,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

/// Raw database row for query runs (used by sqlx in SQLite where timestamps are strings).
/// sql_text is resolved from sql_snapshots via JOIN.
#[derive(Debug, Clone, FromRow)]
pub struct QueryRunRow {
    pub id: String,
    pub sql_text: String,
    pub sql_hash: String,
    pub snapshot_id: String,
    pub trace_id: Option<String>,
    pub status: String,
    pub result_id: Option<String>,
    pub error_message: Option<String>,
    pub warning_message: Option<String>,
    pub row_count: Option<i64>,
    pub execution_time_ms: Option<i64>,
    pub saved_query_id: Option<String>,
    pub saved_query_version: Option<i32>,
    pub created_at: String,
    pub completed_at: Option<String>,
}

impl QueryRunRow {
    pub fn into_query_run(self) -> QueryRun {
        let created_at = self.created_at.parse().unwrap_or_else(|e| {
            tracing::warn!(
                id = %self.id,
                raw_timestamp = %self.created_at,
                error = %e,
                "Failed to parse query run created_at, falling back to now"
            );
            Utc::now()
        });
        let completed_at = self.completed_at.and_then(|s| {
            s.parse()
                .inspect_err(|e| {
                    tracing::warn!(
                        id = %self.id,
                        raw_timestamp = %s,
                        error = %e,
                        "Failed to parse query run completed_at, treating as null"
                    );
                })
                .ok()
        });
        QueryRun {
            id: self.id,
            sql_text: self.sql_text,
            sql_hash: self.sql_hash,
            snapshot_id: self.snapshot_id,
            trace_id: self.trace_id,
            status: QueryRunStatus::parse(&self.status),
            result_id: self.result_id,
            error_message: self.error_message,
            warning_message: self.warning_message,
            row_count: self.row_count,
            execution_time_ms: self.execution_time_ms,
            saved_query_id: self.saved_query_id,
            saved_query_version: self.saved_query_version,
            created_at,
            completed_at,
        }
    }
}

/// Raw database row for query runs in Postgres (native timestamps).
/// sql_text is resolved from sql_snapshots via JOIN.
#[derive(Debug, Clone, FromRow)]
pub struct QueryRunRowPg {
    pub id: String,
    pub sql_text: String,
    pub sql_hash: String,
    pub snapshot_id: String,
    pub trace_id: Option<String>,
    pub status: String,
    pub result_id: Option<String>,
    pub error_message: Option<String>,
    pub warning_message: Option<String>,
    pub row_count: Option<i64>,
    pub execution_time_ms: Option<i64>,
    pub saved_query_id: Option<String>,
    pub saved_query_version: Option<i32>,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

impl From<QueryRunRowPg> for QueryRun {
    fn from(row: QueryRunRowPg) -> Self {
        Self {
            id: row.id,
            sql_text: row.sql_text,
            sql_hash: row.sql_hash,
            snapshot_id: row.snapshot_id,
            trace_id: row.trace_id,
            status: QueryRunStatus::parse(&row.status),
            result_id: row.result_id,
            error_message: row.error_message,
            warning_message: row.warning_message,
            row_count: row.row_count,
            execution_time_ms: row.execution_time_ms,
            saved_query_id: row.saved_query_id,
            saved_query_version: row.saved_query_version,
            created_at: row.created_at,
            completed_at: row.completed_at,
        }
    }
}

/// Parameters for creating a new query run.
pub struct CreateQueryRun<'a> {
    pub id: &'a str,
    pub snapshot_id: &'a str,
    pub trace_id: Option<&'a str>,
    pub saved_query_id: Option<&'a str>,
    pub saved_query_version: Option<i32>,
}

/// Parameters for completing a query run.
pub enum QueryRunUpdate<'a> {
    Succeeded {
        result_id: Option<&'a str>,
        row_count: i64,
        execution_time_ms: i64,
        warning_message: Option<&'a str>,
    },
    Failed {
        error_message: &'a str,
        execution_time_ms: Option<i64>,
    },
}

/// Cursor for keyset pagination of query runs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRunCursor {
    pub created_at: DateTime<Utc>,
    pub id: String,
}

/// A pending file upload.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct UploadInfo {
    pub id: String,
    pub status: String,
    pub storage_url: String,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub size_bytes: i64,
    pub created_at: DateTime<Utc>,
    pub consumed_at: Option<DateTime<Utc>>,
}

/// A saved query with versioned SQL snapshots.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SavedQuery {
    pub id: String,
    pub name: String,
    pub latest_version: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Raw database row for saved queries in SQLite (timestamps as strings).
#[derive(Debug, Clone, FromRow)]
pub struct SavedQueryRow {
    pub id: String,
    pub name: String,
    pub latest_version: i32,
    pub created_at: String,
    pub updated_at: String,
}

impl SavedQueryRow {
    pub fn into_saved_query(self) -> SavedQuery {
        let created_at = self.created_at.parse().unwrap_or_else(|e| {
            tracing::warn!(
                id = %self.id,
                raw_timestamp = %self.created_at,
                error = %e,
                "Failed to parse saved query created_at, falling back to now"
            );
            Utc::now()
        });
        let updated_at = self.updated_at.parse().unwrap_or_else(|e| {
            tracing::warn!(
                id = %self.id,
                raw_timestamp = %self.updated_at,
                error = %e,
                "Failed to parse saved query updated_at, falling back to now"
            );
            Utc::now()
        });
        SavedQuery {
            id: self.id,
            name: self.name,
            latest_version: self.latest_version,
            created_at,
            updated_at,
        }
    }
}

/// Raw database row for saved queries in Postgres (native timestamps).
#[derive(Debug, Clone, FromRow)]
pub struct SavedQueryRowPg {
    pub id: String,
    pub name: String,
    pub latest_version: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<SavedQueryRowPg> for SavedQuery {
    fn from(row: SavedQueryRowPg) -> Self {
        Self {
            id: row.id,
            name: row.name,
            latest_version: row.latest_version,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

/// A content-addressable SQL snapshot. Deduplicated by (sql_hash, sql_text).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlSnapshot {
    pub id: String,
    pub sql_hash: String,
    pub sql_text: String,
    pub created_at: DateTime<Utc>,
}

/// Raw database row for SQL snapshots in SQLite (timestamps as strings).
#[derive(Debug, Clone, FromRow)]
pub struct SqlSnapshotRow {
    pub id: String,
    pub sql_hash: String,
    pub sql_text: String,
    pub created_at: String,
}

impl SqlSnapshotRow {
    pub fn into_sql_snapshot(self) -> SqlSnapshot {
        let created_at = self.created_at.parse().unwrap_or_else(|e| {
            tracing::warn!(
                id = %self.id,
                raw_timestamp = %self.created_at,
                error = %e,
                "Failed to parse sql snapshot created_at, falling back to now"
            );
            Utc::now()
        });
        SqlSnapshot {
            id: self.id,
            sql_hash: self.sql_hash,
            sql_text: self.sql_text,
            created_at,
        }
    }
}

/// Raw database row for SQL snapshots in Postgres (native timestamps).
#[derive(Debug, Clone, FromRow)]
pub struct SqlSnapshotRowPg {
    pub id: String,
    pub sql_hash: String,
    pub sql_text: String,
    pub created_at: DateTime<Utc>,
}

impl From<SqlSnapshotRowPg> for SqlSnapshot {
    fn from(row: SqlSnapshotRowPg) -> Self {
        Self {
            id: row.id,
            sql_hash: row.sql_hash,
            sql_text: row.sql_text,
            created_at: row.created_at,
        }
    }
}

/// An immutable SQL version under a saved query.
/// The sql_text and sql_hash fields are resolved via JOIN to sql_snapshots at query time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SavedQueryVersion {
    pub saved_query_id: String,
    pub version: i32,
    pub snapshot_id: String,
    pub sql_text: String,
    pub sql_hash: String,
    pub created_at: DateTime<Utc>,
}

/// Raw database row for saved query versions in SQLite (timestamps as strings).
/// Fields sql_text and sql_hash come from JOIN to sql_snapshots.
#[derive(Debug, Clone, FromRow)]
pub struct SavedQueryVersionRow {
    pub saved_query_id: String,
    pub version: i32,
    pub snapshot_id: String,
    pub sql_text: String,
    pub sql_hash: String,
    pub created_at: String,
}

impl SavedQueryVersionRow {
    pub fn into_saved_query_version(self) -> SavedQueryVersion {
        let created_at = self.created_at.parse().unwrap_or_else(|e| {
            tracing::warn!(
                saved_query_id = %self.saved_query_id,
                version = %self.version,
                raw_timestamp = %self.created_at,
                error = %e,
                "Failed to parse saved query version created_at, falling back to now"
            );
            Utc::now()
        });
        SavedQueryVersion {
            saved_query_id: self.saved_query_id,
            version: self.version,
            snapshot_id: self.snapshot_id,
            sql_text: self.sql_text,
            sql_hash: self.sql_hash,
            created_at,
        }
    }
}

/// Raw database row for saved query versions in Postgres (native timestamps).
/// Fields sql_text and sql_hash come from JOIN to sql_snapshots.
#[derive(Debug, Clone, FromRow)]
pub struct SavedQueryVersionRowPg {
    pub saved_query_id: String,
    pub version: i32,
    pub snapshot_id: String,
    pub sql_text: String,
    pub sql_hash: String,
    pub created_at: DateTime<Utc>,
}

impl From<SavedQueryVersionRowPg> for SavedQueryVersion {
    fn from(row: SavedQueryVersionRowPg) -> Self {
        Self {
            saved_query_id: row.saved_query_id,
            version: row.version,
            snapshot_id: row.snapshot_id,
            sql_text: row.sql_text,
            sql_hash: row.sql_hash,
            created_at: row.created_at,
        }
    }
}

/// A user-curated dataset.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct DatasetInfo {
    pub id: String,
    pub label: String,
    pub schema_name: String,
    pub table_name: String,
    pub parquet_url: String,
    pub arrow_schema_json: String,
    pub source_type: String,
    pub source_config: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Async interface for catalog operations.
#[async_trait]
pub trait CatalogManager: Debug + Send + Sync {
    /// Initialize the catalog manager. Called after construction to start any background tasks.
    /// Default implementation does nothing.
    async fn init(&self) -> Result<()> {
        Ok(())
    }

    /// Close the catalog connection. This is idempotent and can be called multiple times.
    async fn close(&self) -> Result<()> {
        // Default implementation does nothing - sqlx pools handle cleanup automatically
        Ok(())
    }

    /// Apply any pending schema migrations. Should be idempotent.
    async fn run_migrations(&self) -> Result<()>;

    async fn list_connections(&self) -> Result<Vec<ConnectionInfo>>;
    async fn add_connection(
        &self,
        name: &str,
        source_type: &str,
        config_json: &str,
        secret_id: Option<&str>,
    ) -> Result<String>;
    async fn get_connection(&self, id: &str) -> Result<Option<ConnectionInfo>>;
    async fn get_connection_by_name(&self, name: &str) -> Result<Option<ConnectionInfo>>;
    async fn add_table(
        &self,
        connection_id: &str,
        schema_name: &str,
        table_name: &str,
        arrow_schema_json: &str,
    ) -> Result<i32>;
    async fn list_tables(&self, connection_id: Option<&str>) -> Result<Vec<TableInfo>>;
    async fn get_table(
        &self,
        connection_id: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<TableInfo>>;
    async fn update_table_sync(&self, table_id: i32, parquet_path: &str) -> Result<()>;

    /// Clear table cache metadata (set paths to NULL) without deleting files.
    async fn clear_table_cache_metadata(
        &self,
        connection_id: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<TableInfo>;

    /// Clear cache metadata for all tables in a connection (set paths to NULL).
    async fn clear_connection_cache_metadata(&self, connection_id: &str) -> Result<()>;

    /// Delete connection and all associated table rows from metadata.
    async fn delete_connection(&self, connection_id: &str) -> Result<()>;

    // NOTE: Stale table detection (tables removed from remote source) is intentionally
    // not implemented. The naive approach of comparing discovered vs existing tables
    // is error-prone and doesn't handle data cleanup properly. Stale tables will
    // remain in the catalog until a more robust solution is implemented.

    /// Schedule a file path for deletion after a grace period.
    async fn schedule_file_deletion(&self, path: &str, delete_after: DateTime<Utc>) -> Result<()>;

    /// Get all pending file deletions that are due for cleanup.
    async fn get_pending_deletions(&self) -> Result<Vec<PendingDeletion>>;

    /// Increment the retry count for a failed deletion. Returns the new count.
    async fn increment_deletion_retry(&self, id: i32) -> Result<i32>;

    /// Remove a pending deletion record after successful delete or max retries.
    async fn remove_pending_deletion(&self, id: i32) -> Result<()>;

    // Secret management methods - metadata (used by all secret providers)

    /// Get metadata for an active secret (without value).
    /// Returns None for secrets with status != 'active'.
    async fn get_secret_metadata(&self, name: &str) -> Result<Option<SecretMetadata>>;

    /// Get metadata for a secret regardless of status (for internal cleanup).
    async fn get_secret_metadata_any_status(&self, name: &str) -> Result<Option<SecretMetadata>>;

    /// Create secret metadata. Fails if the secret already exists.
    async fn create_secret_metadata(&self, metadata: &SecretMetadata) -> Result<()>;

    /// Update existing secret metadata.
    /// If `lock` is Some, only updates if created_at matches (returns false on mismatch).
    /// If `lock` is None, updates unconditionally.
    async fn update_secret_metadata(
        &self,
        metadata: &SecretMetadata,
        lock: Option<OptimisticLock>,
    ) -> Result<bool>;

    /// Set the status of a secret.
    async fn set_secret_status(&self, name: &str, status: SecretStatus) -> Result<bool>;

    /// Delete secret metadata. Returns true if the secret existed.
    async fn delete_secret_metadata(&self, name: &str) -> Result<bool>;

    /// List all active secrets (metadata only).
    async fn list_secrets(&self) -> Result<Vec<SecretMetadata>>;

    // Secret management methods - encrypted storage (used by EncryptedSecretManager only)

    /// Get the encrypted value for a secret by its ID.
    async fn get_encrypted_secret(&self, secret_id: &str) -> Result<Option<Vec<u8>>>;

    /// Store or update an encrypted secret value.
    async fn put_encrypted_secret_value(
        &self,
        secret_id: &str,
        encrypted_value: &[u8],
    ) -> Result<()>;

    /// Delete an encrypted secret value. Returns true if it existed.
    async fn delete_encrypted_secret_value(&self, secret_id: &str) -> Result<bool>;

    /// Get secret metadata by ID.
    async fn get_secret_metadata_by_id(&self, id: &str) -> Result<Option<SecretMetadata>>;

    /// Count how many connections reference a given secret_id.
    async fn count_connections_by_secret_id(&self, secret_id: &str) -> Result<i64>;

    // Query run history methods

    /// Create a new query run record (status = 'running').
    /// Returns the generated query run ID.
    async fn create_query_run(&self, params: CreateQueryRun<'_>) -> Result<String>;

    /// Update a query run on completion (success or failure).
    /// Returns true if the query run was found and updated.
    async fn update_query_run(&self, id: &str, update: QueryRunUpdate<'_>) -> Result<bool>;

    /// List query runs with keyset (cursor) pagination.
    /// Returns (runs, has_more).
    async fn list_query_runs(
        &self,
        limit: usize,
        cursor: Option<&QueryRunCursor>,
    ) -> Result<(Vec<QueryRun>, bool)>;

    /// Get a single query run by ID.
    async fn get_query_run(&self, id: &str) -> Result<Option<QueryRun>>;

    // Query result persistence methods

    /// Create a new query result with the given initial status.
    /// Returns the generated result ID.
    ///
    /// For the current sync-query-with-async-persistence flow, use `ResultStatus::Processing`.
    /// For a future fully-async query API, use `ResultStatus::Pending` initially.
    async fn create_result(&self, initial_status: ResultStatus) -> Result<String>;

    /// Update an existing result's state.
    /// Returns true if the result was found and updated, false if not found.
    async fn update_result(&self, id: &str, update: ResultUpdate<'_>) -> Result<bool>;

    /// Get a query result by ID. Returns None if not found.
    async fn get_result(&self, id: &str) -> Result<Option<QueryResult>>;

    /// List query results with pagination.
    /// Results are ordered by created_at descending (newest first).
    /// Returns (results, has_more) where has_more indicates if there are more results after this page.
    async fn list_results(&self, limit: usize, offset: usize) -> Result<(Vec<QueryResult>, bool)>;

    /// Get a queryable result (status = 'ready' only).
    /// Used by ResultsSchemaProvider for SQL queries over results.
    async fn get_queryable_result(&self, id: &str) -> Result<Option<QueryResult>>;

    /// Mark stale processing/pending results as failed.
    /// Results that have been in a non-terminal state for longer than the cutoff time are marked as failed.
    /// Returns the number of results cleaned up.
    async fn cleanup_stale_results(&self, cutoff: DateTime<Utc>) -> Result<usize>;

    // Upload management methods

    /// Create a new upload record.
    async fn create_upload(&self, upload: &UploadInfo) -> Result<()>;

    /// Get an upload by ID.
    async fn get_upload(&self, id: &str) -> Result<Option<UploadInfo>>;

    /// List uploads, optionally filtered by status.
    async fn list_uploads(&self, status: Option<&str>) -> Result<Vec<UploadInfo>>;

    /// Mark an upload as consumed. Returns true if the upload was pending/processing and is now consumed.
    async fn consume_upload(&self, id: &str) -> Result<bool>;

    /// Atomically claim an upload for processing. Returns true if the upload was pending
    /// and is now in "processing" state. This prevents concurrent dataset creation.
    async fn claim_upload(&self, id: &str) -> Result<bool>;

    /// Release a claimed upload back to pending state. Used when dataset creation fails
    /// after claiming but before consuming. Returns true if the upload was processing.
    async fn release_upload(&self, id: &str) -> Result<bool>;

    // SQL snapshot methods

    /// Get or create a content-addressable SQL snapshot.
    /// Deduplicates by (sql_hash, sql_text). Returns the existing or newly created snapshot.
    async fn get_or_create_snapshot(&self, sql_text: &str) -> Result<SqlSnapshot>;

    // Saved query methods

    /// Create a new saved query with its first version.
    /// The snapshot_id should come from get_or_create_snapshot.
    async fn create_saved_query(&self, name: &str, snapshot_id: &str) -> Result<SavedQuery>;

    /// Get a saved query by ID.
    async fn get_saved_query(&self, id: &str) -> Result<Option<SavedQuery>>;

    /// List saved queries with offset pagination.
    /// Returns (queries, has_more).
    async fn list_saved_queries(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<SavedQuery>, bool)>;

    /// Create a new version for a saved query, optionally updating its name.
    /// The snapshot_id should come from get_or_create_snapshot.
    /// Returns the updated saved query, or None if not found.
    async fn update_saved_query(
        &self,
        id: &str,
        name: Option<&str>,
        snapshot_id: &str,
    ) -> Result<Option<SavedQuery>>;

    /// Delete a saved query by ID. Returns true if it existed.
    /// Versions are hard-deleted (explicitly for SQLite, via CASCADE for Postgres).
    /// Query runs retain history via their snapshot_id FK to sql_snapshots.
    async fn delete_saved_query(&self, id: &str) -> Result<bool>;

    /// Get a specific version of a saved query.
    /// sql_text and sql_hash are resolved via JOIN to sql_snapshots.
    async fn get_saved_query_version(
        &self,
        saved_query_id: &str,
        version: i32,
    ) -> Result<Option<SavedQueryVersion>>;

    /// List all versions of a saved query, ordered by version ascending.
    /// sql_text and sql_hash are resolved via JOIN to sql_snapshots.
    async fn list_saved_query_versions(
        &self,
        saved_query_id: &str,
    ) -> Result<Vec<SavedQueryVersion>>;

    // Dataset management methods

    /// Create a new dataset record.
    async fn create_dataset(&self, dataset: &DatasetInfo) -> Result<()>;

    /// Get a dataset by ID.
    async fn get_dataset(&self, id: &str) -> Result<Option<DatasetInfo>>;

    /// Get a dataset by schema and table name.
    async fn get_dataset_by_table_name(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<DatasetInfo>>;

    /// List datasets with pagination.
    /// Datasets are ordered by label ascending.
    /// Returns (datasets, has_more) where has_more indicates if there are more datasets after this page.
    async fn list_datasets(&self, limit: usize, offset: usize) -> Result<(Vec<DatasetInfo>, bool)>;

    /// List all datasets without pagination.
    /// Datasets are ordered by label ascending.
    /// Use this when you need all dataset names (e.g., for schema introspection).
    async fn list_all_datasets(&self) -> Result<Vec<DatasetInfo>>;

    /// List table names for a specific schema.
    /// Returns only the table_name column for efficiency (avoids loading large JSON fields).
    /// Use this for schema introspection where only names are needed.
    async fn list_dataset_table_names(&self, schema_name: &str) -> Result<Vec<String>>;

    /// Update a dataset's label and table_name. Returns true if the dataset existed.
    async fn update_dataset(&self, id: &str, label: &str, table_name: &str) -> Result<bool>;

    /// Delete a dataset by ID. Returns the deleted dataset if it existed.
    async fn delete_dataset(&self, id: &str) -> Result<Option<DatasetInfo>>;
}
