use crate::catalog::backend::CatalogBackend;
use crate::catalog::manager::{
    CatalogManager, ConnectionInfo, CreateQueryRun, DatasetInfo, OptimisticLock, PendingDeletion,
    QueryResult, QueryResultRow, QueryRun, QueryRunCursor, QueryRunRow, QueryRunUpdate,
    ResultStatus, ResultUpdate, SavedQuery, SavedQueryRow, SavedQueryVersion, SavedQueryVersionRow,
    SqlSnapshot, SqlSnapshotRow, TableInfo, UploadInfo,
};
use crate::catalog::migrations::{
    run_migrations, wrap_migration_sql, CatalogMigrations, Migration, SQLITE_MIGRATIONS,
};
use crate::secrets::{SecretMetadata, SecretStatus};
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{Sqlite, SqlitePool};
use std::fmt::{self, Debug, Formatter};
use std::str::FromStr;
use tracing::warn;

pub struct SqliteCatalogManager {
    backend: CatalogBackend<Sqlite>,
    catalog_path: String,
}

/// Row type for secret metadata queries (SQLite stores timestamps as strings)
#[derive(sqlx::FromRow)]
struct SecretMetadataRow {
    id: String,
    name: String,
    provider: String,
    provider_ref: Option<String>,
    status: String,
    created_at: String,
    updated_at: String,
}

impl SecretMetadataRow {
    fn into_metadata(self) -> SecretMetadata {
        SecretMetadata {
            id: self.id,
            name: self.name,
            provider: self.provider,
            provider_ref: self.provider_ref,
            status: SecretStatus::from_str(&self.status).unwrap_or(SecretStatus::Active),
            created_at: self.created_at.parse().unwrap_or_else(|_| Utc::now()),
            updated_at: self.updated_at.parse().unwrap_or_else(|_| Utc::now()),
        }
    }
}

/// Row type for pending deletions (SQLite stores timestamps as TEXT)
#[derive(sqlx::FromRow)]
struct PendingDeletionRow {
    id: i32,
    path: String,
    delete_after: String,
    retry_count: i32,
}

impl PendingDeletionRow {
    fn into_pending_deletion(self) -> PendingDeletion {
        let delete_after = self.delete_after.parse().unwrap_or_else(|e| {
            // Log warning but use a far-future date to avoid premature deletion.
            // This prevents data loss if timestamps become corrupted.
            warn!(
                path = %self.path,
                raw_timestamp = %self.delete_after,
                error = %e,
                "Failed to parse pending deletion timestamp; deferring deletion by 24 hours"
            );
            Utc::now() + chrono::Duration::hours(24)
        });
        PendingDeletion {
            id: self.id,
            path: self.path,
            delete_after,
            retry_count: self.retry_count,
        }
    }
}

/// Row type for upload queries (SQLite stores timestamps as strings)
#[derive(sqlx::FromRow)]
struct UploadInfoRow {
    id: String,
    status: String,
    storage_url: String,
    content_type: Option<String>,
    content_encoding: Option<String>,
    size_bytes: i64,
    created_at: String,
    consumed_at: Option<String>,
}

impl UploadInfoRow {
    fn into_upload_info(self) -> UploadInfo {
        UploadInfo {
            id: self.id,
            status: self.status,
            storage_url: self.storage_url,
            content_type: self.content_type,
            content_encoding: self.content_encoding,
            size_bytes: self.size_bytes,
            created_at: self.created_at.parse().unwrap_or_else(|_| Utc::now()),
            consumed_at: self.consumed_at.and_then(|s| s.parse().ok()),
        }
    }
}

/// Row type for dataset queries (SQLite stores timestamps as strings)
#[derive(sqlx::FromRow)]
struct DatasetInfoRow {
    id: String,
    label: String,
    schema_name: String,
    table_name: String,
    parquet_url: String,
    arrow_schema_json: String,
    source_type: String,
    source_config: String,
    created_at: String,
    updated_at: String,
}

impl DatasetInfoRow {
    fn into_dataset_info(self) -> DatasetInfo {
        DatasetInfo {
            id: self.id,
            label: self.label,
            schema_name: self.schema_name,
            table_name: self.table_name,
            parquet_url: self.parquet_url,
            arrow_schema_json: self.arrow_schema_json,
            source_type: self.source_type,
            source_config: self.source_config,
            created_at: self.created_at.parse().unwrap_or_else(|_| Utc::now()),
            updated_at: self.updated_at.parse().unwrap_or_else(|_| Utc::now()),
        }
    }
}

impl Debug for SqliteCatalogManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteCatalogManager")
            .field("catalog_path", &self.catalog_path)
            .finish()
    }
}

struct SqliteMigrationBackend;

impl SqliteCatalogManager {
    pub async fn new(db_path: &str) -> Result<Self> {
        let uri = format!("sqlite:{}?mode=rwc", db_path);
        let pool = SqlitePool::connect(&uri).await?;
        let backend = CatalogBackend::new(pool);

        Ok(Self {
            backend,
            catalog_path: db_path.to_string(),
        })
    }
}

#[async_trait]
impl CatalogManager for SqliteCatalogManager {
    #[tracing::instrument(name = "catalog_run_migrations", skip(self), fields(db = "sqlite"))]
    async fn run_migrations(&self) -> Result<()> {
        run_migrations::<SqliteMigrationBackend>(self.backend.pool()).await
    }

    #[tracing::instrument(name = "catalog_list_connections", skip(self), fields(db = "sqlite"))]
    async fn list_connections(&self) -> Result<Vec<ConnectionInfo>> {
        self.backend.list_connections().await
    }

    #[tracing::instrument(
        name = "catalog_add_connection",
        skip(self, config_json),
        fields(db = "sqlite")
    )]
    async fn add_connection(
        &self,
        name: &str,
        source_type: &str,
        config_json: &str,
        secret_id: Option<&str>,
    ) -> Result<String> {
        self.backend
            .add_connection(name, source_type, config_json, secret_id)
            .await
    }

    #[tracing::instrument(name = "catalog_get_connection", skip(self), fields(db = "sqlite"))]
    async fn get_connection(&self, id: &str) -> Result<Option<ConnectionInfo>> {
        self.backend.get_connection(id).await
    }

    #[tracing::instrument(
        name = "catalog_get_connection_by_name",
        skip(self),
        fields(db = "sqlite")
    )]
    async fn get_connection_by_name(&self, name: &str) -> Result<Option<ConnectionInfo>> {
        self.backend.get_connection_by_name(name).await
    }

    #[tracing::instrument(
        name = "catalog_add_table",
        skip(self, arrow_schema_json),
        fields(db = "sqlite")
    )]
    async fn add_table(
        &self,
        connection_id: &str,
        schema_name: &str,
        table_name: &str,
        arrow_schema_json: &str,
    ) -> Result<i32> {
        self.backend
            .add_table(connection_id, schema_name, table_name, arrow_schema_json)
            .await
    }

    #[tracing::instrument(name = "catalog_list_tables", skip(self), fields(db = "sqlite"))]
    async fn list_tables(&self, connection_id: Option<&str>) -> Result<Vec<TableInfo>> {
        self.backend.list_tables(connection_id).await
    }

    #[tracing::instrument(name = "catalog_get_table", skip(self), fields(db = "sqlite"))]
    async fn get_table(
        &self,
        connection_id: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<TableInfo>> {
        self.backend
            .get_table(connection_id, schema_name, table_name)
            .await
    }

    #[tracing::instrument(name = "catalog_update_table_sync", skip(self), fields(db = "sqlite"))]
    async fn update_table_sync(&self, table_id: i32, parquet_path: &str) -> Result<()> {
        self.backend.update_table_sync(table_id, parquet_path).await
    }

    #[tracing::instrument(
        name = "catalog_clear_table_cache_metadata",
        skip(self),
        fields(db = "sqlite")
    )]
    async fn clear_table_cache_metadata(
        &self,
        connection_id: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<TableInfo> {
        self.backend
            .clear_table_cache_metadata(connection_id, schema_name, table_name)
            .await
    }

    #[tracing::instrument(
        name = "catalog_clear_connection_cache_metadata",
        skip(self),
        fields(db = "sqlite")
    )]
    async fn clear_connection_cache_metadata(&self, connection_id: &str) -> Result<()> {
        self.backend
            .clear_connection_cache_metadata(connection_id)
            .await
    }

    #[tracing::instrument(name = "catalog_delete_connection", skip(self), fields(db = "sqlite"))]
    async fn delete_connection(&self, connection_id: &str) -> Result<()> {
        self.backend.delete_connection(connection_id).await
    }

    #[tracing::instrument(
        name = "catalog_get_secret_metadata",
        skip(self),
        fields(db = "sqlite")
    )]
    async fn get_secret_metadata(&self, name: &str) -> Result<Option<SecretMetadata>> {
        let row: Option<SecretMetadataRow> = sqlx::query_as(
            "SELECT id, name, provider, provider_ref, status, created_at, updated_at \
             FROM secrets WHERE name = ? AND status = 'active'",
        )
        .bind(name)
        .fetch_optional(self.backend.pool())
        .await?;

        Ok(row.map(SecretMetadataRow::into_metadata))
    }

    #[tracing::instrument(
        name = "catalog_get_secret_metadata_any_status",
        skip(self),
        fields(db = "sqlite")
    )]
    async fn get_secret_metadata_any_status(&self, name: &str) -> Result<Option<SecretMetadata>> {
        let row: Option<SecretMetadataRow> = sqlx::query_as(
            "SELECT id, name, provider, provider_ref, status, created_at, updated_at \
             FROM secrets WHERE name = ?",
        )
        .bind(name)
        .fetch_optional(self.backend.pool())
        .await?;

        Ok(row.map(SecretMetadataRow::into_metadata))
    }

    #[tracing::instrument(
        name = "catalog_create_secret_metadata",
        skip(self, metadata),
        fields(db = "sqlite")
    )]
    async fn create_secret_metadata(&self, metadata: &SecretMetadata) -> Result<()> {
        let created_at = metadata.created_at.to_rfc3339();
        let updated_at = metadata.updated_at.to_rfc3339();

        sqlx::query(
            "INSERT INTO secrets (id, name, provider, provider_ref, status, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&metadata.id)
        .bind(&metadata.name)
        .bind(&metadata.provider)
        .bind(&metadata.provider_ref)
        .bind(metadata.status.as_str())
        .bind(&created_at)
        .bind(&updated_at)
        .execute(self.backend.pool())
        .await?;

        Ok(())
    }

    #[tracing::instrument(
        name = "catalog_update_secret_metadata",
        skip(self, metadata, lock),
        fields(db = "sqlite")
    )]
    async fn update_secret_metadata(
        &self,
        metadata: &SecretMetadata,
        lock: Option<OptimisticLock>,
    ) -> Result<bool> {
        use sqlx::QueryBuilder;

        let updated_at = metadata.updated_at.to_rfc3339();

        let mut qb = QueryBuilder::new("UPDATE secrets SET ");
        qb.push("provider = ")
            .push_bind(&metadata.provider)
            .push(", provider_ref = ")
            .push_bind(&metadata.provider_ref)
            .push(", status = ")
            .push_bind(metadata.status.as_str())
            .push(", updated_at = ")
            .push_bind(&updated_at)
            .push(" WHERE id = ")
            .push_bind(&metadata.id);

        if let Some(lock) = lock {
            let expected_created_at = lock.created_at.to_rfc3339();
            qb.push(" AND created_at = ").push_bind(expected_created_at);
        }

        let result = qb.build().execute(self.backend.pool()).await?;
        Ok(result.rows_affected() > 0)
    }

    #[tracing::instrument(name = "catalog_set_secret_status", skip(self), fields(db = "sqlite"))]
    async fn set_secret_status(&self, name: &str, status: SecretStatus) -> Result<bool> {
        let result = sqlx::query("UPDATE secrets SET status = ? WHERE name = ?")
            .bind(status.as_str())
            .bind(name)
            .execute(self.backend.pool())
            .await?;

        Ok(result.rows_affected() > 0)
    }

    #[tracing::instrument(
        name = "catalog_delete_secret_metadata",
        skip(self),
        fields(db = "sqlite")
    )]
    async fn delete_secret_metadata(&self, name: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM secrets WHERE name = ?")
            .bind(name)
            .execute(self.backend.pool())
            .await?;

        Ok(result.rows_affected() > 0)
    }

    #[tracing::instrument(
        name = "catalog_get_encrypted_secret",
        skip(self),
        fields(db = "sqlite")
    )]
    async fn get_encrypted_secret(&self, secret_id: &str) -> Result<Option<Vec<u8>>> {
        sqlx::query_scalar(
            "SELECT encrypted_value FROM encrypted_secret_values WHERE secret_id = ?",
        )
        .bind(secret_id)
        .fetch_optional(self.backend.pool())
        .await
        .map_err(Into::into)
    }

    #[tracing::instrument(
        name = "catalog_put_encrypted_secret_value",
        skip(self, encrypted_value),
        fields(db = "sqlite")
    )]
    async fn put_encrypted_secret_value(
        &self,
        secret_id: &str,
        encrypted_value: &[u8],
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO encrypted_secret_values (secret_id, encrypted_value) \
             VALUES (?, ?) \
             ON CONFLICT (secret_id) DO UPDATE SET \
             encrypted_value = excluded.encrypted_value",
        )
        .bind(secret_id)
        .bind(encrypted_value)
        .execute(self.backend.pool())
        .await?;

        Ok(())
    }

    #[tracing::instrument(
        name = "catalog_delete_encrypted_secret_value",
        skip(self),
        fields(db = "sqlite")
    )]
    async fn delete_encrypted_secret_value(&self, secret_id: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM encrypted_secret_values WHERE secret_id = ?")
            .bind(secret_id)
            .execute(self.backend.pool())
            .await?;

        Ok(result.rows_affected() > 0)
    }

    #[tracing::instrument(name = "catalog_list_secrets", skip(self), fields(db = "sqlite"))]
    async fn list_secrets(&self) -> Result<Vec<SecretMetadata>> {
        let rows: Vec<SecretMetadataRow> = sqlx::query_as(
            "SELECT id, name, provider, provider_ref, status, created_at, updated_at \
             FROM secrets WHERE status = 'active' ORDER BY name",
        )
        .fetch_all(self.backend.pool())
        .await?;

        Ok(rows
            .into_iter()
            .map(SecretMetadataRow::into_metadata)
            .collect())
    }

    #[tracing::instrument(
        name = "catalog_get_secret_metadata_by_id",
        skip(self),
        fields(db = "sqlite")
    )]
    async fn get_secret_metadata_by_id(&self, id: &str) -> Result<Option<SecretMetadata>> {
        let row: Option<SecretMetadataRow> = sqlx::query_as(
            "SELECT id, name, provider, provider_ref, status, created_at, updated_at \
             FROM secrets WHERE id = ? AND status = 'active'",
        )
        .bind(id)
        .fetch_optional(self.backend.pool())
        .await?;

        Ok(row.map(SecretMetadataRow::into_metadata))
    }

    #[tracing::instrument(
        name = "catalog_schedule_file_deletion",
        skip(self),
        fields(db = "sqlite")
    )]
    async fn schedule_file_deletion(&self, path: &str, delete_after: DateTime<Utc>) -> Result<()> {
        // Use RFC3339 string for SQLite TEXT column
        // INSERT OR IGNORE silently ignores duplicates when path already exists
        sqlx::query("INSERT OR IGNORE INTO pending_deletions (path, delete_after) VALUES (?, ?)")
            .bind(path)
            .bind(delete_after.to_rfc3339())
            .execute(self.backend.pool())
            .await?;
        Ok(())
    }

    #[tracing::instrument(
        name = "catalog_get_pending_deletions",
        skip(self),
        fields(db = "sqlite")
    )]
    async fn get_pending_deletions(&self) -> Result<Vec<PendingDeletion>> {
        // Use RFC3339 string comparison for SQLite TEXT column
        let rows: Vec<PendingDeletionRow> = sqlx::query_as(
            "SELECT id, path, delete_after, retry_count FROM pending_deletions WHERE delete_after <= ?",
        )
        .bind(Utc::now().to_rfc3339())
        .fetch_all(self.backend.pool())
        .await?;

        Ok(rows
            .into_iter()
            .map(PendingDeletionRow::into_pending_deletion)
            .collect())
    }

    #[tracing::instrument(
        name = "catalog_increment_deletion_retry",
        skip(self),
        fields(db = "sqlite")
    )]
    async fn increment_deletion_retry(&self, id: i32) -> Result<i32> {
        let new_count: (i32,) = sqlx::query_as(
            "UPDATE pending_deletions SET retry_count = retry_count + 1 WHERE id = ? RETURNING retry_count",
        )
        .bind(id)
        .fetch_one(self.backend.pool())
        .await?;
        Ok(new_count.0)
    }

    #[tracing::instrument(
        name = "catalog_remove_pending_deletion",
        skip(self),
        fields(db = "sqlite")
    )]
    async fn remove_pending_deletion(&self, id: i32) -> Result<()> {
        self.backend.remove_pending_deletion(id).await
    }

    #[tracing::instrument(
        name = "catalog_create_result",
        skip(self),
        fields(db = "sqlite", runtimedb.result_id = tracing::field::Empty)
    )]
    async fn create_result(&self, initial_status: ResultStatus) -> Result<String> {
        let id = crate::id::generate_result_id();
        tracing::Span::current().record("runtimedb.result_id", &id);
        sqlx::query(
            "INSERT INTO results (id, parquet_path, status, error_message, created_at)
             VALUES (?, NULL, ?, NULL, ?)",
        )
        .bind(&id)
        .bind(initial_status.as_str())
        .bind(Utc::now())
        .execute(self.backend.pool())
        .await?;
        Ok(id)
    }

    #[tracing::instrument(
        name = "catalog_update_result",
        skip(self, update),
        fields(db = "sqlite", runtimedb.result_id = %id, rows_affected = tracing::field::Empty)
    )]
    async fn update_result(&self, id: &str, update: ResultUpdate<'_>) -> Result<bool> {
        let result = match update {
            ResultUpdate::Processing => {
                sqlx::query("UPDATE results SET status = 'processing' WHERE id = ?")
                    .bind(id)
                    .execute(self.backend.pool())
                    .await?
            }
            ResultUpdate::Ready { parquet_path } => {
                sqlx::query("UPDATE results SET parquet_path = ?, status = 'ready' WHERE id = ?")
                    .bind(parquet_path)
                    .bind(id)
                    .execute(self.backend.pool())
                    .await?
            }
            ResultUpdate::Failed { error_message } => {
                sqlx::query("UPDATE results SET status = 'failed', error_message = ? WHERE id = ?")
                    .bind(error_message)
                    .bind(id)
                    .execute(self.backend.pool())
                    .await?
            }
        };
        let rows_affected = result.rows_affected();
        tracing::Span::current().record("rows_affected", rows_affected);
        if rows_affected == 0 {
            tracing::warn!(result_id = %id, "update_result: no matching result found");
        }
        Ok(rows_affected > 0)
    }

    #[tracing::instrument(
        name = "catalog_get_result",
        skip(self),
        fields(db = "sqlite", runtimedb.result_id = %id)
    )]
    async fn get_result(&self, id: &str) -> Result<Option<QueryResult>> {
        let row = sqlx::query_as::<_, QueryResultRow>(
            "SELECT id, parquet_path, status, error_message, created_at FROM results WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(self.backend.pool())
        .await?;
        Ok(row.map(QueryResult::from))
    }

    #[tracing::instrument(
        name = "catalog_get_queryable_result",
        skip(self),
        fields(db = "sqlite", runtimedb.result_id = %id)
    )]
    async fn get_queryable_result(&self, id: &str) -> Result<Option<QueryResult>> {
        let row = sqlx::query_as::<_, QueryResultRow>(
            "SELECT id, parquet_path, status, error_message, created_at FROM results WHERE id = ? AND status = 'ready'",
        )
        .bind(id)
        .fetch_optional(self.backend.pool())
        .await?;
        Ok(row.map(QueryResult::from))
    }

    #[tracing::instrument(name = "catalog_list_results", skip(self), fields(db = "sqlite"))]
    async fn list_results(&self, limit: usize, offset: usize) -> Result<(Vec<QueryResult>, bool)> {
        // Fetch one extra to determine has_more
        let fetch_limit = limit + 1;
        let rows = sqlx::query_as::<_, QueryResultRow>(
            "SELECT id, parquet_path, status, error_message, created_at FROM results ORDER BY created_at DESC LIMIT ? OFFSET ?",
        )
        .bind(fetch_limit as i64)
        .bind(offset as i64)
        .fetch_all(self.backend.pool())
        .await?;

        let has_more = rows.len() > limit;
        let results = rows
            .into_iter()
            .take(limit)
            .map(QueryResult::from)
            .collect();
        Ok((results, has_more))
    }

    #[tracing::instrument(
        name = "catalog_cleanup_stale_results",
        skip(self),
        fields(db = "sqlite")
    )]
    async fn cleanup_stale_results(&self, cutoff: DateTime<Utc>) -> Result<usize> {
        let result = sqlx::query(
            "UPDATE results SET status = 'failed', error_message = 'Cleanup: result timed out'
             WHERE status IN ('pending', 'processing') AND created_at < ?",
        )
        .bind(cutoff)
        .execute(self.backend.pool())
        .await?;
        Ok(result.rows_affected() as usize)
    }

    // Query run history methods

    #[tracing::instrument(
        name = "catalog_create_query_run",
        skip(self, params),
        fields(db = "sqlite", runtimedb.query_run_id = %params.id)
    )]
    async fn create_query_run(&self, params: CreateQueryRun<'_>) -> Result<String> {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO query_runs (id, snapshot_id, trace_id, status, saved_query_id, saved_query_version, created_at)
             VALUES (?, ?, ?, 'running', ?, ?, ?)",
        )
        .bind(params.id)
        .bind(params.snapshot_id)
        .bind(params.trace_id)
        .bind(params.saved_query_id)
        .bind(params.saved_query_version)
        .bind(&now)
        .execute(self.backend.pool())
        .await?;
        Ok(params.id.to_string())
    }

    #[tracing::instrument(
        name = "catalog_update_query_run",
        skip(self, update),
        fields(db = "sqlite", runtimedb.query_run_id = %id)
    )]
    async fn update_query_run(&self, id: &str, update: QueryRunUpdate<'_>) -> Result<bool> {
        let now = Utc::now().to_rfc3339();
        let result =
            match update {
                QueryRunUpdate::Succeeded {
                    result_id,
                    row_count,
                    execution_time_ms,
                    warning_message,
                } => sqlx::query(
                    "UPDATE query_runs SET status = 'succeeded', result_id = ?, row_count = ?, \
                     execution_time_ms = ?, warning_message = ?, completed_at = ? WHERE id = ?",
                )
                .bind(result_id)
                .bind(row_count)
                .bind(execution_time_ms)
                .bind(warning_message)
                .bind(&now)
                .bind(id)
                .execute(self.backend.pool())
                .await?,
                QueryRunUpdate::Failed {
                    error_message,
                    execution_time_ms,
                } => {
                    sqlx::query(
                        "UPDATE query_runs SET status = 'failed', error_message = ?, \
                     execution_time_ms = ?, completed_at = ? WHERE id = ?",
                    )
                    .bind(error_message)
                    .bind(execution_time_ms)
                    .bind(&now)
                    .bind(id)
                    .execute(self.backend.pool())
                    .await?
                }
            };
        Ok(result.rows_affected() > 0)
    }

    #[tracing::instrument(name = "catalog_list_query_runs", skip(self), fields(db = "sqlite"))]
    async fn list_query_runs(
        &self,
        limit: usize,
        cursor: Option<&QueryRunCursor>,
    ) -> Result<(Vec<QueryRun>, bool)> {
        let fetch_limit = (limit + 1) as i64;
        let rows: Vec<QueryRunRow> = if let Some(cursor) = cursor {
            let cursor_ts = cursor.created_at.to_rfc3339();
            sqlx::query_as(
                "SELECT qr.id, snap.sql_text, snap.sql_hash, qr.snapshot_id, \
                 qr.trace_id, qr.status, qr.result_id, \
                 qr.error_message, qr.warning_message, qr.row_count, qr.execution_time_ms, \
                 qr.saved_query_id, qr.saved_query_version, qr.created_at, qr.completed_at \
                 FROM query_runs qr \
                 JOIN sql_snapshots snap ON snap.id = qr.snapshot_id \
                 WHERE (qr.created_at < ? OR (qr.created_at = ? AND qr.id < ?)) \
                 ORDER BY qr.created_at DESC, qr.id DESC \
                 LIMIT ?",
            )
            .bind(&cursor_ts)
            .bind(&cursor_ts)
            .bind(&cursor.id)
            .bind(fetch_limit)
            .fetch_all(self.backend.pool())
            .await?
        } else {
            sqlx::query_as(
                "SELECT qr.id, snap.sql_text, snap.sql_hash, qr.snapshot_id, \
                 qr.trace_id, qr.status, qr.result_id, \
                 qr.error_message, qr.warning_message, qr.row_count, qr.execution_time_ms, \
                 qr.saved_query_id, qr.saved_query_version, qr.created_at, qr.completed_at \
                 FROM query_runs qr \
                 JOIN sql_snapshots snap ON snap.id = qr.snapshot_id \
                 ORDER BY qr.created_at DESC, qr.id DESC \
                 LIMIT ?",
            )
            .bind(fetch_limit)
            .fetch_all(self.backend.pool())
            .await?
        };

        let has_more = rows.len() > limit;
        let runs = rows
            .into_iter()
            .take(limit)
            .map(QueryRunRow::into_query_run)
            .collect();
        Ok((runs, has_more))
    }

    #[tracing::instrument(
        name = "catalog_get_query_run",
        skip(self),
        fields(db = "sqlite", runtimedb.query_run_id = %id)
    )]
    async fn get_query_run(&self, id: &str) -> Result<Option<QueryRun>> {
        let row: Option<QueryRunRow> = sqlx::query_as(
            "SELECT qr.id, snap.sql_text, snap.sql_hash, qr.snapshot_id, \
             qr.trace_id, qr.status, qr.result_id, \
             qr.error_message, qr.warning_message, qr.row_count, qr.execution_time_ms, \
             qr.saved_query_id, qr.saved_query_version, qr.created_at, qr.completed_at \
             FROM query_runs qr \
             JOIN sql_snapshots snap ON snap.id = qr.snapshot_id \
             WHERE qr.id = ?",
        )
        .bind(id)
        .fetch_optional(self.backend.pool())
        .await?;
        Ok(row.map(QueryRunRow::into_query_run))
    }

    #[tracing::instrument(
        name = "catalog_cleanup_stale_query_runs",
        skip(self),
        fields(db = "sqlite")
    )]
    async fn cleanup_stale_query_runs(&self, cutoff: DateTime<Utc>) -> Result<usize> {
        let now = Utc::now().to_rfc3339();
        let cutoff_str = cutoff.to_rfc3339();
        let result = sqlx::query(
            "UPDATE query_runs SET status = 'failed', \
             error_message = 'Server interrupted before query completed', \
             completed_at = ? \
             WHERE status = 'running' AND created_at < ?",
        )
        .bind(&now)
        .bind(&cutoff_str)
        .execute(self.backend.pool())
        .await?;
        Ok(result.rows_affected() as usize)
    }

    #[tracing::instrument(
        name = "catalog_count_connections_by_secret_id",
        skip(self),
        fields(db = "sqlite")
    )]
    async fn count_connections_by_secret_id(&self, secret_id: &str) -> Result<i64> {
        self.backend.count_connections_by_secret_id(secret_id).await
    }

    // Upload management methods

    #[tracing::instrument(
        name = "catalog_create_upload",
        skip(self, upload),
        fields(db = "sqlite")
    )]
    async fn create_upload(&self, upload: &UploadInfo) -> Result<()> {
        let created_at = upload.created_at.to_rfc3339();
        let consumed_at = upload.consumed_at.map(|t| t.to_rfc3339());

        sqlx::query(
            "INSERT INTO uploads (id, status, storage_url, content_type, content_encoding, size_bytes, created_at, consumed_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&upload.id)
        .bind(&upload.status)
        .bind(&upload.storage_url)
        .bind(&upload.content_type)
        .bind(&upload.content_encoding)
        .bind(upload.size_bytes)
        .bind(&created_at)
        .bind(&consumed_at)
        .execute(self.backend.pool())
        .await?;

        Ok(())
    }

    #[tracing::instrument(name = "catalog_get_upload", skip(self), fields(db = "sqlite"))]
    async fn get_upload(&self, id: &str) -> Result<Option<UploadInfo>> {
        let row: Option<UploadInfoRow> = sqlx::query_as(
            "SELECT id, status, storage_url, content_type, content_encoding, size_bytes, created_at, consumed_at \
             FROM uploads WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(self.backend.pool())
        .await?;

        Ok(row.map(UploadInfoRow::into_upload_info))
    }

    #[tracing::instrument(name = "catalog_list_uploads", skip(self), fields(db = "sqlite"))]
    async fn list_uploads(&self, status: Option<&str>) -> Result<Vec<UploadInfo>> {
        let rows: Vec<UploadInfoRow> = if let Some(status) = status {
            sqlx::query_as(
                "SELECT id, status, storage_url, content_type, content_encoding, size_bytes, created_at, consumed_at \
                 FROM uploads WHERE status = ? ORDER BY created_at DESC",
            )
            .bind(status)
            .fetch_all(self.backend.pool())
            .await?
        } else {
            sqlx::query_as(
                "SELECT id, status, storage_url, content_type, content_encoding, size_bytes, created_at, consumed_at \
                 FROM uploads ORDER BY created_at DESC",
            )
            .fetch_all(self.backend.pool())
            .await?
        };

        Ok(rows
            .into_iter()
            .map(UploadInfoRow::into_upload_info)
            .collect())
    }

    #[tracing::instrument(name = "catalog_consume_upload", skip(self), fields(db = "sqlite"))]
    async fn consume_upload(&self, id: &str) -> Result<bool> {
        let consumed_at = Utc::now().to_rfc3339();
        // Accept both pending and processing states (processing is the expected state after claim)
        let result = sqlx::query(
            "UPDATE uploads SET status = 'consumed', consumed_at = ? WHERE id = ? AND status IN ('pending', 'processing')",
        )
        .bind(&consumed_at)
        .bind(id)
        .execute(self.backend.pool())
        .await?;

        Ok(result.rows_affected() > 0)
    }

    #[tracing::instrument(name = "catalog_claim_upload", skip(self), fields(db = "sqlite"))]
    async fn claim_upload(&self, id: &str) -> Result<bool> {
        let result = sqlx::query(
            "UPDATE uploads SET status = 'processing' WHERE id = ? AND status = 'pending'",
        )
        .bind(id)
        .execute(self.backend.pool())
        .await?;

        Ok(result.rows_affected() > 0)
    }

    #[tracing::instrument(name = "catalog_release_upload", skip(self), fields(db = "sqlite"))]
    async fn release_upload(&self, id: &str) -> Result<bool> {
        let result = sqlx::query(
            "UPDATE uploads SET status = 'pending' WHERE id = ? AND status = 'processing'",
        )
        .bind(id)
        .execute(self.backend.pool())
        .await?;

        Ok(result.rows_affected() > 0)
    }

    // SQL snapshot methods

    #[tracing::instrument(
        name = "catalog_get_or_create_snapshot",
        skip(self, sql_text),
        fields(db = "sqlite")
    )]
    async fn get_or_create_snapshot(&self, sql_text: &str) -> Result<SqlSnapshot> {
        let hash = crate::catalog::manager::sql_hash(sql_text);
        let id = crate::id::generate_snapshot_id();
        let now = Utc::now().to_rfc3339();

        // Use a single connection so the INSERT OR IGNORE and SELECT are
        // guaranteed to see the same row, even if a concurrent DELETE were
        // to run between them (snapshots are never deleted, but this is
        // defensive).
        let mut conn = self.backend.pool().acquire().await?;

        // INSERT OR IGNORE for idempotent upsert (race-safe via UNIQUE constraint)
        sqlx::query(
            "INSERT OR IGNORE INTO sql_snapshots (id, sql_hash, sql_text, created_at) \
             VALUES (?, ?, ?, ?)",
        )
        .bind(&id)
        .bind(&hash)
        .bind(sql_text)
        .bind(&now)
        .execute(&mut *conn)
        .await?;

        // Fetch the existing or newly inserted row
        let row: SqlSnapshotRow = sqlx::query_as(
            "SELECT id, sql_hash, sql_text, created_at FROM sql_snapshots \
             WHERE sql_hash = ? AND sql_text = ?",
        )
        .bind(&hash)
        .bind(sql_text)
        .fetch_one(&mut *conn)
        .await?;

        Ok(row.into_sql_snapshot())
    }

    // Saved query methods

    #[tracing::instrument(name = "catalog_create_saved_query", skip(self), fields(db = "sqlite"))]
    async fn create_saved_query(&self, name: &str, snapshot_id: &str) -> Result<SavedQuery> {
        let id = crate::id::generate_saved_query_id();
        let now = Utc::now().to_rfc3339();

        // Use BEGIN IMMEDIATE to acquire a RESERVED lock up front, preventing
        // concurrent writers from interleaving reads before locks are held.
        let mut conn = self.backend.pool().acquire().await?;
        sqlx::query("BEGIN IMMEDIATE").execute(&mut *conn).await?;

        let result: Result<SavedQuery> = async {
            sqlx::query(
                "INSERT INTO saved_queries (id, name, latest_version, created_at, updated_at) \
                 VALUES (?, ?, 1, ?, ?)",
            )
            .bind(&id)
            .bind(name)
            .bind(&now)
            .bind(&now)
            .execute(&mut *conn)
            .await?;

            sqlx::query(
                "INSERT INTO saved_query_versions (saved_query_id, version, snapshot_id, created_at) \
                 VALUES (?, 1, ?, ?)",
            )
            .bind(&id)
            .bind(snapshot_id)
            .bind(&now)
            .execute(&mut *conn)
            .await?;

            let row: SavedQueryRow = sqlx::query_as(
                "SELECT id, name, latest_version, created_at, updated_at \
                 FROM saved_queries WHERE id = ?",
            )
            .bind(&id)
            .fetch_one(&mut *conn)
            .await?;

            Ok(row.into_saved_query())
        }
        .await;

        match &result {
            Ok(_) => {
                sqlx::query("COMMIT").execute(&mut *conn).await?;
            }
            Err(_) => {
                let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
            }
        }

        result
    }

    #[tracing::instrument(
        name = "catalog_get_saved_query",
        skip(self),
        fields(db = "sqlite", runtimedb.saved_query_id = %id)
    )]
    async fn get_saved_query(&self, id: &str) -> Result<Option<SavedQuery>> {
        let row: Option<SavedQueryRow> = sqlx::query_as(
            "SELECT id, name, latest_version, created_at, updated_at \
             FROM saved_queries WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(self.backend.pool())
        .await?;

        Ok(row.map(SavedQueryRow::into_saved_query))
    }

    #[tracing::instrument(name = "catalog_list_saved_queries", skip(self), fields(db = "sqlite"))]
    async fn list_saved_queries(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<SavedQuery>, bool)> {
        let fetch_limit = i64::try_from(limit.saturating_add(1)).unwrap_or(i64::MAX);
        let rows: Vec<SavedQueryRow> = sqlx::query_as(
            "SELECT id, name, latest_version, created_at, updated_at \
             FROM saved_queries \
             ORDER BY name ASC, id ASC \
             LIMIT ? OFFSET ?",
        )
        .bind(fetch_limit)
        .bind(i64::try_from(offset).unwrap_or(i64::MAX))
        .fetch_all(self.backend.pool())
        .await?;

        let has_more = rows.len() > limit;
        let queries = rows
            .into_iter()
            .take(limit)
            .map(SavedQueryRow::into_saved_query)
            .collect();
        Ok((queries, has_more))
    }

    #[tracing::instrument(
        name = "catalog_update_saved_query",
        skip(self),
        fields(db = "sqlite", runtimedb.saved_query_id = %id)
    )]
    async fn update_saved_query(
        &self,
        id: &str,
        name: Option<&str>,
        snapshot_id: &str,
    ) -> Result<Option<SavedQuery>> {
        // Use BEGIN IMMEDIATE to acquire a RESERVED lock up front, so the
        // read of latest_version and subsequent insert of the next version are
        // atomic. A deferred BEGIN would only take a SHARED lock on the read,
        // allowing two concurrent updates to read the same latest_version and
        // race on the (saved_query_id, version) primary key.
        let mut conn = self.backend.pool().acquire().await?;
        sqlx::query("BEGIN IMMEDIATE").execute(&mut *conn).await?;

        let result: Result<Option<SavedQuery>> = async {
            let existing: Option<SavedQueryRow> = sqlx::query_as(
                "SELECT id, name, latest_version, created_at, updated_at \
                 FROM saved_queries WHERE id = ?",
            )
            .bind(id)
            .fetch_optional(&mut *conn)
            .await?;

            let existing = match existing {
                Some(row) => row,
                None => return Ok(None),
            };

            let effective_name = name.unwrap_or(&existing.name);

            // Check if the latest version already points to the same snapshot
            // and the name is unchanged — skip creating a redundant version.
            let current_snapshot: Option<(String,)> = sqlx::query_as(
                "SELECT snapshot_id FROM saved_query_versions \
                 WHERE saved_query_id = ? AND version = ?",
            )
            .bind(id)
            .bind(existing.latest_version)
            .fetch_optional(&mut *conn)
            .await?;

            let sql_unchanged = current_snapshot
                .as_ref()
                .is_some_and(|(sid,)| sid == snapshot_id);
            let name_unchanged = effective_name == existing.name;

            if sql_unchanged && name_unchanged {
                // Complete no-op — nothing to change
                return Ok(Some(existing.into_saved_query()));
            }

            let now = Utc::now().to_rfc3339();

            if sql_unchanged {
                // Name-only rename — no new version needed
                sqlx::query(
                    "UPDATE saved_queries SET name = ?, updated_at = ? WHERE id = ?",
                )
                .bind(effective_name)
                .bind(&now)
                .bind(id)
                .execute(&mut *conn)
                .await?;
            } else {
                // SQL changed — create a new version
                let new_version = existing.latest_version.checked_add(1).ok_or_else(|| {
                    anyhow::anyhow!("Version limit reached for saved query '{}'", id)
                })?;

                sqlx::query(
                    "INSERT INTO saved_query_versions (saved_query_id, version, snapshot_id, created_at) \
                     VALUES (?, ?, ?, ?)",
                )
                .bind(id)
                .bind(new_version)
                .bind(snapshot_id)
                .bind(&now)
                .execute(&mut *conn)
                .await?;

                sqlx::query(
                    "UPDATE saved_queries SET name = ?, latest_version = ?, updated_at = ? WHERE id = ?",
                )
                .bind(effective_name)
                .bind(new_version)
                .bind(&now)
                .bind(id)
                .execute(&mut *conn)
                .await?;
            }

            let row: SavedQueryRow = sqlx::query_as(
                "SELECT id, name, latest_version, created_at, updated_at \
                 FROM saved_queries WHERE id = ?",
            )
            .bind(id)
            .fetch_one(&mut *conn)
            .await?;

            Ok(Some(row.into_saved_query()))
        }
        .await;

        match &result {
            Ok(_) => {
                sqlx::query("COMMIT").execute(&mut *conn).await?;
            }
            Err(_) => {
                let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
            }
        }

        result
    }

    #[tracing::instrument(
        name = "catalog_delete_saved_query",
        skip(self),
        fields(db = "sqlite", runtimedb.saved_query_id = %id)
    )]
    async fn delete_saved_query(&self, id: &str) -> Result<bool> {
        // Use BEGIN IMMEDIATE for consistency with other write transactions.
        let mut conn = self.backend.pool().acquire().await?;
        sqlx::query("BEGIN IMMEDIATE").execute(&mut *conn).await?;

        let result: Result<bool> = async {
            // Explicitly delete child versions first (SQLite PRAGMA foreign_keys is
            // not guaranteed to be ON, so we cannot rely on ON DELETE CASCADE).
            // Query runs retain history via their snapshot_id FK to sql_snapshots.
            sqlx::query("DELETE FROM saved_query_versions WHERE saved_query_id = ?")
                .bind(id)
                .execute(&mut *conn)
                .await?;

            let del = sqlx::query("DELETE FROM saved_queries WHERE id = ?")
                .bind(id)
                .execute(&mut *conn)
                .await?;

            Ok(del.rows_affected() > 0)
        }
        .await;

        match &result {
            Ok(_) => {
                sqlx::query("COMMIT").execute(&mut *conn).await?;
            }
            Err(_) => {
                let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
            }
        }

        result
    }

    #[tracing::instrument(
        name = "catalog_get_saved_query_version",
        skip(self),
        fields(db = "sqlite", runtimedb.saved_query_id = %saved_query_id)
    )]
    async fn get_saved_query_version(
        &self,
        saved_query_id: &str,
        version: i32,
    ) -> Result<Option<SavedQueryVersion>> {
        let row: Option<SavedQueryVersionRow> = sqlx::query_as(
            "SELECT sqv.saved_query_id, sqv.version, sqv.snapshot_id, \
             snap.sql_text, snap.sql_hash, sqv.created_at \
             FROM saved_query_versions sqv \
             JOIN sql_snapshots snap ON snap.id = sqv.snapshot_id \
             WHERE sqv.saved_query_id = ? AND sqv.version = ?",
        )
        .bind(saved_query_id)
        .bind(version)
        .fetch_optional(self.backend.pool())
        .await?;

        Ok(row.map(SavedQueryVersionRow::into_saved_query_version))
    }

    #[tracing::instrument(
        name = "catalog_list_saved_query_versions",
        skip(self),
        fields(db = "sqlite", runtimedb.saved_query_id = %saved_query_id)
    )]
    async fn list_saved_query_versions(
        &self,
        saved_query_id: &str,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<SavedQueryVersion>, bool)> {
        let fetch_limit = i64::try_from(limit.saturating_add(1)).unwrap_or(i64::MAX);
        let rows: Vec<SavedQueryVersionRow> = sqlx::query_as(
            "SELECT sqv.saved_query_id, sqv.version, sqv.snapshot_id, \
             snap.sql_text, snap.sql_hash, sqv.created_at \
             FROM saved_query_versions sqv \
             JOIN sql_snapshots snap ON snap.id = sqv.snapshot_id \
             WHERE sqv.saved_query_id = ? \
             ORDER BY sqv.version DESC \
             LIMIT ? OFFSET ?",
        )
        .bind(saved_query_id)
        .bind(fetch_limit)
        .bind(i64::try_from(offset).unwrap_or(i64::MAX))
        .fetch_all(self.backend.pool())
        .await?;

        let has_more = rows.len() > limit;
        let versions = rows
            .into_iter()
            .take(limit)
            .map(SavedQueryVersionRow::into_saved_query_version)
            .collect();

        Ok((versions, has_more))
    }

    // Dataset management methods

    #[tracing::instrument(
        name = "catalog_create_dataset",
        skip(self, dataset),
        fields(db = "sqlite", runtimedb.dataset_id = %dataset.id)
    )]
    async fn create_dataset(&self, dataset: &DatasetInfo) -> Result<()> {
        let created_at = dataset.created_at.to_rfc3339();
        let updated_at = dataset.updated_at.to_rfc3339();

        sqlx::query(
            "INSERT INTO datasets (id, label, schema_name, table_name, parquet_url, arrow_schema_json, source_type, source_config, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&dataset.id)
        .bind(&dataset.label)
        .bind(&dataset.schema_name)
        .bind(&dataset.table_name)
        .bind(&dataset.parquet_url)
        .bind(&dataset.arrow_schema_json)
        .bind(&dataset.source_type)
        .bind(&dataset.source_config)
        .bind(&created_at)
        .bind(&updated_at)
        .execute(self.backend.pool())
        .await?;

        Ok(())
    }

    #[tracing::instrument(
        name = "catalog_get_dataset",
        skip(self),
        fields(db = "sqlite", runtimedb.dataset_id = %id)
    )]
    async fn get_dataset(&self, id: &str) -> Result<Option<DatasetInfo>> {
        let row: Option<DatasetInfoRow> = sqlx::query_as(
            "SELECT id, label, schema_name, table_name, parquet_url, arrow_schema_json, source_type, source_config, created_at, updated_at \
             FROM datasets WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(self.backend.pool())
        .await?;

        Ok(row.map(DatasetInfoRow::into_dataset_info))
    }

    #[tracing::instrument(
        name = "catalog_get_dataset_by_table_name",
        skip(self),
        fields(db = "sqlite")
    )]
    async fn get_dataset_by_table_name(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<DatasetInfo>> {
        let row: Option<DatasetInfoRow> = sqlx::query_as(
            "SELECT id, label, schema_name, table_name, parquet_url, arrow_schema_json, source_type, source_config, created_at, updated_at \
             FROM datasets WHERE schema_name = ? AND table_name = ?",
        )
        .bind(schema_name)
        .bind(table_name)
        .fetch_optional(self.backend.pool())
        .await?;

        Ok(row.map(DatasetInfoRow::into_dataset_info))
    }

    #[tracing::instrument(
        name = "catalog_list_datasets",
        skip(self),
        fields(db = "sqlite", runtimedb.count = tracing::field::Empty)
    )]
    async fn list_datasets(&self, limit: usize, offset: usize) -> Result<(Vec<DatasetInfo>, bool)> {
        // Fetch one extra to determine if there are more results
        // Use saturating_add to prevent overflow when limit is very large
        let fetch_limit = limit.saturating_add(1);
        let rows: Vec<DatasetInfoRow> = sqlx::query_as(
            "SELECT id, label, schema_name, table_name, parquet_url, arrow_schema_json, source_type, source_config, created_at, updated_at \
             FROM datasets ORDER BY label LIMIT ? OFFSET ?",
        )
        .bind(fetch_limit as i64)
        .bind(offset as i64)
        .fetch_all(self.backend.pool())
        .await?;

        let has_more = rows.len() > limit;
        let datasets: Vec<DatasetInfo> = rows
            .into_iter()
            .take(limit)
            .map(DatasetInfoRow::into_dataset_info)
            .collect();

        tracing::Span::current().record("runtimedb.count", datasets.len());
        Ok((datasets, has_more))
    }

    #[tracing::instrument(name = "catalog_list_all_datasets", skip(self), fields(db = "sqlite"))]
    async fn list_all_datasets(&self) -> Result<Vec<DatasetInfo>> {
        let rows: Vec<DatasetInfoRow> = sqlx::query_as(
            "SELECT id, label, schema_name, table_name, parquet_url, arrow_schema_json, source_type, source_config, created_at, updated_at \
             FROM datasets ORDER BY label",
        )
        .fetch_all(self.backend.pool())
        .await?;

        Ok(rows
            .into_iter()
            .map(DatasetInfoRow::into_dataset_info)
            .collect())
    }

    #[tracing::instrument(
        name = "catalog_list_dataset_table_names",
        skip(self),
        fields(db = "sqlite")
    )]
    async fn list_dataset_table_names(&self, schema_name: &str) -> Result<Vec<String>> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT table_name FROM datasets WHERE schema_name = ? ORDER BY table_name",
        )
        .bind(schema_name)
        .fetch_all(self.backend.pool())
        .await?;

        Ok(rows.into_iter().map(|(name,)| name).collect())
    }

    #[tracing::instrument(name = "catalog_update_dataset", skip(self), fields(db = "sqlite"))]
    async fn update_dataset(&self, id: &str, label: &str, table_name: &str) -> Result<bool> {
        let updated_at = Utc::now().to_rfc3339();
        let result = sqlx::query(
            "UPDATE datasets SET label = ?, table_name = ?, updated_at = ? WHERE id = ?",
        )
        .bind(label)
        .bind(table_name)
        .bind(&updated_at)
        .bind(id)
        .execute(self.backend.pool())
        .await?;

        Ok(result.rows_affected() > 0)
    }

    #[tracing::instrument(name = "catalog_delete_dataset", skip(self), fields(db = "sqlite"))]
    async fn delete_dataset(&self, id: &str) -> Result<Option<DatasetInfo>> {
        // First get the dataset so we can return it
        let dataset = self.get_dataset(id).await?;

        if dataset.is_some() {
            sqlx::query("DELETE FROM datasets WHERE id = ?")
                .bind(id)
                .execute(self.backend.pool())
                .await?;
        }

        Ok(dataset)
    }
}

impl CatalogMigrations for SqliteMigrationBackend {
    type Pool = SqlitePool;

    fn migrations() -> &'static [Migration] {
        SQLITE_MIGRATIONS
    }

    async fn ensure_migrations_table(pool: &Self::Pool) -> Result<()> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                hash TEXT NOT NULL,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )",
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn get_applied_migrations(pool: &Self::Pool) -> Result<Vec<(i64, String)>> {
        let rows: Vec<(i64, String)> =
            sqlx::query_as("SELECT version, hash FROM schema_migrations ORDER BY version")
                .fetch_all(pool)
                .await?;
        Ok(rows)
    }

    async fn apply_migration(pool: &Self::Pool, version: i64, hash: &str, sql: &str) -> Result<()> {
        let wrapped_sql = wrap_migration_sql(sql, version, hash);
        sqlx::raw_sql(&wrapped_sql).execute(pool).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[tokio::test]
    async fn test_pending_deletions_crud() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let manager = SqliteCatalogManager::new(db_path.to_str().unwrap())
            .await
            .unwrap();
        manager.run_migrations().await.unwrap();

        // Schedule a deletion in the past (should be due immediately)
        let past = Utc::now() - chrono::Duration::seconds(10);
        manager
            .schedule_file_deletion("/tmp/test.parquet", past)
            .await
            .unwrap();

        // Get due deletions
        let due = manager.get_pending_deletions().await.unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].path, "/tmp/test.parquet");

        // Remove the deletion
        manager.remove_pending_deletion(due[0].id).await.unwrap();

        // Verify it's gone
        let due = manager.get_pending_deletions().await.unwrap();
        assert!(due.is_empty());
    }

    #[tokio::test]
    async fn test_pending_deletions_respects_time() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let manager = SqliteCatalogManager::new(db_path.to_str().unwrap())
            .await
            .unwrap();
        manager.run_migrations().await.unwrap();

        // Schedule a deletion in the future
        let future = Utc::now() + chrono::Duration::seconds(3600);
        manager
            .schedule_file_deletion("/tmp/future.parquet", future)
            .await
            .unwrap();

        // Should not be due yet
        let due = manager.get_pending_deletions().await.unwrap();
        assert!(due.is_empty());
    }

    #[tokio::test]
    async fn test_pending_deletions_timestamp_roundtrip() {
        use chrono::TimeZone;

        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let manager = SqliteCatalogManager::new(db_path.to_str().unwrap())
            .await
            .unwrap();
        manager.run_migrations().await.unwrap();

        // Use a specific timestamp in the past to verify round-trip
        let specific_time = Utc.with_ymd_and_hms(2024, 6, 15, 12, 30, 45).unwrap();
        manager
            .schedule_file_deletion("/tmp/roundtrip.parquet", specific_time)
            .await
            .unwrap();

        // Since specific_time is in the past, get_pending_deletions should return it
        let pending = manager.get_pending_deletions().await.unwrap();

        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].path, "/tmp/roundtrip.parquet");

        // Verify the timestamp matches (within second precision due to RFC3339)
        let stored_time = pending[0].delete_after;
        assert_eq!(
            stored_time.timestamp(),
            specific_time.timestamp(),
            "Stored timestamp should match original"
        );
    }

    #[tokio::test]
    async fn test_pending_deletions_duplicate_path_ignored() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let manager = SqliteCatalogManager::new(db_path.to_str().unwrap())
            .await
            .unwrap();
        manager.run_migrations().await.unwrap();

        // Schedule deletion for a path
        let past = Utc::now() - chrono::Duration::seconds(10);
        manager
            .schedule_file_deletion("/tmp/duplicate.parquet", past)
            .await
            .unwrap();

        // Try to schedule the same path again (should be ignored, not error)
        let later = Utc::now() - chrono::Duration::seconds(5);
        manager
            .schedule_file_deletion("/tmp/duplicate.parquet", later)
            .await
            .unwrap();

        // Should only have one record for this path
        let due = manager.get_pending_deletions().await.unwrap();
        let matching: Vec<_> = due
            .iter()
            .filter(|d| d.path == "/tmp/duplicate.parquet")
            .collect();
        assert_eq!(
            matching.len(),
            1,
            "Should only have one pending deletion for the same path"
        );
    }
}
