use crate::catalog::backend::CatalogBackend;
use crate::catalog::manager::{
    CatalogManager, ConnectionInfo, CreateQueryRun, DatasetInfo, OptimisticLock, PendingDeletion,
    QueryResult, QueryResultRow, QueryRun, QueryRunCursor, QueryRunRowPg, QueryRunUpdate,
    ResultStatus, ResultUpdate, SavedQuery, SavedQueryRowPg, SavedQueryVersion,
    SavedQueryVersionRowPg, SqlSnapshot, SqlSnapshotRowPg, TableInfo, UploadInfo,
};
use crate::catalog::migrations::{
    run_migrations, wrap_migration_sql, CatalogMigrations, Migration, POSTGRES_MIGRATIONS,
};
use crate::secrets::{SecretMetadata, SecretStatus};
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres};
use std::fmt::{self, Debug, Formatter};
use std::str::FromStr;

pub struct PostgresCatalogManager {
    backend: CatalogBackend<Postgres>,
}

/// Row type for secret metadata queries (Postgres handles timestamps natively)
#[derive(sqlx::FromRow)]
struct SecretMetadataRow {
    id: String,
    name: String,
    provider: String,
    provider_ref: Option<String>,
    status: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl SecretMetadataRow {
    fn into_metadata(self) -> SecretMetadata {
        SecretMetadata {
            id: self.id,
            name: self.name,
            provider: self.provider,
            provider_ref: self.provider_ref,
            status: SecretStatus::from_str(&self.status).unwrap_or(SecretStatus::Active),
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}

/// Row type for upload queries (Postgres handles timestamps natively)
#[derive(sqlx::FromRow)]
struct UploadInfoRow {
    id: String,
    status: String,
    storage_url: String,
    content_type: Option<String>,
    content_encoding: Option<String>,
    size_bytes: i64,
    created_at: DateTime<Utc>,
    consumed_at: Option<DateTime<Utc>>,
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
            created_at: self.created_at,
            consumed_at: self.consumed_at,
        }
    }
}

/// Row type for dataset queries (Postgres handles timestamps natively)
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
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
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
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}

impl PostgresCatalogManager {
    pub async fn new(connection_string: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(connection_string)
            .await?;

        let backend = CatalogBackend::new(pool);
        Ok(Self { backend })
    }
}

struct PostgresMigrationBackend;

#[async_trait]
impl CatalogManager for PostgresCatalogManager {
    #[tracing::instrument(name = "catalog_run_migrations", skip(self), fields(db = "postgres"))]
    async fn run_migrations(&self) -> Result<()> {
        run_migrations::<PostgresMigrationBackend>(self.backend.pool()).await
    }

    #[tracing::instrument(name = "catalog_list_connections", skip(self), fields(db = "postgres"))]
    async fn list_connections(&self) -> Result<Vec<ConnectionInfo>> {
        self.backend.list_connections().await
    }

    #[tracing::instrument(
        name = "catalog_add_connection",
        skip(self, config_json),
        fields(db = "postgres")
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

    #[tracing::instrument(name = "catalog_get_connection", skip(self), fields(db = "postgres"))]
    async fn get_connection(&self, id: &str) -> Result<Option<ConnectionInfo>> {
        self.backend.get_connection(id).await
    }

    #[tracing::instrument(
        name = "catalog_get_connection_by_name",
        skip(self),
        fields(db = "postgres")
    )]
    async fn get_connection_by_name(&self, name: &str) -> Result<Option<ConnectionInfo>> {
        self.backend.get_connection_by_name(name).await
    }

    #[tracing::instrument(
        name = "catalog_add_table",
        skip(self, arrow_schema_json),
        fields(db = "postgres")
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

    #[tracing::instrument(name = "catalog_list_tables", skip(self), fields(db = "postgres"))]
    async fn list_tables(&self, connection_id: Option<&str>) -> Result<Vec<TableInfo>> {
        self.backend.list_tables(connection_id).await
    }

    #[tracing::instrument(name = "catalog_get_table", skip(self), fields(db = "postgres"))]
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

    #[tracing::instrument(
        name = "catalog_update_table_sync",
        skip(self),
        fields(db = "postgres")
    )]
    async fn update_table_sync(&self, table_id: i32, parquet_path: &str) -> Result<()> {
        self.backend.update_table_sync(table_id, parquet_path).await
    }

    #[tracing::instrument(
        name = "catalog_clear_table_cache_metadata",
        skip(self),
        fields(db = "postgres")
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
        fields(db = "postgres")
    )]
    async fn clear_connection_cache_metadata(&self, connection_id: &str) -> Result<()> {
        self.backend
            .clear_connection_cache_metadata(connection_id)
            .await
    }

    #[tracing::instrument(
        name = "catalog_delete_connection",
        skip(self),
        fields(db = "postgres")
    )]
    async fn delete_connection(&self, connection_id: &str) -> Result<()> {
        self.backend.delete_connection(connection_id).await
    }

    #[tracing::instrument(
        name = "catalog_schedule_file_deletion",
        skip(self),
        fields(db = "postgres")
    )]
    async fn schedule_file_deletion(&self, path: &str, delete_after: DateTime<Utc>) -> Result<()> {
        // Use native TIMESTAMPTZ binding for Postgres (not RFC3339 string)
        // ON CONFLICT DO NOTHING silently ignores duplicates when path already exists
        sqlx::query(
            "INSERT INTO pending_deletions (path, delete_after) VALUES ($1, $2) ON CONFLICT (path) DO NOTHING",
        )
        .bind(path)
        .bind(delete_after)
        .execute(self.backend.pool())
        .await?;
        Ok(())
    }

    #[tracing::instrument(
        name = "catalog_get_pending_deletions",
        skip(self),
        fields(db = "postgres")
    )]
    async fn get_pending_deletions(&self) -> Result<Vec<PendingDeletion>> {
        // Use native TIMESTAMPTZ comparison for Postgres (not RFC3339 string)
        sqlx::query_as(
            "SELECT id, path, delete_after, retry_count FROM pending_deletions WHERE delete_after <= $1",
        )
        .bind(Utc::now())
        .fetch_all(self.backend.pool())
        .await
        .map_err(Into::into)
    }

    #[tracing::instrument(
        name = "catalog_increment_deletion_retry",
        skip(self),
        fields(db = "postgres")
    )]
    async fn increment_deletion_retry(&self, id: i32) -> Result<i32> {
        let new_count: (i32,) = sqlx::query_as(
            "UPDATE pending_deletions SET retry_count = retry_count + 1 WHERE id = $1 RETURNING retry_count",
        )
        .bind(id)
        .fetch_one(self.backend.pool())
        .await?;
        Ok(new_count.0)
    }

    #[tracing::instrument(
        name = "catalog_remove_pending_deletion",
        skip(self),
        fields(db = "postgres")
    )]
    async fn remove_pending_deletion(&self, id: i32) -> Result<()> {
        self.backend.remove_pending_deletion(id).await
    }

    #[tracing::instrument(
        name = "catalog_get_secret_metadata",
        skip(self),
        fields(db = "postgres")
    )]
    async fn get_secret_metadata(&self, name: &str) -> Result<Option<SecretMetadata>> {
        let row: Option<SecretMetadataRow> = sqlx::query_as(
            "SELECT id, name, provider, provider_ref, status, created_at, updated_at \
             FROM secrets WHERE name = $1 AND status = 'active'",
        )
        .bind(name)
        .fetch_optional(self.backend.pool())
        .await?;

        Ok(row.map(SecretMetadataRow::into_metadata))
    }

    #[tracing::instrument(
        name = "catalog_get_secret_metadata_any_status",
        skip(self),
        fields(db = "postgres")
    )]
    async fn get_secret_metadata_any_status(&self, name: &str) -> Result<Option<SecretMetadata>> {
        let row: Option<SecretMetadataRow> = sqlx::query_as(
            "SELECT id, name, provider, provider_ref, status, created_at, updated_at \
             FROM secrets WHERE name = $1",
        )
        .bind(name)
        .fetch_optional(self.backend.pool())
        .await?;

        Ok(row.map(SecretMetadataRow::into_metadata))
    }

    #[tracing::instrument(
        name = "catalog_create_secret_metadata",
        skip(self, metadata),
        fields(db = "postgres")
    )]
    async fn create_secret_metadata(&self, metadata: &SecretMetadata) -> Result<()> {
        sqlx::query(
            "INSERT INTO secrets (id, name, provider, provider_ref, status, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(&metadata.id)
        .bind(&metadata.name)
        .bind(&metadata.provider)
        .bind(&metadata.provider_ref)
        .bind(metadata.status.as_str())
        .bind(metadata.created_at)
        .bind(metadata.updated_at)
        .execute(self.backend.pool())
        .await?;

        Ok(())
    }

    #[tracing::instrument(
        name = "catalog_update_secret_metadata",
        skip(self, metadata, lock),
        fields(db = "postgres")
    )]
    async fn update_secret_metadata(
        &self,
        metadata: &SecretMetadata,
        lock: Option<OptimisticLock>,
    ) -> Result<bool> {
        use sqlx::QueryBuilder;

        let mut qb = QueryBuilder::new("UPDATE secrets SET ");
        qb.push("provider = ")
            .push_bind(&metadata.provider)
            .push(", provider_ref = ")
            .push_bind(&metadata.provider_ref)
            .push(", status = ")
            .push_bind(metadata.status.as_str())
            .push(", updated_at = ")
            .push_bind(metadata.updated_at)
            .push(" WHERE id = ")
            .push_bind(&metadata.id);

        if let Some(lock) = lock {
            qb.push(" AND created_at = ").push_bind(lock.created_at);
        }

        let result = qb.build().execute(self.backend.pool()).await?;
        Ok(result.rows_affected() > 0)
    }

    #[tracing::instrument(
        name = "catalog_set_secret_status",
        skip(self),
        fields(db = "postgres")
    )]
    async fn set_secret_status(&self, name: &str, status: SecretStatus) -> Result<bool> {
        let result = sqlx::query("UPDATE secrets SET status = $1 WHERE name = $2")
            .bind(status.as_str())
            .bind(name)
            .execute(self.backend.pool())
            .await?;

        Ok(result.rows_affected() > 0)
    }

    #[tracing::instrument(
        name = "catalog_delete_secret_metadata",
        skip(self),
        fields(db = "postgres")
    )]
    async fn delete_secret_metadata(&self, name: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM secrets WHERE name = $1")
            .bind(name)
            .execute(self.backend.pool())
            .await?;

        Ok(result.rows_affected() > 0)
    }

    #[tracing::instrument(
        name = "catalog_get_encrypted_secret",
        skip(self),
        fields(db = "postgres")
    )]
    async fn get_encrypted_secret(&self, secret_id: &str) -> Result<Option<Vec<u8>>> {
        sqlx::query_scalar(
            "SELECT encrypted_value FROM encrypted_secret_values WHERE secret_id = $1",
        )
        .bind(secret_id)
        .fetch_optional(self.backend.pool())
        .await
        .map_err(Into::into)
    }

    #[tracing::instrument(
        name = "catalog_put_encrypted_secret_value",
        skip(self, encrypted_value),
        fields(db = "postgres")
    )]
    async fn put_encrypted_secret_value(
        &self,
        secret_id: &str,
        encrypted_value: &[u8],
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO encrypted_secret_values (secret_id, encrypted_value) \
             VALUES ($1, $2) \
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
        fields(db = "postgres")
    )]
    async fn delete_encrypted_secret_value(&self, secret_id: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM encrypted_secret_values WHERE secret_id = $1")
            .bind(secret_id)
            .execute(self.backend.pool())
            .await?;

        Ok(result.rows_affected() > 0)
    }

    #[tracing::instrument(name = "catalog_list_secrets", skip(self), fields(db = "postgres"))]
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
        fields(db = "postgres")
    )]
    async fn get_secret_metadata_by_id(&self, id: &str) -> Result<Option<SecretMetadata>> {
        let row: Option<SecretMetadataRow> = sqlx::query_as(
            "SELECT id, name, provider, provider_ref, status, created_at, updated_at \
             FROM secrets WHERE id = $1 AND status = 'active'",
        )
        .bind(id)
        .fetch_optional(self.backend.pool())
        .await?;

        Ok(row.map(SecretMetadataRow::into_metadata))
    }

    #[tracing::instrument(
        name = "catalog_create_result",
        skip(self),
        fields(db = "postgres", runtimedb.result_id = tracing::field::Empty)
    )]
    async fn create_result(&self, initial_status: ResultStatus) -> Result<String> {
        let id = crate::id::generate_result_id();
        tracing::Span::current().record("runtimedb.result_id", &id);
        sqlx::query(
            "INSERT INTO results (id, parquet_path, status, error_message, created_at)
             VALUES ($1, NULL, $2, NULL, $3)",
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
        fields(db = "postgres", runtimedb.result_id = %id, rows_affected = tracing::field::Empty)
    )]
    async fn update_result(&self, id: &str, update: ResultUpdate<'_>) -> Result<bool> {
        let result = match update {
            ResultUpdate::Processing => {
                sqlx::query("UPDATE results SET status = 'processing' WHERE id = $1")
                    .bind(id)
                    .execute(self.backend.pool())
                    .await?
            }
            ResultUpdate::Ready { parquet_path } => {
                sqlx::query("UPDATE results SET parquet_path = $1, status = 'ready' WHERE id = $2")
                    .bind(parquet_path)
                    .bind(id)
                    .execute(self.backend.pool())
                    .await?
            }
            ResultUpdate::Failed { error_message } => {
                sqlx::query(
                    "UPDATE results SET status = 'failed', error_message = $1 WHERE id = $2",
                )
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
        fields(db = "postgres", runtimedb.result_id = %id)
    )]
    async fn get_result(&self, id: &str) -> Result<Option<QueryResult>> {
        let row = sqlx::query_as::<_, QueryResultRow>(
            "SELECT id, parquet_path, status, error_message, created_at FROM results WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(self.backend.pool())
        .await?;
        Ok(row.map(QueryResult::from))
    }

    #[tracing::instrument(
        name = "catalog_get_queryable_result",
        skip(self),
        fields(db = "postgres", runtimedb.result_id = %id)
    )]
    async fn get_queryable_result(&self, id: &str) -> Result<Option<QueryResult>> {
        let row = sqlx::query_as::<_, QueryResultRow>(
            "SELECT id, parquet_path, status, error_message, created_at FROM results WHERE id = $1 AND status = 'ready'",
        )
        .bind(id)
        .fetch_optional(self.backend.pool())
        .await?;
        Ok(row.map(QueryResult::from))
    }

    #[tracing::instrument(name = "catalog_list_results", skip(self), fields(db = "postgres"))]
    async fn list_results(&self, limit: usize, offset: usize) -> Result<(Vec<QueryResult>, bool)> {
        let fetch_limit = limit + 1;
        let rows = sqlx::query_as::<_, QueryResultRow>(
            "SELECT id, parquet_path, status, error_message, created_at FROM results ORDER BY created_at DESC LIMIT $1 OFFSET $2",
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
        fields(db = "postgres")
    )]
    async fn cleanup_stale_results(&self, cutoff: DateTime<Utc>) -> Result<usize> {
        let result = sqlx::query(
            "UPDATE results SET status = 'failed', error_message = 'Cleanup: result timed out'
             WHERE status IN ('pending', 'processing') AND created_at < $1",
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
        fields(db = "postgres", runtimedb.query_run_id = %params.id)
    )]
    async fn create_query_run(&self, params: CreateQueryRun<'_>) -> Result<String> {
        let now = Utc::now();
        sqlx::query(
            "INSERT INTO query_runs (id, snapshot_id, trace_id, status, saved_query_id, saved_query_version, created_at)
             VALUES ($1, $2, $3, 'running', $4, $5, $6)",
        )
        .bind(params.id)
        .bind(params.snapshot_id)
        .bind(params.trace_id)
        .bind(params.saved_query_id)
        .bind(params.saved_query_version)
        .bind(now)
        .execute(self.backend.pool())
        .await?;
        Ok(params.id.to_string())
    }

    #[tracing::instrument(
        name = "catalog_update_query_run",
        skip(self, update),
        fields(db = "postgres", runtimedb.query_run_id = %id)
    )]
    async fn update_query_run(&self, id: &str, update: QueryRunUpdate<'_>) -> Result<bool> {
        let now = Utc::now();
        let result =
            match update {
                QueryRunUpdate::Succeeded {
                    result_id,
                    row_count,
                    execution_time_ms,
                    warning_message,
                } => sqlx::query(
                    "UPDATE query_runs SET status = 'succeeded', result_id = $1, row_count = $2, \
                     execution_time_ms = $3, warning_message = $4, completed_at = $5 WHERE id = $6",
                )
                .bind(result_id)
                .bind(row_count)
                .bind(execution_time_ms)
                .bind(warning_message)
                .bind(now)
                .bind(id)
                .execute(self.backend.pool())
                .await?,
                QueryRunUpdate::Failed {
                    error_message,
                    execution_time_ms,
                } => {
                    sqlx::query(
                        "UPDATE query_runs SET status = 'failed', error_message = $1, \
                     execution_time_ms = $2, completed_at = $3 WHERE id = $4",
                    )
                    .bind(error_message)
                    .bind(execution_time_ms)
                    .bind(now)
                    .bind(id)
                    .execute(self.backend.pool())
                    .await?
                }
            };
        Ok(result.rows_affected() > 0)
    }

    #[tracing::instrument(name = "catalog_list_query_runs", skip(self), fields(db = "postgres"))]
    async fn list_query_runs(
        &self,
        limit: usize,
        cursor: Option<&QueryRunCursor>,
    ) -> Result<(Vec<QueryRun>, bool)> {
        let fetch_limit = (limit + 1) as i64;
        let rows: Vec<QueryRunRowPg> = if let Some(cursor) = cursor {
            sqlx::query_as(
                "SELECT qr.id, snap.sql_text, snap.sql_hash, qr.snapshot_id, \
                 qr.trace_id, qr.status, qr.result_id, \
                 qr.error_message, qr.warning_message, qr.row_count, qr.execution_time_ms, \
                 qr.saved_query_id, qr.saved_query_version, qr.created_at, qr.completed_at \
                 FROM query_runs qr \
                 JOIN sql_snapshots snap ON snap.id = qr.snapshot_id \
                 WHERE (qr.created_at < $1 OR (qr.created_at = $1 AND qr.id < $2)) \
                 ORDER BY qr.created_at DESC, qr.id DESC \
                 LIMIT $3",
            )
            .bind(cursor.created_at)
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
                 LIMIT $1",
            )
            .bind(fetch_limit)
            .fetch_all(self.backend.pool())
            .await?
        };

        let has_more = rows.len() > limit;
        let runs = rows.into_iter().take(limit).map(QueryRun::from).collect();
        Ok((runs, has_more))
    }

    #[tracing::instrument(
        name = "catalog_get_query_run",
        skip(self),
        fields(db = "postgres", runtimedb.query_run_id = %id)
    )]
    async fn get_query_run(&self, id: &str) -> Result<Option<QueryRun>> {
        let row: Option<QueryRunRowPg> = sqlx::query_as(
            "SELECT qr.id, snap.sql_text, snap.sql_hash, qr.snapshot_id, \
             qr.trace_id, qr.status, qr.result_id, \
             qr.error_message, qr.warning_message, qr.row_count, qr.execution_time_ms, \
             qr.saved_query_id, qr.saved_query_version, qr.created_at, qr.completed_at \
             FROM query_runs qr \
             JOIN sql_snapshots snap ON snap.id = qr.snapshot_id \
             WHERE qr.id = $1",
        )
        .bind(id)
        .fetch_optional(self.backend.pool())
        .await?;
        Ok(row.map(QueryRun::from))
    }

    #[tracing::instrument(
        name = "catalog_count_connections_by_secret_id",
        skip(self),
        fields(db = "postgres")
    )]
    async fn count_connections_by_secret_id(&self, secret_id: &str) -> Result<i64> {
        self.backend.count_connections_by_secret_id(secret_id).await
    }

    // Upload management methods

    #[tracing::instrument(
        name = "catalog_create_upload",
        skip(self, upload),
        fields(db = "postgres")
    )]
    async fn create_upload(&self, upload: &UploadInfo) -> Result<()> {
        sqlx::query(
            "INSERT INTO uploads (id, status, storage_url, content_type, content_encoding, size_bytes, created_at, consumed_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(&upload.id)
        .bind(&upload.status)
        .bind(&upload.storage_url)
        .bind(&upload.content_type)
        .bind(&upload.content_encoding)
        .bind(upload.size_bytes)
        .bind(upload.created_at)
        .bind(upload.consumed_at)
        .execute(self.backend.pool())
        .await?;

        Ok(())
    }

    #[tracing::instrument(name = "catalog_get_upload", skip(self), fields(db = "postgres"))]
    async fn get_upload(&self, id: &str) -> Result<Option<UploadInfo>> {
        let row: Option<UploadInfoRow> = sqlx::query_as(
            "SELECT id, status, storage_url, content_type, content_encoding, size_bytes, created_at, consumed_at \
             FROM uploads WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(self.backend.pool())
        .await?;

        Ok(row.map(UploadInfoRow::into_upload_info))
    }

    #[tracing::instrument(name = "catalog_list_uploads", skip(self), fields(db = "postgres"))]
    async fn list_uploads(&self, status: Option<&str>) -> Result<Vec<UploadInfo>> {
        let rows: Vec<UploadInfoRow> = if let Some(status) = status {
            sqlx::query_as(
                "SELECT id, status, storage_url, content_type, content_encoding, size_bytes, created_at, consumed_at \
                 FROM uploads WHERE status = $1 ORDER BY created_at DESC",
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

    #[tracing::instrument(name = "catalog_consume_upload", skip(self), fields(db = "postgres"))]
    async fn consume_upload(&self, id: &str) -> Result<bool> {
        // Accept both pending and processing states (processing is the expected state after claim)
        let result = sqlx::query(
            "UPDATE uploads SET status = 'consumed', consumed_at = NOW() WHERE id = $1 AND status IN ('pending', 'processing')",
        )
        .bind(id)
        .execute(self.backend.pool())
        .await?;

        Ok(result.rows_affected() > 0)
    }

    #[tracing::instrument(name = "catalog_claim_upload", skip(self), fields(db = "postgres"))]
    async fn claim_upload(&self, id: &str) -> Result<bool> {
        let result = sqlx::query(
            "UPDATE uploads SET status = 'processing' WHERE id = $1 AND status = 'pending'",
        )
        .bind(id)
        .execute(self.backend.pool())
        .await?;

        Ok(result.rows_affected() > 0)
    }

    #[tracing::instrument(name = "catalog_release_upload", skip(self), fields(db = "postgres"))]
    async fn release_upload(&self, id: &str) -> Result<bool> {
        let result = sqlx::query(
            "UPDATE uploads SET status = 'pending' WHERE id = $1 AND status = 'processing'",
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
        fields(db = "postgres")
    )]
    async fn get_or_create_snapshot(&self, sql_text: &str) -> Result<SqlSnapshot> {
        let hash = crate::catalog::manager::sql_hash(sql_text);
        let id = crate::id::generate_snapshot_id();
        let now = Utc::now();

        // Single-statement upsert: ON CONFLICT performs a no-op update so RETURNING
        // works on both insert and conflict paths. Race-safe via UNIQUE constraint.
        let row: SqlSnapshotRowPg = sqlx::query_as(
            "INSERT INTO sql_snapshots (id, sql_hash, sql_text, created_at) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (sql_hash, sql_text) DO UPDATE SET sql_hash = EXCLUDED.sql_hash \
             RETURNING id, sql_hash, sql_text, created_at",
        )
        .bind(&id)
        .bind(&hash)
        .bind(sql_text)
        .bind(now)
        .fetch_one(self.backend.pool())
        .await?;

        Ok(SqlSnapshot::from(row))
    }

    // Saved query methods

    #[tracing::instrument(
        name = "catalog_create_saved_query",
        skip(self),
        fields(db = "postgres")
    )]
    async fn create_saved_query(&self, name: &str, snapshot_id: &str) -> Result<SavedQuery> {
        let id = crate::id::generate_saved_query_id();
        let now = Utc::now();

        let mut tx = self.backend.pool().begin().await?;

        sqlx::query(
            "INSERT INTO saved_queries (id, name, latest_version, created_at, updated_at) \
             VALUES ($1, $2, 1, $3, $4)",
        )
        .bind(&id)
        .bind(name)
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            "INSERT INTO saved_query_versions (saved_query_id, version, snapshot_id, created_at) \
             VALUES ($1, 1, $2, $3)",
        )
        .bind(&id)
        .bind(snapshot_id)
        .bind(now)
        .execute(&mut *tx)
        .await?;

        let row: SavedQueryRowPg = sqlx::query_as(
            "SELECT id, name, latest_version, created_at, updated_at \
             FROM saved_queries WHERE id = $1",
        )
        .bind(&id)
        .fetch_one(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(SavedQuery::from(row))
    }

    #[tracing::instrument(
        name = "catalog_get_saved_query",
        skip(self),
        fields(db = "postgres", runtimedb.saved_query_id = %id)
    )]
    async fn get_saved_query(&self, id: &str) -> Result<Option<SavedQuery>> {
        let row: Option<SavedQueryRowPg> = sqlx::query_as(
            "SELECT id, name, latest_version, created_at, updated_at \
             FROM saved_queries WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(self.backend.pool())
        .await?;

        Ok(row.map(SavedQuery::from))
    }

    #[tracing::instrument(
        name = "catalog_list_saved_queries",
        skip(self),
        fields(db = "postgres")
    )]
    async fn list_saved_queries(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<SavedQuery>, bool)> {
        let fetch_limit = i64::try_from(limit.saturating_add(1)).unwrap_or(i64::MAX);
        let rows: Vec<SavedQueryRowPg> = sqlx::query_as(
            "SELECT id, name, latest_version, created_at, updated_at \
             FROM saved_queries \
             ORDER BY name ASC \
             LIMIT $1 OFFSET $2",
        )
        .bind(fetch_limit)
        .bind(i64::try_from(offset).unwrap_or(i64::MAX))
        .fetch_all(self.backend.pool())
        .await?;

        let has_more = rows.len() > limit;
        let queries = rows.into_iter().take(limit).map(SavedQuery::from).collect();
        Ok((queries, has_more))
    }

    #[tracing::instrument(
        name = "catalog_update_saved_query",
        skip(self),
        fields(db = "postgres", runtimedb.saved_query_id = %id)
    )]
    async fn update_saved_query(
        &self,
        id: &str,
        name: Option<&str>,
        snapshot_id: &str,
    ) -> Result<Option<SavedQuery>> {
        let mut tx = self.backend.pool().begin().await?;

        // FOR UPDATE acquires a row-level lock to prevent concurrent updates
        // from reading the same latest_version and producing duplicate versions.
        let existing: Option<SavedQueryRowPg> = sqlx::query_as(
            "SELECT id, name, latest_version, created_at, updated_at \
             FROM saved_queries WHERE id = $1 FOR UPDATE",
        )
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?;

        let existing = match existing {
            Some(row) => row,
            None => return Ok(None),
        };

        let effective_name = name.unwrap_or(&existing.name);

        // Check if the latest version already points to the same snapshot
        // and the name is unchanged — skip creating a redundant version.
        // This check runs inside the FOR UPDATE lock, so no TOCTOU race.
        let current_snapshot: Option<(String,)> = sqlx::query_as(
            "SELECT snapshot_id FROM saved_query_versions \
             WHERE saved_query_id = $1 AND version = $2",
        )
        .bind(id)
        .bind(existing.latest_version)
        .fetch_optional(&mut *tx)
        .await?;

        if let Some((current_sid,)) = current_snapshot {
            if current_sid == snapshot_id && effective_name == existing.name {
                tx.commit().await?;
                return Ok(Some(SavedQuery::from(existing)));
            }
        }

        let new_version = existing.latest_version + 1;
        let now = Utc::now();

        sqlx::query(
            "INSERT INTO saved_query_versions (saved_query_id, version, snapshot_id, created_at) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(id)
        .bind(new_version)
        .bind(snapshot_id)
        .bind(now)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            "UPDATE saved_queries SET name = $1, latest_version = $2, updated_at = $3 WHERE id = $4",
        )
        .bind(effective_name)
        .bind(new_version)
        .bind(now)
        .bind(id)
        .execute(&mut *tx)
        .await?;

        let row: SavedQueryRowPg = sqlx::query_as(
            "SELECT id, name, latest_version, created_at, updated_at \
             FROM saved_queries WHERE id = $1",
        )
        .bind(id)
        .fetch_one(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(Some(SavedQuery::from(row)))
    }

    #[tracing::instrument(
        name = "catalog_delete_saved_query",
        skip(self),
        fields(db = "postgres", runtimedb.saved_query_id = %id)
    )]
    async fn delete_saved_query(&self, id: &str) -> Result<bool> {
        // ON DELETE CASCADE on the saved_query_versions FK handles child deletion.
        // Query runs retain history via their snapshot_id FK to sql_snapshots.
        let result = sqlx::query("DELETE FROM saved_queries WHERE id = $1")
            .bind(id)
            .execute(self.backend.pool())
            .await?;

        Ok(result.rows_affected() > 0)
    }

    #[tracing::instrument(
        name = "catalog_get_saved_query_version",
        skip(self),
        fields(db = "postgres", runtimedb.saved_query_id = %saved_query_id)
    )]
    async fn get_saved_query_version(
        &self,
        saved_query_id: &str,
        version: i32,
    ) -> Result<Option<SavedQueryVersion>> {
        let row: Option<SavedQueryVersionRowPg> = sqlx::query_as(
            "SELECT sqv.saved_query_id, sqv.version, sqv.snapshot_id, \
             snap.sql_text, snap.sql_hash, sqv.created_at \
             FROM saved_query_versions sqv \
             JOIN sql_snapshots snap ON snap.id = sqv.snapshot_id \
             WHERE sqv.saved_query_id = $1 AND sqv.version = $2",
        )
        .bind(saved_query_id)
        .bind(version)
        .fetch_optional(self.backend.pool())
        .await?;

        Ok(row.map(SavedQueryVersion::from))
    }

    #[tracing::instrument(
        name = "catalog_list_saved_query_versions",
        skip(self),
        fields(db = "postgres", runtimedb.saved_query_id = %saved_query_id)
    )]
    async fn list_saved_query_versions(
        &self,
        saved_query_id: &str,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<SavedQueryVersion>, bool)> {
        let fetch_limit = limit.saturating_add(1);
        let rows: Vec<SavedQueryVersionRowPg> = sqlx::query_as(
            "SELECT sqv.saved_query_id, sqv.version, sqv.snapshot_id, \
             snap.sql_text, snap.sql_hash, sqv.created_at \
             FROM saved_query_versions sqv \
             JOIN sql_snapshots snap ON snap.id = sqv.snapshot_id \
             WHERE sqv.saved_query_id = $1 \
             ORDER BY sqv.version DESC \
             LIMIT $2 OFFSET $3",
        )
        .bind(saved_query_id)
        .bind(i64::try_from(fetch_limit).unwrap_or(i64::MAX))
        .bind(i64::try_from(offset).unwrap_or(i64::MAX))
        .fetch_all(self.backend.pool())
        .await?;

        let has_more = rows.len() > limit;
        let versions = rows
            .into_iter()
            .take(limit)
            .map(SavedQueryVersion::from)
            .collect();

        Ok((versions, has_more))
    }

    // Dataset management methods

    #[tracing::instrument(
        name = "catalog_create_dataset",
        skip(self, dataset),
        fields(db = "postgres", runtimedb.dataset_id = %dataset.id)
    )]
    async fn create_dataset(&self, dataset: &DatasetInfo) -> Result<()> {
        sqlx::query(
            "INSERT INTO datasets (id, label, schema_name, table_name, parquet_url, arrow_schema_json, source_type, source_config, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(&dataset.id)
        .bind(&dataset.label)
        .bind(&dataset.schema_name)
        .bind(&dataset.table_name)
        .bind(&dataset.parquet_url)
        .bind(&dataset.arrow_schema_json)
        .bind(&dataset.source_type)
        .bind(&dataset.source_config)
        .bind(dataset.created_at)
        .bind(dataset.updated_at)
        .execute(self.backend.pool())
        .await?;

        Ok(())
    }

    #[tracing::instrument(
        name = "catalog_get_dataset",
        skip(self),
        fields(db = "postgres", runtimedb.dataset_id = %id)
    )]
    async fn get_dataset(&self, id: &str) -> Result<Option<DatasetInfo>> {
        let row: Option<DatasetInfoRow> = sqlx::query_as(
            "SELECT id, label, schema_name, table_name, parquet_url, arrow_schema_json, source_type, source_config, created_at, updated_at \
             FROM datasets WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(self.backend.pool())
        .await?;

        Ok(row.map(DatasetInfoRow::into_dataset_info))
    }

    #[tracing::instrument(
        name = "catalog_get_dataset_by_table_name",
        skip(self),
        fields(db = "postgres")
    )]
    async fn get_dataset_by_table_name(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<DatasetInfo>> {
        let row: Option<DatasetInfoRow> = sqlx::query_as(
            "SELECT id, label, schema_name, table_name, parquet_url, arrow_schema_json, source_type, source_config, created_at, updated_at \
             FROM datasets WHERE schema_name = $1 AND table_name = $2",
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
        fields(db = "postgres", runtimedb.count = tracing::field::Empty)
    )]
    async fn list_datasets(&self, limit: usize, offset: usize) -> Result<(Vec<DatasetInfo>, bool)> {
        // Fetch one extra to determine if there are more results
        // Use saturating_add to prevent overflow when limit is very large
        let fetch_limit = limit.saturating_add(1) as i64;
        let rows: Vec<DatasetInfoRow> = sqlx::query_as(
            "SELECT id, label, schema_name, table_name, parquet_url, arrow_schema_json, source_type, source_config, created_at, updated_at \
             FROM datasets ORDER BY label LIMIT $1 OFFSET $2",
        )
        .bind(fetch_limit)
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

    #[tracing::instrument(
        name = "catalog_list_all_datasets",
        skip(self),
        fields(db = "postgres")
    )]
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
        fields(db = "postgres")
    )]
    async fn list_dataset_table_names(&self, schema_name: &str) -> Result<Vec<String>> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT table_name FROM datasets WHERE schema_name = $1 ORDER BY table_name",
        )
        .bind(schema_name)
        .fetch_all(self.backend.pool())
        .await?;

        Ok(rows.into_iter().map(|(name,)| name).collect())
    }

    #[tracing::instrument(name = "catalog_update_dataset", skip(self), fields(db = "postgres"))]
    async fn update_dataset(&self, id: &str, label: &str, table_name: &str) -> Result<bool> {
        let result = sqlx::query(
            "UPDATE datasets SET label = $1, table_name = $2, updated_at = NOW() WHERE id = $3",
        )
        .bind(label)
        .bind(table_name)
        .bind(id)
        .execute(self.backend.pool())
        .await?;

        Ok(result.rows_affected() > 0)
    }

    #[tracing::instrument(name = "catalog_delete_dataset", skip(self), fields(db = "postgres"))]
    async fn delete_dataset(&self, id: &str) -> Result<Option<DatasetInfo>> {
        // First get the dataset so we can return it
        let dataset = self.get_dataset(id).await?;

        if dataset.is_some() {
            sqlx::query("DELETE FROM datasets WHERE id = $1")
                .bind(id)
                .execute(self.backend.pool())
                .await?;
        }

        Ok(dataset)
    }
}

impl Debug for PostgresCatalogManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresCatalogManager")
            .field("pool", self.backend.pool())
            .finish()
    }
}

impl CatalogMigrations for PostgresMigrationBackend {
    type Pool = PgPool;

    fn migrations() -> &'static [Migration] {
        POSTGRES_MIGRATIONS
    }

    async fn ensure_migrations_table(pool: &Self::Pool) -> Result<()> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS schema_migrations (
                version BIGINT PRIMARY KEY,
                hash TEXT NOT NULL,
                applied_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
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
