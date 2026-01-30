use crate::catalog::backend::CatalogBackend;
use crate::catalog::manager::{
    CatalogManager, ConnectionInfo, DatasetInfo, OptimisticLock, PendingDeletion, QueryResult,
    TableInfo, UploadInfo,
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
    async fn run_migrations(&self) -> Result<()> {
        run_migrations::<SqliteMigrationBackend>(self.backend.pool()).await
    }

    async fn list_connections(&self) -> Result<Vec<ConnectionInfo>> {
        self.backend.list_connections().await
    }

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

    async fn get_connection(&self, id: &str) -> Result<Option<ConnectionInfo>> {
        self.backend.get_connection(id).await
    }

    async fn get_connection_by_name(&self, name: &str) -> Result<Option<ConnectionInfo>> {
        self.backend.get_connection_by_name(name).await
    }

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

    async fn list_tables(&self, connection_id: Option<&str>) -> Result<Vec<TableInfo>> {
        self.backend.list_tables(connection_id).await
    }

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

    async fn update_table_sync(&self, table_id: i32, parquet_path: &str) -> Result<()> {
        self.backend.update_table_sync(table_id, parquet_path).await
    }

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

    async fn clear_connection_cache_metadata(&self, connection_id: &str) -> Result<()> {
        self.backend
            .clear_connection_cache_metadata(connection_id)
            .await
    }

    async fn delete_connection(&self, connection_id: &str) -> Result<()> {
        self.backend.delete_connection(connection_id).await
    }

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

    async fn set_secret_status(&self, name: &str, status: SecretStatus) -> Result<bool> {
        let result = sqlx::query("UPDATE secrets SET status = ? WHERE name = ?")
            .bind(status.as_str())
            .bind(name)
            .execute(self.backend.pool())
            .await?;

        Ok(result.rows_affected() > 0)
    }

    async fn delete_secret_metadata(&self, name: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM secrets WHERE name = ?")
            .bind(name)
            .execute(self.backend.pool())
            .await?;

        Ok(result.rows_affected() > 0)
    }

    async fn get_encrypted_secret(&self, secret_id: &str) -> Result<Option<Vec<u8>>> {
        sqlx::query_scalar(
            "SELECT encrypted_value FROM encrypted_secret_values WHERE secret_id = ?",
        )
        .bind(secret_id)
        .fetch_optional(self.backend.pool())
        .await
        .map_err(Into::into)
    }

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

    async fn delete_encrypted_secret_value(&self, secret_id: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM encrypted_secret_values WHERE secret_id = ?")
            .bind(secret_id)
            .execute(self.backend.pool())
            .await?;

        Ok(result.rows_affected() > 0)
    }

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

    async fn increment_deletion_retry(&self, id: i32) -> Result<i32> {
        let new_count: (i32,) = sqlx::query_as(
            "UPDATE pending_deletions SET retry_count = retry_count + 1 WHERE id = ? RETURNING retry_count",
        )
        .bind(id)
        .fetch_one(self.backend.pool())
        .await?;
        Ok(new_count.0)
    }

    async fn remove_pending_deletion(&self, id: i32) -> Result<()> {
        self.backend.remove_pending_deletion(id).await
    }

    #[tracing::instrument(
        name = "catalog_store_result",
        skip(self, result),
        fields(runtimedb.result_id = %result.id)
    )]
    async fn store_result(&self, result: &QueryResult) -> Result<()> {
        sqlx::query(
            "INSERT INTO results (id, parquet_path, status, created_at)
             VALUES (?, ?, ?, ?)",
        )
        .bind(&result.id)
        .bind(&result.parquet_path)
        .bind(&result.status)
        .bind(result.created_at)
        .execute(self.backend.pool())
        .await?;
        Ok(())
    }

    #[tracing::instrument(
        name = "catalog_get_result",
        skip(self),
        fields(runtimedb.result_id = %id)
    )]
    async fn get_result(&self, id: &str) -> Result<Option<QueryResult>> {
        let result = sqlx::query_as::<_, QueryResult>(
            "SELECT id, parquet_path, status, created_at FROM results WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(self.backend.pool())
        .await?;
        Ok(result)
    }

    #[tracing::instrument(
        name = "catalog_store_result_pending",
        skip(self),
        fields(runtimedb.result_id = %id)
    )]
    async fn store_result_pending(&self, id: &str, created_at: DateTime<Utc>) -> Result<()> {
        sqlx::query(
            "INSERT INTO results (id, parquet_path, status, created_at)
             VALUES (?, NULL, 'processing', ?)",
        )
        .bind(id)
        .bind(created_at)
        .execute(self.backend.pool())
        .await?;
        Ok(())
    }

    #[tracing::instrument(
        name = "catalog_finalize_result",
        skip(self),
        fields(runtimedb.result_id = %id)
    )]
    async fn finalize_result(&self, id: &str, parquet_path: &str) -> Result<()> {
        sqlx::query("UPDATE results SET parquet_path = ?, status = 'ready' WHERE id = ?")
            .bind(parquet_path)
            .bind(id)
            .execute(self.backend.pool())
            .await?;
        Ok(())
    }

    #[tracing::instrument(
        name = "catalog_fail_result",
        skip(self),
        fields(runtimedb.result_id = %id)
    )]
    async fn fail_result(&self, id: &str) -> Result<()> {
        sqlx::query("UPDATE results SET status = 'failed' WHERE id = ?")
            .bind(id)
            .execute(self.backend.pool())
            .await?;
        Ok(())
    }

    #[tracing::instrument(
        name = "catalog_get_queryable_result",
        skip(self),
        fields(runtimedb.result_id = %id)
    )]
    async fn get_queryable_result(&self, id: &str) -> Result<Option<QueryResult>> {
        let result = sqlx::query_as::<_, QueryResult>(
            "SELECT id, parquet_path, status, created_at FROM results WHERE id = ? AND status = 'ready'",
        )
        .bind(id)
        .fetch_optional(self.backend.pool())
        .await?;
        Ok(result)
    }

    async fn list_results(&self, limit: usize, offset: usize) -> Result<(Vec<QueryResult>, bool)> {
        // Fetch one extra to determine has_more
        let fetch_limit = limit + 1;
        let results = sqlx::query_as::<_, QueryResult>(
            "SELECT id, parquet_path, status, created_at FROM results ORDER BY created_at DESC LIMIT ? OFFSET ?",
        )
        .bind(fetch_limit as i64)
        .bind(offset as i64)
        .fetch_all(self.backend.pool())
        .await?;

        let has_more = results.len() > limit;
        let results = results.into_iter().take(limit).collect();
        Ok((results, has_more))
    }

    async fn count_connections_by_secret_id(&self, secret_id: &str) -> Result<i64> {
        self.backend.count_connections_by_secret_id(secret_id).await
    }

    // Upload management methods

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

    async fn claim_upload(&self, id: &str) -> Result<bool> {
        let result = sqlx::query(
            "UPDATE uploads SET status = 'processing' WHERE id = ? AND status = 'pending'",
        )
        .bind(id)
        .execute(self.backend.pool())
        .await?;

        Ok(result.rows_affected() > 0)
    }

    async fn release_upload(&self, id: &str) -> Result<bool> {
        let result = sqlx::query(
            "UPDATE uploads SET status = 'pending' WHERE id = ? AND status = 'processing'",
        )
        .bind(id)
        .execute(self.backend.pool())
        .await?;

        Ok(result.rows_affected() > 0)
    }

    // Dataset management methods

    #[tracing::instrument(
        name = "catalog_create_dataset",
        skip(self, dataset),
        fields(runtimedb.dataset_id = %dataset.id)
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
        fields(runtimedb.dataset_id = %id)
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
        fields(runtimedb.count = tracing::field::Empty)
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

    async fn list_dataset_table_names(&self, schema_name: &str) -> Result<Vec<String>> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT table_name FROM datasets WHERE schema_name = ? ORDER BY table_name",
        )
        .bind(schema_name)
        .fetch_all(self.backend.pool())
        .await?;

        Ok(rows.into_iter().map(|(name,)| name).collect())
    }

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
