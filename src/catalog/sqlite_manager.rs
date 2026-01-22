use crate::catalog::backend::CatalogBackend;
use crate::catalog::manager::{
    CatalogManager, ConnectionInfo, OptimisticLock, PendingDeletion, QueryResult, TableInfo,
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

    async fn get_connection(&self, name: &str) -> Result<Option<ConnectionInfo>> {
        self.backend.get_connection(name).await
    }

    async fn get_connection_by_external_id(
        &self,
        external_id: &str,
    ) -> Result<Option<ConnectionInfo>> {
        self.backend
            .get_connection_by_external_id(external_id)
            .await
    }

    async fn add_table(
        &self,
        connection_id: i32,
        schema_name: &str,
        table_name: &str,
        arrow_schema_json: &str,
    ) -> Result<i32> {
        self.backend
            .add_table(connection_id, schema_name, table_name, arrow_schema_json)
            .await
    }

    async fn list_tables(&self, connection_id: Option<i32>) -> Result<Vec<TableInfo>> {
        self.backend.list_tables(connection_id).await
    }

    async fn get_table(
        &self,
        connection_id: i32,
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
        connection_id: i32,
        schema_name: &str,
        table_name: &str,
    ) -> Result<TableInfo> {
        self.backend
            .clear_table_cache_metadata(connection_id, schema_name, table_name)
            .await
    }

    async fn clear_connection_cache_metadata(&self, name: &str) -> Result<()> {
        self.backend.clear_connection_cache_metadata(name).await
    }

    async fn delete_connection(&self, name: &str) -> Result<()> {
        self.backend.delete_connection(name).await
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

    async fn get_connection_by_id(&self, id: i32) -> Result<Option<ConnectionInfo>> {
        self.backend.get_connection_by_id(id).await
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

    async fn store_result(&self, result: &QueryResult) -> Result<()> {
        sqlx::query(
            "INSERT INTO results (id, parquet_path, created_at)
             VALUES (?, ?, ?)",
        )
        .bind(&result.id)
        .bind(&result.parquet_path)
        .bind(result.created_at)
        .execute(self.backend.pool())
        .await?;
        Ok(())
    }

    async fn get_result(&self, id: &str) -> Result<Option<QueryResult>> {
        let result = sqlx::query_as::<_, QueryResult>(
            "SELECT id, parquet_path, created_at FROM results WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(self.backend.pool())
        .await?;
        Ok(result)
    }

    async fn list_results(&self, limit: usize, offset: usize) -> Result<(Vec<QueryResult>, bool)> {
        // Fetch one extra to determine if there are more results
        let fetch_limit = limit + 1;
        let results = sqlx::query_as::<_, QueryResult>(
            "SELECT id, parquet_path, created_at FROM results
             ORDER BY created_at DESC
             LIMIT ? OFFSET ?",
        )
        .bind(fetch_limit as i64)
        .bind(offset as i64)
        .fetch_all(self.backend.pool())
        .await?;

        let has_more = results.len() > limit;
        let results = if has_more {
            results.into_iter().take(limit).collect()
        } else {
            results
        };

        Ok((results, has_more))
    }

    async fn count_connections_by_secret_id(&self, secret_id: &str) -> Result<i64> {
        self.backend.count_connections_by_secret_id(secret_id).await
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
