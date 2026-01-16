use crate::catalog::backend::CatalogBackend;
use crate::catalog::manager::{
    CatalogManager, ConnectionInfo, OptimisticLock, PendingDeletion, TableInfo,
};
use crate::catalog::migrations::{run_migrations, CatalogMigrations};
use crate::secrets::{SecretMetadata, SecretStatus};
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{Sqlite, SqlitePool};
use std::fmt::{self, Debug, Formatter};
use std::str::FromStr;

pub struct SqliteCatalogManager {
    backend: CatalogBackend<Sqlite>,
    catalog_path: String,
}

/// Row type for secret metadata queries (SQLite stores timestamps as strings)
#[derive(sqlx::FromRow)]
struct SecretMetadataRow {
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
}

impl PendingDeletionRow {
    fn into_pending_deletion(self) -> PendingDeletion {
        PendingDeletion {
            id: self.id,
            path: self.path,
            delete_after: self.delete_after.parse().unwrap_or_else(|_| Utc::now()),
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

    async fn initialize_schema(pool: &SqlitePool) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE connections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                external_id TEXT UNIQUE NOT NULL,
                name TEXT UNIQUE NOT NULL,
                source_type TEXT NOT NULL,
                config_json TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        "#,
        )
        .execute(pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE tables (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                connection_id INTEGER NOT NULL,
                schema_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                parquet_path TEXT,
                last_sync TIMESTAMP,
                arrow_schema_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (connection_id) REFERENCES connections(id),
                UNIQUE (connection_id, schema_name, table_name)
            )
        "#,
        )
        .execute(pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE secrets (
                name TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                provider_ref TEXT,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            "#,
        )
        .execute(pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE encrypted_secret_values (
                name TEXT PRIMARY KEY,
                encrypted_value BLOB NOT NULL
            )
            "#,
        )
        .execute(pool)
        .await?;

        Ok(())
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
    ) -> Result<i32> {
        self.backend
            .add_connection(name, source_type, config_json)
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
            "SELECT name, provider, provider_ref, status, created_at, updated_at \
             FROM secrets WHERE name = ? AND status = 'active'",
        )
        .bind(name)
        .fetch_optional(self.backend.pool())
        .await?;

        Ok(row.map(SecretMetadataRow::into_metadata))
    }

    async fn get_secret_metadata_any_status(&self, name: &str) -> Result<Option<SecretMetadata>> {
        let row: Option<SecretMetadataRow> = sqlx::query_as(
            "SELECT name, provider, provider_ref, status, created_at, updated_at \
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
            "INSERT INTO secrets (name, provider, provider_ref, status, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
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
            .push(" WHERE name = ")
            .push_bind(&metadata.name);

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

    async fn get_encrypted_secret(&self, name: &str) -> Result<Option<Vec<u8>>> {
        sqlx::query_scalar("SELECT encrypted_value FROM encrypted_secret_values WHERE name = ?")
            .bind(name)
            .fetch_optional(self.backend.pool())
            .await
            .map_err(Into::into)
    }

    async fn put_encrypted_secret_value(&self, name: &str, encrypted_value: &[u8]) -> Result<()> {
        sqlx::query(
            "INSERT INTO encrypted_secret_values (name, encrypted_value) \
             VALUES (?, ?) \
             ON CONFLICT (name) DO UPDATE SET \
             encrypted_value = excluded.encrypted_value",
        )
        .bind(name)
        .bind(encrypted_value)
        .execute(self.backend.pool())
        .await?;

        Ok(())
    }

    async fn delete_encrypted_secret_value(&self, name: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM encrypted_secret_values WHERE name = ?")
            .bind(name)
            .execute(self.backend.pool())
            .await?;

        Ok(result.rows_affected() > 0)
    }

    async fn list_secrets(&self) -> Result<Vec<SecretMetadata>> {
        let rows: Vec<SecretMetadataRow> = sqlx::query_as(
            "SELECT name, provider, provider_ref, status, created_at, updated_at \
             FROM secrets WHERE status = 'active' ORDER BY name",
        )
        .fetch_all(self.backend.pool())
        .await?;

        Ok(rows
            .into_iter()
            .map(SecretMetadataRow::into_metadata)
            .collect())
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
            "SELECT id, path, delete_after FROM pending_deletions WHERE delete_after <= ?",
        )
        .bind(Utc::now().to_rfc3339())
        .fetch_all(self.backend.pool())
        .await?;

        Ok(rows
            .into_iter()
            .map(PendingDeletionRow::into_pending_deletion)
            .collect())
    }

    async fn remove_pending_deletion(&self, id: i32) -> Result<()> {
        self.backend.remove_pending_deletion(id).await
    }
}

impl CatalogMigrations for SqliteMigrationBackend {
    type Pool = SqlitePool;

    async fn ensure_migrations_table(pool: &Self::Pool) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            "#,
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn current_version(pool: &Self::Pool) -> Result<i64> {
        sqlx::query_scalar("SELECT COALESCE(MAX(version), 0) FROM schema_migrations")
            .fetch_one(pool)
            .await
            .map_err(Into::into)
    }

    async fn record_version(pool: &Self::Pool, version: i64) -> Result<()> {
        sqlx::query("INSERT INTO schema_migrations (version) VALUES (?)")
            .bind(version)
            .execute(pool)
            .await?;
        Ok(())
    }

    async fn migrate_v1(pool: &Self::Pool) -> Result<()> {
        SqliteCatalogManager::initialize_schema(pool).await
    }

    async fn migrate_v2(pool: &Self::Pool) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS pending_deletions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                path TEXT NOT NULL,
                delete_after TEXT NOT NULL
            )
            "#,
        )
        .execute(pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_pending_deletions_due ON pending_deletions(delete_after)",
        )
        .execute(pool)
        .await?;

        Ok(())
    }

    async fn migrate_v3(pool: &Self::Pool) -> Result<()> {
        // Add UNIQUE index on path column to prevent duplicate deletion records
        sqlx::query(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_pending_deletions_path ON pending_deletions(path)",
        )
        .execute(pool)
        .await?;

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
