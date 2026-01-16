use crate::catalog::backend::CatalogBackend;
use crate::catalog::manager::{
    CatalogManager, ConnectionInfo, OptimisticLock, PendingDeletion, TableInfo,
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
            name: self.name,
            provider: self.provider,
            provider_ref: self.provider_ref,
            status: SecretStatus::from_str(&self.status).unwrap_or(SecretStatus::Active),
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
    async fn run_migrations(&self) -> Result<()> {
        run_migrations::<PostgresMigrationBackend>(self.backend.pool()).await
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

    async fn get_connection_by_id(&self, id: i32) -> Result<Option<ConnectionInfo>> {
        self.backend.get_connection_by_id(id).await
    }

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

    async fn increment_deletion_retry(&self, id: i32) -> Result<i32> {
        let new_count: (i32,) = sqlx::query_as(
            "UPDATE pending_deletions SET retry_count = retry_count + 1 WHERE id = $1 RETURNING retry_count",
        )
        .bind(id)
        .fetch_one(self.backend.pool())
        .await?;
        Ok(new_count.0)
    }

    async fn remove_pending_deletion(&self, id: i32) -> Result<()> {
        self.backend.remove_pending_deletion(id).await
    }

    async fn get_secret_metadata(&self, name: &str) -> Result<Option<SecretMetadata>> {
        let row: Option<SecretMetadataRow> = sqlx::query_as(
            "SELECT name, provider, provider_ref, status, created_at, updated_at \
             FROM secrets WHERE name = $1 AND status = 'active'",
        )
        .bind(name)
        .fetch_optional(self.backend.pool())
        .await?;

        Ok(row.map(SecretMetadataRow::into_metadata))
    }

    async fn get_secret_metadata_any_status(&self, name: &str) -> Result<Option<SecretMetadata>> {
        let row: Option<SecretMetadataRow> = sqlx::query_as(
            "SELECT name, provider, provider_ref, status, created_at, updated_at \
             FROM secrets WHERE name = $1",
        )
        .bind(name)
        .fetch_optional(self.backend.pool())
        .await?;

        Ok(row.map(SecretMetadataRow::into_metadata))
    }

    async fn create_secret_metadata(&self, metadata: &SecretMetadata) -> Result<()> {
        sqlx::query(
            "INSERT INTO secrets (name, provider, provider_ref, status, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
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
            .push(" WHERE name = ")
            .push_bind(&metadata.name);

        if let Some(lock) = lock {
            qb.push(" AND created_at = ").push_bind(lock.created_at);
        }

        let result = qb.build().execute(self.backend.pool()).await?;
        Ok(result.rows_affected() > 0)
    }

    async fn set_secret_status(&self, name: &str, status: SecretStatus) -> Result<bool> {
        let result = sqlx::query("UPDATE secrets SET status = $1 WHERE name = $2")
            .bind(status.as_str())
            .bind(name)
            .execute(self.backend.pool())
            .await?;

        Ok(result.rows_affected() > 0)
    }

    async fn delete_secret_metadata(&self, name: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM secrets WHERE name = $1")
            .bind(name)
            .execute(self.backend.pool())
            .await?;

        Ok(result.rows_affected() > 0)
    }

    async fn get_encrypted_secret(&self, name: &str) -> Result<Option<Vec<u8>>> {
        sqlx::query_scalar("SELECT encrypted_value FROM encrypted_secret_values WHERE name = $1")
            .bind(name)
            .fetch_optional(self.backend.pool())
            .await
            .map_err(Into::into)
    }

    async fn put_encrypted_secret_value(&self, name: &str, encrypted_value: &[u8]) -> Result<()> {
        sqlx::query(
            "INSERT INTO encrypted_secret_values (name, encrypted_value) \
             VALUES ($1, $2) \
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
        let result = sqlx::query("DELETE FROM encrypted_secret_values WHERE name = $1")
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
