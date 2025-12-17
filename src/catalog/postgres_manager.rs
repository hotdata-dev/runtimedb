use crate::catalog::backend::CatalogBackend;
use crate::catalog::manager::{CatalogManager, ConnectionInfo, TableInfo};
use crate::catalog::migrations::{run_migrations, CatalogMigrations};
use crate::secrets::SecretMetadata;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres};
use std::fmt::{self, Debug, Formatter};

pub struct PostgresCatalogManager {
    backend: CatalogBackend<Postgres>,
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

    async fn initialize_schema(pool: &PgPool) -> Result<()> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS connections (
                id SERIAL PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                source_type TEXT NOT NULL,
                config_json TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )",
        )
        .execute(pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS tables (
                id SERIAL PRIMARY KEY,
                connection_id INTEGER NOT NULL,
                schema_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                parquet_path TEXT,
                last_sync TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                arrow_schema_json TEXT,
                FOREIGN KEY (connection_id) REFERENCES connections(id),
                UNIQUE (connection_id, schema_name, table_name)
            )",
        )
        .execute(pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS secrets (
                name TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                provider_ref TEXT,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            )",
        )
        .execute(pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS encrypted_secret_values (
                name TEXT PRIMARY KEY,
                encrypted_value BYTEA NOT NULL
            )",
        )
        .execute(pool)
        .await?;

        Ok(())
    }
}

struct PostgresMigrationBackend;

impl CatalogMigrations for PostgresMigrationBackend {
    type Pool = PgPool;

    async fn ensure_migrations_table(pool: &Self::Pool) -> Result<()> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS schema_migrations (
                version BIGINT PRIMARY KEY,
                applied_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            )",
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
        sqlx::query("INSERT INTO schema_migrations (version) VALUES ($1)")
            .bind(version)
            .execute(pool)
            .await?;
        Ok(())
    }

    async fn migrate_v1(pool: &Self::Pool) -> Result<()> {
        PostgresCatalogManager::initialize_schema(pool).await
    }
}

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
        #[derive(sqlx::FromRow)]
        struct Row {
            name: String,
            provider_ref: Option<String>,
            created_at: DateTime<Utc>,
            updated_at: DateTime<Utc>,
        }

        let row: Option<Row> = sqlx::query_as(
            "SELECT name, provider_ref, created_at, updated_at FROM secrets WHERE name = $1 AND status = 'active'",
        )
        .bind(name)
        .fetch_optional(self.backend.pool())
        .await?;

        Ok(row.map(|r| SecretMetadata {
            name: r.name,
            provider_ref: r.provider_ref,
            created_at: r.created_at,
            updated_at: r.updated_at,
        }))
    }

    async fn get_secret_metadata_any_status(&self, name: &str) -> Result<Option<SecretMetadata>> {
        #[derive(sqlx::FromRow)]
        struct Row {
            name: String,
            provider_ref: Option<String>,
            created_at: DateTime<Utc>,
            updated_at: DateTime<Utc>,
        }

        let row: Option<Row> = sqlx::query_as(
            "SELECT name, provider_ref, created_at, updated_at FROM secrets WHERE name = $1",
        )
        .bind(name)
        .fetch_optional(self.backend.pool())
        .await?;

        Ok(row.map(|r| SecretMetadata {
            name: r.name,
            provider_ref: r.provider_ref,
            created_at: r.created_at,
            updated_at: r.updated_at,
        }))
    }

    async fn put_secret_metadata(
        &self,
        name: &str,
        provider: &str,
        provider_ref: Option<&str>,
        timestamp: DateTime<Utc>,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO secrets (name, provider, provider_ref, status, created_at, updated_at) \
             VALUES ($1, $2, $3, 'active', $4, $5) \
             ON CONFLICT (name) DO UPDATE SET \
             provider = excluded.provider, \
             provider_ref = excluded.provider_ref, \
             status = 'active', \
             updated_at = excluded.updated_at",
        )
        .bind(name)
        .bind(provider)
        .bind(provider_ref)
        .bind(timestamp)
        .bind(timestamp)
        .execute(self.backend.pool())
        .await?;

        Ok(())
    }

    async fn set_secret_status(&self, name: &str, status: &str) -> Result<bool> {
        let result = sqlx::query("UPDATE secrets SET status = $1 WHERE name = $2")
            .bind(status)
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
        #[derive(sqlx::FromRow)]
        struct Row {
            name: String,
            provider_ref: Option<String>,
            created_at: DateTime<Utc>,
            updated_at: DateTime<Utc>,
        }

        let rows: Vec<Row> = sqlx::query_as(
            "SELECT name, provider_ref, created_at, updated_at FROM secrets WHERE status = 'active' ORDER BY name",
        )
        .fetch_all(self.backend.pool())
        .await?;

        Ok(rows
            .into_iter()
            .map(|r| SecretMetadata {
                name: r.name,
                provider_ref: r.provider_ref,
                created_at: r.created_at,
                updated_at: r.updated_at,
            })
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
