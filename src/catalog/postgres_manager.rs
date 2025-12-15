use crate::catalog::backend::CatalogBackend;
use crate::catalog::manager::{CatalogManager, ConnectionInfo, TableInfo};
use crate::catalog::migrations::{run_migrations, CatalogMigrations};
use anyhow::Result;
use async_trait::async_trait;
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
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            )",
        )
        .execute(pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS encrypted_secret_values (
                name TEXT PRIMARY KEY REFERENCES secrets(name) ON DELETE CASCADE,
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
}

impl Debug for PostgresCatalogManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresCatalogManager")
            .field("pool", self.backend.pool())
            .finish()
    }
}
