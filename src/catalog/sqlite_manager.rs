use crate::catalog::backend::CatalogBackend;
use crate::catalog::manager::{CatalogManager, ConnectionInfo, TableInfo};
use crate::catalog::migrations::{run_migrations, CatalogMigrations};
use anyhow::Result;
use async_trait::async_trait;
use sqlx::{Sqlite, SqlitePool};
use std::fmt::{self, Debug, Formatter};

pub struct SqliteCatalogManager {
    backend: CatalogBackend<Sqlite>,
    catalog_path: String,
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
            CREATE TABLE IF NOT EXISTS connections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            CREATE TABLE IF NOT EXISTS tables (
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
            CREATE TABLE IF NOT EXISTS secrets (
                name TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                provider_ref TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            "#,
        )
        .execute(pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS encrypted_secret_values (
                name TEXT PRIMARY KEY REFERENCES secrets(name) ON DELETE CASCADE,
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
}
