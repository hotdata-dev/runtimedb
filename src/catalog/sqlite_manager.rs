use crate::catalog::manager::{CatalogManager, ConnectionInfo, TableInfo};
use crate::catalog::migrations::{run_migrations, CatalogMigrations};
use crate::catalog::store::CatalogStore;
use anyhow::Result;
use futures::future::BoxFuture;
use futures::FutureExt;
use sqlx::{Sqlite, SqlitePool};
use std::fmt::Debug;
use tokio::task::block_in_place;

pub struct SqliteCatalogManager {
    store: CatalogStore<Sqlite>,
    catalog_path: String,
}

struct SqliteMigrationBackend;

impl SqliteCatalogManager {
    pub fn new(db_path: &str) -> Result<Self> {
        let pool = block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let uri = format!("sqlite:{}?mode=rwc", db_path);
                let pool = SqlitePool::connect(&uri).await?;
                Ok::<_, anyhow::Error>(pool)
            })
        })?;

        let store = CatalogStore::new(pool);

        Ok(Self {
            store,
            catalog_path: db_path.to_string(),
        })
    }

    async fn run_migrations_async(pool: &SqlitePool) -> Result<()> {
        run_migrations::<SqliteMigrationBackend>(pool).await
    }

    fn sqlite_initialize_schema(pool: &SqlitePool) -> BoxFuture<'_, Result<()>> {
        async move {
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
                    state_path TEXT,
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

            Ok(())
        }
        .boxed()
    }

    fn block_on<F, T>(&self, f: F) -> Result<T>
    where
        F: std::future::Future<Output = Result<T>>,
    {
        block_in_place(|| tokio::runtime::Handle::current().block_on(f))
    }
}

impl CatalogManager for SqliteCatalogManager {
    fn close(&self) -> Result<()> {
        // sqlx pools do not need explicit close
        Ok(())
    }

    fn run_migrations(&self) -> Result<()> {
        self.block_on(Self::run_migrations_async(self.store.pool()))
    }

    fn list_connections(&self) -> Result<Vec<ConnectionInfo>> {
        self.block_on(self.store.list_connections())
    }

    fn add_connection(&self, name: &str, source_type: &str, config_json: &str) -> Result<i32> {
        self.block_on(self.store.add_connection(name, source_type, config_json))
    }

    fn get_connection(&self, name: &str) -> Result<Option<ConnectionInfo>> {
        self.block_on(self.store.get_connection(name))
    }

    fn add_table(
        &self,
        connection_id: i32,
        schema_name: &str,
        table_name: &str,
        arrow_schema_json: &str,
    ) -> Result<i32> {
        self.block_on(self.store.add_table(
            connection_id,
            schema_name,
            table_name,
            arrow_schema_json,
        ))
    }

    fn list_tables(&self, connection_id: Option<i32>) -> Result<Vec<TableInfo>> {
        self.block_on(self.store.list_tables(connection_id))
    }

    fn get_table(
        &self,
        connection_id: i32,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<TableInfo>> {
        self.block_on(self.store.get_table(connection_id, schema_name, table_name))
    }

    fn update_table_sync(&self, table_id: i32, parquet_path: &str, state_path: &str) -> Result<()> {
        self.block_on(
            self.store
                .update_table_sync(table_id, parquet_path, state_path),
        )
    }

    fn clear_table_cache_metadata(
        &self,
        connection_id: i32,
        schema_name: &str,
        table_name: &str,
    ) -> Result<TableInfo> {
        self.block_on(
            self.store
                .clear_table_cache_metadata(connection_id, schema_name, table_name),
        )
    }

    fn clear_connection_cache_metadata(&self, name: &str) -> Result<()> {
        self.block_on(self.store.clear_connection_cache_metadata(name))
    }

    fn delete_connection(&self, name: &str) -> Result<()> {
        self.block_on(self.store.delete_connection(name))
    }
}

impl Debug for SqliteCatalogManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteCatalogManager")
            .field("catalog_path", &self.catalog_path)
            .finish()
    }
}

impl CatalogMigrations for SqliteMigrationBackend {
    type Pool = SqlitePool;

    fn ensure_migrations_table(pool: &Self::Pool) -> BoxFuture<'_, Result<()>> {
        async move {
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
        .boxed()
    }

    fn current_version(pool: &Self::Pool) -> BoxFuture<'_, Result<i64>> {
        async move {
            sqlx::query_scalar("SELECT COALESCE(MAX(version), 0) FROM schema_migrations")
                .fetch_one(pool)
                .await
                .map_err(Into::into)
        }
        .boxed()
    }

    fn record_version(pool: &Self::Pool, version: i64) -> BoxFuture<'_, Result<()>> {
        async move {
            sqlx::query("INSERT INTO schema_migrations (version) VALUES (?)")
                .bind(version)
                .execute(pool)
                .await?;
            Ok(())
        }
        .boxed()
    }
    fn migrate_v1(pool: &Self::Pool) -> BoxFuture<'_, Result<()>> {
        SqliteCatalogManager::sqlite_initialize_schema(pool)
    }
}
