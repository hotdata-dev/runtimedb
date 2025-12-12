use crate::catalog::manager::{CatalogManager, ConnectionInfo, TableInfo};
use crate::catalog::migrations::{run_migrations, CatalogMigrations};
use crate::catalog::store::CatalogStore;
use anyhow::Result;
use futures::future::BoxFuture;
use futures::FutureExt;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres};
use std::fmt::{self, Debug, Formatter};
use tokio::task::block_in_place;

pub struct PostgresCatalogManager {
    store: CatalogStore<Postgres>,
}

impl PostgresCatalogManager {
    pub fn new(connection_string: &str) -> Result<Self> {
        let pool = block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let pool = PgPoolOptions::new()
                    .max_connections(5)
                    .connect(connection_string)
                    .await?;

                Ok::<_, anyhow::Error>(pool)
            })
        })?;

        let store = CatalogStore::new(pool);
        Ok(Self { store })
    }

    fn block_on<F, T>(&self, f: F) -> Result<T>
    where
        F: std::future::Future<Output = Result<T>>,
    {
        block_in_place(|| tokio::runtime::Handle::current().block_on(f))
    }

    async fn run_migrations_async(pool: &PgPool) -> Result<()> {
        run_migrations::<PostgresMigrationBackend>(pool).await
    }

    fn postgres_initialize_schema(pool: &PgPool) -> BoxFuture<'_, Result<()>> {
        async move {
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
                    state_path TEXT,
                    last_sync TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    arrow_schema_json TEXT,
                    FOREIGN KEY (connection_id) REFERENCES connections(id),
                    UNIQUE (connection_id, schema_name, table_name)
                )",
            )
            .execute(pool)
            .await?;

            Ok(())
        }
        .boxed()
    }
}

struct PostgresMigrationBackend;

impl CatalogMigrations for PostgresMigrationBackend {
    type Pool = PgPool;

    fn ensure_migrations_table(pool: &Self::Pool) -> BoxFuture<'_, Result<()>> {
        async move {
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
            sqlx::query("INSERT INTO schema_migrations (version) VALUES ($1)")
                .bind(version)
                .execute(pool)
                .await?;
            Ok(())
        }
        .boxed()
    }
    fn migrate_v1(pool: &Self::Pool) -> BoxFuture<'_, Result<()>> {
        PostgresCatalogManager::postgres_initialize_schema(pool)
    }
}

impl CatalogManager for PostgresCatalogManager {
    fn close(&self) -> Result<()> {
        // sqlx pool handles connection cleanup automatically
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

impl Debug for PostgresCatalogManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresCatalogManager")
            .field("pool", self.store.pool())
            .finish()
    }
}
