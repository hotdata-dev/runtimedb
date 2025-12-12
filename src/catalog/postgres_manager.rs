use crate::catalog::manager::{CatalogManager, ConnectionInfo, TableInfo};
use crate::catalog::store::CatalogStore;
use anyhow::Result;
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

                Self::initialize_schema_async(&pool).await?;
                Ok::<_, anyhow::Error>(pool)
            })
        })?;

        let store = CatalogStore::new(pool);
        Ok(Self { store })
    }

    async fn initialize_schema_async(pool: &PgPool) -> Result<()> {
        // Create connections table
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

        // Create tables table
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

        // Add arrow_schema_json column if it doesn't exist (migration for older schemas)
        // This is safe to run even if column exists due to IF NOT EXISTS
        let _ = sqlx::query("ALTER TABLE tables ADD COLUMN IF NOT EXISTS arrow_schema_json TEXT")
            .execute(pool)
            .await;

        Ok(())
    }

    fn block_on<F, T>(&self, f: F) -> Result<T>
    where
        F: std::future::Future<Output = Result<T>>,
    {
        block_in_place(|| tokio::runtime::Handle::current().block_on(f))
    }
}

impl CatalogManager for PostgresCatalogManager {
    fn close(&self) -> Result<()> {
        // sqlx pool handles connection cleanup automatically
        Ok(())
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
