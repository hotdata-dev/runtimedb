use crate::catalog::manager::{CatalogManager, ConnectionInfo, TableInfo};
use anyhow::Result;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use std::fmt::{self, Debug, Formatter};
use tokio::task::block_in_place;

pub struct PostgresCatalogManager {
    pool: PgPool,
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

        Ok(Self { pool })
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
        self.block_on(async {
            let rows = sqlx::query(
                "SELECT id, name, source_type, config_json FROM connections ORDER BY name",
            )
            .fetch_all(&self.pool)
            .await?;

            let connections = rows
                .iter()
                .map(|row| ConnectionInfo {
                    id: row.get("id"),
                    name: row.get("name"),
                    source_type: row.get("source_type"),
                    config_json: row.get("config_json"),
                })
                .collect();

            Ok(connections)
        })
    }

    fn add_connection(&self, name: &str, source_type: &str, config_json: &str) -> Result<i32> {
        self.block_on(async {
            let row = sqlx::query(
                "INSERT INTO connections (name, source_type, config_json) VALUES ($1, $2, $3) RETURNING id",
            )
            .bind(name)
            .bind(source_type)
            .bind(config_json)
            .fetch_one(&self.pool)
            .await?;

            Ok(row.get("id"))
        })
    }

    fn get_connection(&self, name: &str) -> Result<Option<ConnectionInfo>> {
        self.block_on(async {
            let row = sqlx::query(
                "SELECT id, name, source_type, config_json FROM connections WHERE name = $1",
            )
            .bind(name)
            .fetch_optional(&self.pool)
            .await?;

            Ok(row.map(|r| ConnectionInfo {
                id: r.get("id"),
                name: r.get("name"),
                source_type: r.get("source_type"),
                config_json: r.get("config_json"),
            }))
        })
    }

    fn add_table(
        &self,
        connection_id: i32,
        schema_name: &str,
        table_name: &str,
        arrow_schema_json: &str,
    ) -> Result<i32> {
        self.block_on(async {
            // Insert or update table with schema
            sqlx::query(
                "INSERT INTO tables (connection_id, schema_name, table_name, arrow_schema_json)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (connection_id, schema_name, table_name)
                 DO UPDATE SET arrow_schema_json = $4",
            )
            .bind(connection_id)
            .bind(schema_name)
            .bind(table_name)
            .bind(arrow_schema_json)
            .execute(&self.pool)
            .await?;

            let row = sqlx::query(
                "SELECT id FROM tables WHERE connection_id = $1 AND schema_name = $2 AND table_name = $3",
            )
            .bind(connection_id)
            .bind(schema_name)
            .bind(table_name)
            .fetch_one(&self.pool)
            .await?;

            Ok(row.get("id"))
        })
    }

    fn list_tables(&self, connection_id: Option<i32>) -> Result<Vec<TableInfo>> {
        self.block_on(async {
            let rows = if let Some(conn_id) = connection_id {
                sqlx::query(
                    "SELECT id, connection_id, schema_name, table_name, parquet_path, state_path, \
                     CAST(last_sync AS VARCHAR) as last_sync, arrow_schema_json \
                     FROM tables WHERE connection_id = $1 ORDER BY schema_name, table_name",
                )
                .bind(conn_id)
                .fetch_all(&self.pool)
                .await?
            } else {
                sqlx::query(
                    "SELECT id, connection_id, schema_name, table_name, parquet_path, state_path, \
                     CAST(last_sync AS VARCHAR) as last_sync, arrow_schema_json \
                     FROM tables ORDER BY schema_name, table_name",
                )
                .fetch_all(&self.pool)
                .await?
            };

            let tables = rows
                .iter()
                .map(|row| TableInfo {
                    id: row.get("id"),
                    connection_id: row.get("connection_id"),
                    schema_name: row.get("schema_name"),
                    table_name: row.get("table_name"),
                    parquet_path: row.get("parquet_path"),
                    state_path: row.get("state_path"),
                    last_sync: row.get("last_sync"),
                    arrow_schema_json: row.get("arrow_schema_json"),
                })
                .collect();

            Ok(tables)
        })
    }

    fn get_table(
        &self,
        connection_id: i32,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<TableInfo>> {
        self.block_on(async {
            let row = sqlx::query(
                "SELECT id, connection_id, schema_name, table_name, parquet_path, state_path, \
                 CAST(last_sync AS VARCHAR) as last_sync, arrow_schema_json \
                 FROM tables WHERE connection_id = $1 AND schema_name = $2 AND table_name = $3",
            )
            .bind(connection_id)
            .bind(schema_name)
            .bind(table_name)
            .fetch_optional(&self.pool)
            .await?;

            Ok(row.map(|r| TableInfo {
                id: r.get("id"),
                connection_id: r.get("connection_id"),
                schema_name: r.get("schema_name"),
                table_name: r.get("table_name"),
                parquet_path: r.get("parquet_path"),
                state_path: r.get("state_path"),
                last_sync: r.get("last_sync"),
                arrow_schema_json: r.get("arrow_schema_json"),
            }))
        })
    }

    fn update_table_sync(&self, table_id: i32, parquet_path: &str, state_path: &str) -> Result<()> {
        self.block_on(async {
            sqlx::query(
                "UPDATE tables SET parquet_path = $1, state_path = $2, last_sync = CURRENT_TIMESTAMP WHERE id = $3",
            )
            .bind(parquet_path)
            .bind(state_path)
            .bind(table_id)
            .execute(&self.pool)
            .await?;
            Ok(())
        })
    }

    fn clear_table_cache_metadata(
        &self,
        connection_id: i32,
        schema_name: &str,
        table_name: &str,
    ) -> Result<TableInfo> {
        // Get table info before clearing
        let table_info = self
            .get_table(connection_id, schema_name, table_name)?
            .ok_or_else(|| anyhow::anyhow!("Table '{}.{}' not found", schema_name, table_name))?;

        // Update table entry to NULL out paths and sync time
        self.block_on(async {
            sqlx::query(
                "UPDATE tables SET parquet_path = NULL, state_path = NULL, last_sync = NULL WHERE id = $1",
            )
            .bind(table_info.id)
            .execute(&self.pool)
            .await?;

            Ok(table_info)
        })
    }

    fn clear_connection_cache_metadata(&self, name: &str) -> Result<()> {
        // Get connection info
        let conn_info = self
            .get_connection(name)?
            .ok_or_else(|| anyhow::anyhow!("Connection '{}' not found", name))?;

        // Update all table entries to NULL out paths and sync time
        self.block_on(async {
            sqlx::query(
                "UPDATE tables SET parquet_path = NULL, state_path = NULL, last_sync = NULL WHERE connection_id = $1",
            )
            .bind(conn_info.id)
            .execute(&self.pool)
            .await?;

            Ok(())
        })
    }

    fn delete_connection(&self, name: &str) -> Result<()> {
        // Get connection info
        let conn_info = self
            .get_connection(name)?
            .ok_or_else(|| anyhow::anyhow!("Connection '{}' not found", name))?;

        // Delete from database
        self.block_on(async {
            sqlx::query("DELETE FROM tables WHERE connection_id = $1")
                .bind(conn_info.id)
                .execute(&self.pool)
                .await?;

            sqlx::query("DELETE FROM connections WHERE id = $1")
                .bind(conn_info.id)
                .execute(&self.pool)
                .await?;

            Ok(())
        })
    }
}

impl Debug for PostgresCatalogManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresCatalogManager")
            .field("pool", &"<sqlx PgPool>")
            .finish()
    }
}
