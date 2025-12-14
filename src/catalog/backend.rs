//! Database backend abstraction for the catalog.
//!
//! This module provides a generic database backend that works with any sqlx-compatible
//! database (currently Postgres and SQLite). It abstracts over database-specific differences
//! like parameter binding syntax while providing a unified API for catalog operations.
//!
//! # Architecture
//!
//! The [`CatalogBackend`] struct is parameterized by a database type that implements
//! [`CatalogDatabase`]. This trait extends sqlx's `Database` trait with additional
//! functionality needed for cross-database compatibility.
//!
//! # Example
//!
//! ```ignore
//! use rivetdb::catalog::backend::CatalogBackend;
//! use sqlx::SqlitePool;
//!
//! let pool = SqlitePool::connect("sqlite::memory:").await?;
//! let backend = CatalogBackend::new(pool);
//! let connections = backend.list_connections().await?;
//! ```

use crate::catalog::manager::{ConnectionInfo, TableInfo};
use anyhow::{anyhow, Result};
use sqlx::{
    query, query_as, query_scalar, ColumnIndex, Database, Decode, Encode, Executor, FromRow,
    IntoArguments, Pool, Postgres, Sqlite, Type,
};
use std::borrow::Cow;

/// Extension trait for sqlx databases that provides catalog-specific functionality.
///
/// This trait handles differences in SQL syntax between database backends,
/// particularly parameter binding syntax (e.g., `$1` for Postgres vs `?` for SQLite).
pub trait CatalogDatabase: Database {
    /// Returns the parameter placeholder for the given 1-based index.
    ///
    /// - Postgres uses `$1`, `$2`, etc.
    /// - SQLite uses `?` for all parameters (index is ignored).
    fn bind_param(index: usize) -> Cow<'static, str>;
}

impl CatalogDatabase for Postgres {
    fn bind_param(index: usize) -> Cow<'static, str> {
        Cow::Owned(format!("${}", index))
    }
}

impl CatalogDatabase for Sqlite {
    fn bind_param(_: usize) -> Cow<'static, str> {
        Cow::Borrowed("?")
    }
}

/// Generic database backend for catalog operations.
///
/// Wraps a sqlx connection pool and provides methods for managing connections
/// and tables in the catalog. Works with any database that implements [`CatalogDatabase`].
pub struct CatalogBackend<DB: CatalogDatabase> {
    pool: Pool<DB>,
}

impl<DB: CatalogDatabase> CatalogBackend<DB> {
    /// Creates a new backend with the given connection pool.
    pub fn new(pool: Pool<DB>) -> Self {
        Self { pool }
    }

    /// Returns a reference to the underlying connection pool.
    pub fn pool(&self) -> &Pool<DB> {
        &self.pool
    }
}

impl<DB> CatalogBackend<DB>
where
    DB: CatalogDatabase,
    ConnectionInfo: for<'r> FromRow<'r, DB::Row>,
    TableInfo: for<'r> FromRow<'r, DB::Row>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> i32: Encode<'q, DB> + Type<DB>,
    for<'r> i32: Decode<'r, DB>,
    for<'q> <DB as Database>::Arguments<'q>: IntoArguments<'q, DB> + Send,
    for<'c> &'c Pool<DB>: Executor<'c, Database = DB>,
    usize: ColumnIndex<DB::Row>,
{
    pub async fn list_connections(&self) -> Result<Vec<ConnectionInfo>> {
        query_as::<DB, ConnectionInfo>(
            "SELECT id, name, source_type, config_json FROM connections ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub async fn add_connection(
        &self,
        name: &str,
        source_type: &str,
        config_json: &str,
    ) -> Result<i32> {
        let insert_sql = format!(
            "INSERT INTO connections (name, source_type, config_json) VALUES ({}, {}, {})",
            DB::bind_param(1),
            DB::bind_param(2),
            DB::bind_param(3)
        );

        query(&insert_sql)
            .bind(name)
            .bind(source_type)
            .bind(config_json)
            .execute(&self.pool)
            .await?;

        let select_sql = format!(
            "SELECT id FROM connections WHERE name = {}",
            DB::bind_param(1)
        );

        query_scalar::<DB, i32>(&select_sql)
            .bind(name)
            .fetch_one(&self.pool)
            .await
            .map_err(Into::into)
    }

    pub async fn get_connection(&self, name: &str) -> Result<Option<ConnectionInfo>> {
        let sql = format!(
            "SELECT id, name, source_type, config_json FROM connections WHERE name = {}",
            DB::bind_param(1)
        );

        query_as::<DB, ConnectionInfo>(&sql)
            .bind(name)
            .fetch_optional(&self.pool)
            .await
            .map_err(Into::into)
    }

    pub async fn add_table(
        &self,
        connection_id: i32,
        schema_name: &str,
        table_name: &str,
        arrow_schema_json: &str,
    ) -> Result<i32> {
        let insert_sql = format!(
            "INSERT INTO tables (connection_id, schema_name, table_name, arrow_schema_json) \
             VALUES ({}, {}, {}, {}) \
             ON CONFLICT (connection_id, schema_name, table_name) \
             DO UPDATE SET arrow_schema_json = excluded.arrow_schema_json",
            DB::bind_param(1),
            DB::bind_param(2),
            DB::bind_param(3),
            DB::bind_param(4)
        );

        query(&insert_sql)
            .bind(connection_id)
            .bind(schema_name)
            .bind(table_name)
            .bind(arrow_schema_json)
            .execute(&self.pool)
            .await?;

        let select_sql = format!(
            "SELECT id FROM tables WHERE connection_id = {} AND schema_name = {} AND table_name = {}",
            DB::bind_param(1),
            DB::bind_param(2),
            DB::bind_param(3),
        );

        query_scalar::<DB, i32>(&select_sql)
            .bind(connection_id)
            .bind(schema_name)
            .bind(table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(Into::into)
    }

    pub async fn list_tables(&self, connection_id: Option<i32>) -> Result<Vec<TableInfo>> {
        let mut sql = String::from(
            "SELECT id, connection_id, schema_name, table_name, parquet_path, \
             CAST(last_sync AS TEXT) as last_sync, arrow_schema_json \
             FROM tables",
        );

        if connection_id.is_some() {
            sql.push_str(" WHERE connection_id = ");
            sql.push_str(DB::bind_param(1).as_ref());
        }

        sql.push_str(" ORDER BY schema_name, table_name");

        let mut stmt = query_as::<DB, TableInfo>(&sql);
        if let Some(conn_id) = connection_id {
            stmt = stmt.bind(conn_id);
        }

        stmt.fetch_all(&self.pool).await.map_err(Into::into)
    }

    pub async fn get_table(
        &self,
        connection_id: i32,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<TableInfo>> {
        let sql = format!(
            "SELECT id, connection_id, schema_name, table_name, parquet_path, \
             CAST(last_sync AS TEXT) as last_sync, arrow_schema_json \
             FROM tables WHERE connection_id = {} AND schema_name = {} AND table_name = {}",
            DB::bind_param(1),
            DB::bind_param(2),
            DB::bind_param(3),
        );

        query_as::<DB, TableInfo>(&sql)
            .bind(connection_id)
            .bind(schema_name)
            .bind(table_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(Into::into)
    }

    pub async fn update_table_sync(&self, table_id: i32, parquet_path: &str) -> Result<()> {
        let sql = format!(
            "UPDATE tables SET parquet_path = {}, last_sync = CURRENT_TIMESTAMP \
             WHERE id = {}",
            DB::bind_param(1),
            DB::bind_param(2),
        );

        query(&sql)
            .bind(parquet_path)
            .bind(table_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn clear_table_cache_metadata(
        &self,
        connection_id: i32,
        schema_name: &str,
        table_name: &str,
    ) -> Result<TableInfo> {
        let table = self
            .get_table(connection_id, schema_name, table_name)
            .await?
            .ok_or_else(|| anyhow!("Table '{}.{}' not found", schema_name, table_name))?;

        let sql = format!(
            "UPDATE tables SET parquet_path = NULL, last_sync = NULL WHERE id = {}",
            DB::bind_param(1)
        );

        query(&sql).bind(table.id).execute(&self.pool).await?;

        Ok(table)
    }

    pub async fn clear_connection_cache_metadata(&self, name: &str) -> Result<()> {
        let connection = self
            .get_connection(name)
            .await?
            .ok_or_else(|| anyhow!("Connection '{}' not found", name))?;

        let sql = format!(
            "UPDATE tables SET parquet_path = NULL, last_sync = NULL \
             WHERE connection_id = {}",
            DB::bind_param(1)
        );

        query(&sql).bind(connection.id).execute(&self.pool).await?;

        Ok(())
    }

    pub async fn delete_connection(&self, name: &str) -> Result<()> {
        let connection = self
            .get_connection(name)
            .await?
            .ok_or_else(|| anyhow!("Connection '{}' not found", name))?;

        let delete_tables_sql = format!(
            "DELETE FROM tables WHERE connection_id = {}",
            DB::bind_param(1)
        );

        query(&delete_tables_sql)
            .bind(connection.id)
            .execute(&self.pool)
            .await?;

        let delete_connection_sql =
            format!("DELETE FROM connections WHERE id = {}", DB::bind_param(1));

        query(&delete_connection_sql)
            .bind(connection.id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}
