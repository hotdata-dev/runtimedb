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
//! use runtimedb::catalog::backend::CatalogBackend;
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
    for<'q> String: Encode<'q, DB> + Type<DB>,
    for<'q> Option<&'q str>: Encode<'q, DB> + Type<DB>,
    for<'q> i32: Encode<'q, DB> + Type<DB>,
    for<'r> i32: Decode<'r, DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB>,
    for<'r> i64: Decode<'r, DB>,
    for<'q> <DB as Database>::Arguments<'q>: IntoArguments<'q, DB> + Send,
    for<'c> &'c Pool<DB>: Executor<'c, Database = DB>,
    usize: ColumnIndex<DB::Row>,
{
    pub async fn list_connections(&self) -> Result<Vec<ConnectionInfo>> {
        query_as::<DB, ConnectionInfo>(
            "SELECT external_id as id, name, source_type, config_json, secret_id FROM connections ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    #[tracing::instrument(
        name = "catalog_add_connection",
        skip(self, config_json),
        fields(
            runtimedb.connection_name = %name,
            runtimedb.source_type = %source_type,
        )
    )]
    pub async fn add_connection(
        &self,
        name: &str,
        source_type: &str,
        config_json: &str,
        secret_id: Option<&str>,
    ) -> Result<String> {
        // Retry logic handles the astronomically rare case of nanoid collision.
        // With 26-char nanoid (alphabet of 64 chars), collision probability is < 1 in 10^40.
        const MAX_RETRIES: usize = 3;

        for attempt in 0..MAX_RETRIES {
            let external_id = crate::id::generate_connection_id();
            let insert_sql = format!(
                "INSERT INTO connections (external_id, name, source_type, config_json, secret_id) VALUES ({}, {}, {}, {}, {})",
                DB::bind_param(1),
                DB::bind_param(2),
                DB::bind_param(3),
                DB::bind_param(4),
                DB::bind_param(5)
            );

            let result = query(&insert_sql)
                .bind(external_id.as_str())
                .bind(name)
                .bind(source_type)
                .bind(config_json)
                .bind(secret_id)
                .execute(&self.pool)
                .await;

            match result {
                Ok(_) => {
                    // Success - return the external_id we just inserted
                    return Ok(external_id);
                }
                Err(sqlx::Error::Database(db_err)) => {
                    // Check for unique constraint violation using structured error inspection
                    let is_unique_violation = match db_err.code().map(|c| c.to_string()).as_deref()
                    {
                        // Postgres: 23505 = unique_violation
                        Some("23505") => true,
                        // SQLite: 2067 = SQLITE_CONSTRAINT_UNIQUE (returned as string "2067")
                        Some("2067") => true,
                        // SQLite may also report 1555 for SQLITE_CONSTRAINT_PRIMARYKEY
                        Some("1555") => true,
                        _ => false,
                    };

                    if is_unique_violation {
                        // Check if the violation is on external_id (retry) or name (conflict error)
                        let msg = db_err.message().to_lowercase();
                        if msg.contains("external_id") && attempt < MAX_RETRIES - 1 {
                            // Retry with new ID
                            continue;
                        }
                        // Violation on name or max retries - return the error
                    }
                    return Err(sqlx::Error::Database(db_err).into());
                }
                Err(e) => {
                    // Other error types - don't retry
                    return Err(e.into());
                }
            }
        }

        Err(anyhow!(
            "Failed to generate unique connection ID after {} attempts",
            MAX_RETRIES
        ))
    }

    pub async fn get_connection(&self, id: &str) -> Result<Option<ConnectionInfo>> {
        let sql = format!(
            "SELECT external_id as id, name, source_type, config_json, secret_id FROM connections WHERE external_id = {}",
            DB::bind_param(1)
        );

        query_as::<DB, ConnectionInfo>(&sql)
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(Into::into)
    }

    pub async fn get_connection_by_name(&self, name: &str) -> Result<Option<ConnectionInfo>> {
        let sql = format!(
            "SELECT external_id as id, name, source_type, config_json, secret_id FROM connections WHERE name = {}",
            DB::bind_param(1)
        );

        query_as::<DB, ConnectionInfo>(&sql)
            .bind(name)
            .fetch_optional(&self.pool)
            .await
            .map_err(Into::into)
    }

    #[tracing::instrument(
        name = "catalog_add_table",
        skip(self, arrow_schema_json),
        fields(
            runtimedb.connection_id = %connection_id,
            runtimedb.schema = %schema_name,
            runtimedb.table = %table_name,
        )
    )]
    pub async fn add_table(
        &self,
        connection_id: &str,
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

    pub async fn list_tables(&self, connection_id: Option<&str>) -> Result<Vec<TableInfo>> {
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
            stmt = stmt.bind(conn_id.to_string());
        }

        stmt.fetch_all(&self.pool).await.map_err(Into::into)
    }

    pub async fn get_table(
        &self,
        connection_id: &str,
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

    #[tracing::instrument(
        name = "catalog_clear_table_cache",
        skip(self),
        fields(
            runtimedb.connection_id = %connection_id,
            runtimedb.schema = %schema_name,
            runtimedb.table = %table_name,
        )
    )]
    pub async fn clear_table_cache_metadata(
        &self,
        connection_id: &str,
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

    #[tracing::instrument(
        name = "catalog_clear_connection_cache",
        skip(self),
        fields(runtimedb.connection_id = %connection_id)
    )]
    pub async fn clear_connection_cache_metadata(&self, connection_id: &str) -> Result<()> {
        let sql = format!(
            "UPDATE tables SET parquet_path = NULL, last_sync = NULL \
             WHERE connection_id = {}",
            DB::bind_param(1)
        );

        query(&sql).bind(connection_id).execute(&self.pool).await?;

        Ok(())
    }

    #[tracing::instrument(
        name = "catalog_delete_connection",
        skip(self),
        fields(runtimedb.connection_id = %connection_id)
    )]
    pub async fn delete_connection(&self, connection_id: &str) -> Result<()> {
        let delete_tables_sql = format!(
            "DELETE FROM tables WHERE connection_id = {}",
            DB::bind_param(1)
        );

        query(&delete_tables_sql)
            .bind(connection_id)
            .execute(&self.pool)
            .await?;

        let delete_connection_sql = format!(
            "DELETE FROM connections WHERE external_id = {}",
            DB::bind_param(1)
        );

        query(&delete_connection_sql)
            .bind(connection_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn remove_pending_deletion(&self, id: i32) -> Result<()> {
        let sql = format!(
            "DELETE FROM pending_deletions WHERE id = {}",
            DB::bind_param(1)
        );
        query(&sql).bind(id).execute(&self.pool).await?;
        Ok(())
    }

    /// Count how many connections reference a given secret_id.
    pub async fn count_connections_by_secret_id(&self, secret_id: &str) -> Result<i64> {
        let sql = format!(
            "SELECT COUNT(*) FROM connections WHERE secret_id = {}",
            DB::bind_param(1)
        );
        let count: i64 = query_scalar(&sql)
            .bind(secret_id)
            .fetch_one(&self.pool)
            .await?;
        Ok(count)
    }
}
