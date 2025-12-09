//! PostgreSQL native driver implementation using sqlx

use sqlx::postgres::PgConnection;
use sqlx::{Connection, Row};

use crate::datafetch::{ColumnMetadata, DataFetchError, TableMetadata};
use crate::source::Source;

use super::arrow_convert::pg_type_to_arrow;
use super::StreamingParquetWriter;

/// Connect to PostgreSQL with automatic SSL retry.
/// If the initial connection fails with an "insecure connection" error,
/// automatically retries with `sslmode=require` appended to the connection string.
async fn connect_with_ssl_retry(connection_string: &str) -> Result<PgConnection, sqlx::Error> {
    match PgConnection::connect(connection_string).await {
        Ok(conn) => Ok(conn),
        Err(e) => {
            let error_msg = e.to_string();
            // Check if the error indicates SSL is required
            if error_msg.contains("connection is insecure")
                || error_msg.contains("sslmode=require")
            {
                // Append sslmode=require and retry
                let ssl_connection_string = if connection_string.contains('?') {
                    format!("{}&sslmode=require", connection_string)
                } else {
                    format!("{}?sslmode=require", connection_string)
                };
                PgConnection::connect(&ssl_connection_string).await
            } else {
                Err(e)
            }
        }
    }
}

/// Discover tables and columns from PostgreSQL
pub async fn discover_tables(source: &Source) -> Result<Vec<TableMetadata>, DataFetchError> {
    let mut conn = connect_with_ssl_retry(&source.connection_string()).await?;

    let rows = sqlx::query(
        r#"
        SELECT
            t.table_catalog,
            t.table_schema,
            t.table_name,
            t.table_type,
            c.column_name,
            c.data_type,
            c.is_nullable,
            c.ordinal_position::int
        FROM information_schema.tables t
        JOIN information_schema.columns c
            ON t.table_catalog = c.table_catalog
            AND t.table_schema = c.table_schema
            AND t.table_name = c.table_name
        WHERE t.table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY t.table_schema, t.table_name, c.ordinal_position
        "#,
    )
    .fetch_all(&mut conn)
    .await?;

    let mut tables: Vec<TableMetadata> = Vec::new();

    for row in rows {
        let catalog: Option<String> = row.get(0);
        let schema: String = row.get(1);
        let table: String = row.get(2);
        let table_type: String = row.get(3);
        let col_name: String = row.get(4);
        let data_type: String = row.get(5);
        let is_nullable: String = row.get(6);
        let ordinal: i32 = row.get(7);

        let column = ColumnMetadata {
            name: col_name,
            data_type: pg_type_to_arrow(&data_type),
            nullable: is_nullable.to_uppercase() == "YES",
            ordinal_position: ordinal,
        };

        // Find or create table entry
        if let Some(existing) = tables.iter_mut().find(|t| {
            t.catalog_name == catalog && t.schema_name == schema && t.table_name == table
        }) {
            existing.columns.push(column);
        } else {
            tables.push(TableMetadata {
                catalog_name: catalog,
                schema_name: schema,
                table_name: table,
                table_type,
                columns: vec![column],
            });
        }
    }

    Ok(tables)
}

/// Fetch table data and write to Parquet
pub async fn fetch_table(
    source: &Source,
    _catalog: Option<&str>,
    schema: &str,
    table: &str,
    writer: &mut StreamingParquetWriter,
) -> Result<(), DataFetchError> {
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    use super::arrow_convert::{rows_to_batch, schema_from_columns};

    let mut conn = connect_with_ssl_retry(&source.connection_string()).await?;

    // Build query - properly escape identifiers
    let query = format!(
        "SELECT * FROM \"{}\".\"{}\"",
        schema.replace('"', "\"\""),
        table.replace('"', "\"\"")
    );

    // Fetch all rows
    let rows = sqlx::query(&query).fetch_all(&mut conn).await?;

    // Extract schema
    let arrow_schema = if !rows.is_empty() {
        // Non-empty: extract schema from first row's columns
        schema_from_columns(rows[0].columns())
    } else {
        // Empty table: query information_schema for schema
        let schema_rows = sqlx::query(
            r#"
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            ORDER BY ordinal_position
            "#,
        )
        .bind(schema)
        .bind(table)
        .fetch_all(&mut conn)
        .await?;

        if schema_rows.is_empty() {
            return Err(DataFetchError::Query(format!(
                "Table {}.{} has no columns",
                schema, table
            )));
        }

        let fields: Vec<Field> = schema_rows
            .iter()
            .map(|row| {
                let col_name: String = row.get(0);
                let data_type: String = row.get(1);
                let is_nullable: String = row.get(2);
                Field::new(
                    col_name,
                    pg_type_to_arrow(&data_type),
                    is_nullable.to_uppercase() == "YES",
                )
            })
            .collect();

        Schema::new(fields)
    };

    // Initialize writer with schema
    writer.init(&arrow_schema)?;

    // Write batches
    if rows.is_empty() {
        let empty_batch = RecordBatch::new_empty(Arc::new(arrow_schema));
        writer.write_batch(&empty_batch)?;
    } else {
        const BATCH_SIZE: usize = 10_000;
        for chunk in rows.chunks(BATCH_SIZE) {
            let batch = rows_to_batch(chunk, &arrow_schema)?;
            writer.write_batch(&batch)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pg_type_mapping() {
        use datafusion::arrow::datatypes::DataType;

        assert!(matches!(pg_type_to_arrow("integer"), DataType::Int32));
        assert!(matches!(pg_type_to_arrow("varchar"), DataType::Utf8));
        assert!(matches!(
            pg_type_to_arrow("character varying"),
            DataType::Utf8
        ));
        assert!(matches!(pg_type_to_arrow("boolean"), DataType::Boolean));
        assert!(matches!(pg_type_to_arrow("bigint"), DataType::Int64));
        assert!(matches!(pg_type_to_arrow("bytea"), DataType::Binary));
    }
}