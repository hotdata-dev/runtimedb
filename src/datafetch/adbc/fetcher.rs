use adbc_core::options::{AdbcVersion, OptionDatabase, OptionValue};
use adbc_core::{Connection, Database, Driver, Statement};
use adbc_driver_manager::ManagedDriver;
use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;

use crate::datafetch::{ConnectionConfig, DataFetchError, DataFetcher, TableMetadata};
use crate::storage::StorageManager;

use super::DriverManager;

/// ADBC-based data fetcher implementation
#[derive(Debug)]
pub struct AdbcFetcher {
    driver_manager: DriverManager,
}

impl AdbcFetcher {
    pub fn new() -> Self {
        Self {
            driver_manager: DriverManager::new(),
        }
    }

    /// Create an ADBC connection to the remote database
    fn connect(
        &self,
        config: &ConnectionConfig,
    ) -> Result<impl adbc_core::Connection, DataFetchError> {
        let driver_path = self.driver_manager.driver_path(&config.source_type)?;
        let entrypoint = self.driver_manager.driver_entrypoint(&config.source_type);

        // Load the driver
        let mut driver: ManagedDriver = ManagedDriver::load_dynamic_from_filename(
            driver_path,
            Some(entrypoint.as_bytes()),
            AdbcVersion::V110,
        )
        .map_err(|e| DataFetchError::DriverLoad(e.to_string()))?;

        // Create database with URI option
        let database = driver
            .new_database_with_opts([(
                OptionDatabase::Uri,
                OptionValue::String(config.connection_string.clone()),
            )])
            .map_err(|e| DataFetchError::Connection(e.to_string()))?;

        // Create and return connection
        let connection = database
            .new_connection()
            .map_err(|e| DataFetchError::Connection(e.to_string()))?;

        Ok(connection)
    }

    /// Parse get_objects result into TableMetadata
    fn parse_objects_result(
        &self,
        reader: impl Iterator<Item = Result<datafusion::arrow::record_batch::RecordBatch, datafusion::arrow::error::ArrowError>>,
    ) -> Result<Vec<TableMetadata>, DataFetchError> {
        let mut tables = Vec::new();

        for batch_result in reader {
            let batch = batch_result.map_err(|e| DataFetchError::Discovery(e.to_string()))?;

            // get_objects returns a nested structure:
            // catalog_name, catalog_db_schemas (list of schemas)
            //   -> db_schema_name, db_schema_tables (list of tables)
            //     -> table_name, table_type, table_columns (list of columns)
            //       -> column_name, ordinal_position, xdbc_data_type, etc.

            // This is complex nested Arrow data - we'll need to traverse it
            // For now, extract what we can from the batch
            tables.extend(self.extract_tables_from_batch(&batch)?);
        }

        Ok(tables)
    }

    fn extract_tables_from_batch(
        &self,
        batch: &datafusion::arrow::record_batch::RecordBatch,
    ) -> Result<Vec<TableMetadata>, DataFetchError> {
        use datafusion::arrow::array::{Array, ListArray, StringArray, StructArray};

        let mut tables = Vec::new();

        // Column 0: catalog_name (string)
        // Column 1: catalog_db_schemas (list<struct>)
        let catalog_names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFetchError::Discovery("Invalid catalog_name column".into()))?;

        let catalog_schemas = batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFetchError::Discovery("Invalid catalog_db_schemas column".into()))?;

        for catalog_idx in 0..batch.num_rows() {
            let catalog_name = catalog_names.value(catalog_idx);

            // Get the list of schemas for this catalog
            let schemas_list = catalog_schemas.value(catalog_idx);
            let schemas_struct = schemas_list
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| DataFetchError::Discovery("Invalid schema struct".into()))?;

            // Schema struct has: db_schema_name, db_schema_tables
            let schema_names = schemas_struct
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFetchError::Discovery("Invalid db_schema_name".into()))?;

            let schema_tables = schemas_struct
                .column(1)
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| DataFetchError::Discovery("Invalid db_schema_tables".into()))?;

            for schema_idx in 0..schemas_struct.len() {
                let schema_name = schema_names.value(schema_idx);

                // Get tables for this schema
                let tables_list = schema_tables.value(schema_idx);
                let tables_struct = tables_list
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| DataFetchError::Discovery("Invalid table struct".into()))?;

                // Table struct has: table_name, table_type, table_columns, table_constraints
                let table_names = tables_struct
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| DataFetchError::Discovery("Invalid table_name".into()))?;

                let table_types = tables_struct
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| DataFetchError::Discovery("Invalid table_type".into()))?;

                let table_columns_list = tables_struct
                    .column(2)
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| DataFetchError::Discovery("Invalid table_columns".into()))?;

                for table_idx in 0..tables_struct.len() {
                    let table_name = table_names.value(table_idx);
                    let table_type = table_types.value(table_idx);

                    // Parse columns
                    let columns = self.extract_columns(table_columns_list.value(table_idx))?;

                    tables.push(TableMetadata {
                        catalog_name: if catalog_name.is_empty() {
                            None
                        } else {
                            Some(catalog_name.to_string())
                        },
                        schema_name: schema_name.to_string(),
                        table_name: table_name.to_string(),
                        table_type: table_type.to_string(),
                        columns,
                    });
                }
            }
        }

        Ok(tables)
    }

    fn extract_columns(
        &self,
        columns_array: datafusion::arrow::array::ArrayRef,
    ) -> Result<Vec<crate::datafetch::ColumnMetadata>, DataFetchError> {
        use crate::datafetch::ColumnMetadata;
        use datafusion::arrow::array::{Array, Int16Array, StructArray};

        let columns_struct = columns_array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| DataFetchError::Discovery("Invalid columns struct".into()))?;

        // Column struct has: column_name, ordinal_position, remarks, xdbc_data_type, etc.
        let column_names = columns_struct
            .column_by_name("column_name")
            .and_then(|c| c.as_any().downcast_ref::<datafusion::arrow::array::StringArray>())
            .ok_or_else(|| DataFetchError::Discovery("Invalid column_name".into()))?;

        let ordinal_positions = columns_struct
            .column_by_name("ordinal_position")
            .and_then(|c| c.as_any().downcast_ref::<Int16Array>())
            .ok_or_else(|| DataFetchError::Discovery("Invalid ordinal_position".into()))?;

        // xdbc_data_type is the SQL type code
        let data_types = columns_struct
            .column_by_name("xdbc_data_type")
            .and_then(|c| c.as_any().downcast_ref::<Int16Array>());

        let nullables = columns_struct
            .column_by_name("xdbc_nullable")
            .and_then(|c| c.as_any().downcast_ref::<Int16Array>());

        let mut columns = Vec::new();

        for i in 0..columns_struct.len() {
            let name = column_names.value(i).to_string();
            let ordinal = ordinal_positions.value(i);

            // Convert SQL type code to Arrow type (simplified)
            let sql_type = data_types.map(|dt| dt.value(i)).unwrap_or(12); // 12 = VARCHAR default
            let arrow_type = sql_type_to_arrow(sql_type);

            // Nullable: 1 = nullable, 0 = not nullable
            let nullable = nullables.map(|n| n.value(i) != 0).unwrap_or(true);

            columns.push(ColumnMetadata {
                name,
                data_type: arrow_type,
                nullable,
                ordinal_position: ordinal,
            });
        }

        // Sort by ordinal position
        columns.sort_by_key(|c| c.ordinal_position);

        Ok(columns)
    }
}

impl Default for AdbcFetcher {
    fn default() -> Self {
        Self::new()
    }
}

/// Quote a SQL identifier to prevent injection
/// Wraps the identifier in double quotes and escapes any internal double quotes
fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[async_trait]
impl DataFetcher for AdbcFetcher {
    async fn discover_tables(
        &self,
        config: &ConnectionConfig,
    ) -> Result<Vec<TableMetadata>, DataFetchError> {
        let connection = self.connect(config)?;

        // Get objects with depth=all (includes columns)
        let reader = connection
            .get_objects(
                adbc_core::options::ObjectDepth::All,
                None, // catalog
                None, // db_schema
                None, // table_name
                None, // table_type
                None, // column_name
            )
            .map_err(|e| DataFetchError::Discovery(e.to_string()))?;

        self.parse_objects_result(reader)
    }

    async fn fetch_table(
        &self,
        config: &ConnectionConfig,
        catalog: Option<&str>,
        schema: &str,
        table: &str,
        storage: &dyn StorageManager,
        connection_id: i32,
    ) -> Result<String, DataFetchError> {
        // Execute query and write to parquet buffer in a separate block
        // to ensure connection/statement don't cross await boundary
        let buffer = {
            let mut connection = self.connect(config)?;

            // Build fully-qualified table name with properly quoted identifiers
            let table_ref = match catalog {
                Some(cat) => format!(
                    "{}.{}.{}",
                    quote_identifier(cat),
                    quote_identifier(schema),
                    quote_identifier(table)
                ),
                None => format!("{}.{}", quote_identifier(schema), quote_identifier(table)),
            };

            // Create statement and execute query
            let mut statement = connection
                .new_statement()
                .map_err(|e| DataFetchError::Query(e.to_string()))?;

            statement
                .set_sql_query(format!("SELECT * FROM {}", table_ref))
                .map_err(|e| DataFetchError::Query(e.to_string()))?;

            let reader = statement
                .execute()
                .map_err(|e| DataFetchError::Query(e.to_string()))?;

            // Collect all batches
            let mut batches: Vec<RecordBatch> = Vec::new();
            for batch_result in reader {
                let batch = batch_result.map_err(|e| DataFetchError::Query(e.to_string()))?;
                batches.push(batch);
            }

            if batches.is_empty() {
                return Err(DataFetchError::Query("No data returned from query".into()));
            }

            // Write to parquet in memory
            let arrow_schema = batches[0].schema();
            let mut buffer = Vec::new();

            {
                let props = WriterProperties::builder().build();
                let mut writer = ArrowWriter::try_new(&mut buffer, arrow_schema.clone(), Some(props))
                    .map_err(|e| DataFetchError::Storage(e.to_string()))?;

                for batch in &batches {
                    writer
                        .write(batch)
                        .map_err(|e| DataFetchError::Storage(e.to_string()))?;
                }

                writer
                    .close()
                    .map_err(|e| DataFetchError::Storage(e.to_string()))?;
            }

            buffer
        }; // statement and connection dropped here

        // Write to storage (async)
        let parquet_url = storage.cache_url(connection_id, schema, table);
        storage
            .write(&parquet_url, &buffer)
            .await
            .map_err(|e| DataFetchError::Storage(e.to_string()))?;

        Ok(parquet_url)
    }
}

/// Convert SQL type code to Arrow DataType
fn sql_type_to_arrow(sql_type: i16) -> datafusion::arrow::datatypes::DataType {
    use datafusion::arrow::datatypes::{DataType, TimeUnit};

    // Based on JDBC/ODBC type codes
    match sql_type {
        -7 => DataType::Boolean,          // BIT
        -6 => DataType::Int8,             // TINYINT
        5 => DataType::Int16,             // SMALLINT
        4 => DataType::Int32,             // INTEGER
        -5 => DataType::Int64,            // BIGINT
        6 => DataType::Float32,           // FLOAT
        7 => DataType::Float32,           // REAL
        8 => DataType::Float64,           // DOUBLE
        2 | 3 => DataType::Decimal128(38, 10), // NUMERIC, DECIMAL
        1 | 12 | -1 => DataType::Utf8,    // CHAR, VARCHAR, LONGVARCHAR
        91 => DataType::Date32,           // DATE
        92 => DataType::Time64(TimeUnit::Microsecond), // TIME
        93 => DataType::Timestamp(TimeUnit::Microsecond, None), // TIMESTAMP
        -2 | -3 | -4 => DataType::Binary, // BINARY, VARBINARY, LONGVARBINARY
        _ => DataType::Utf8,              // Default to string
    }
}