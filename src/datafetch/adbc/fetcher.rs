use adbc_core::options::{AdbcVersion, OptionDatabase, OptionValue};
use adbc_core::{Connection, Database, Driver};
use adbc_driver_manager::ManagedDriver;
use async_trait::async_trait;

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
        _batch: &datafusion::arrow::record_batch::RecordBatch,
    ) -> Result<Vec<TableMetadata>, DataFetchError> {
        // TODO: Implement full nested structure parsing
        // The get_objects result has a complex nested schema
        // For now, return empty - will implement in next task
        Ok(Vec::new())
    }
}

impl Default for AdbcFetcher {
    fn default() -> Self {
        Self::new()
    }
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
        _config: &ConnectionConfig,
        _catalog: Option<&str>,
        _schema: &str,
        _table: &str,
        _storage: &dyn StorageManager,
        _connection_id: i32,
    ) -> Result<String, DataFetchError> {
        // TODO: Implement in next task
        todo!("Implement fetch_table")
    }
}