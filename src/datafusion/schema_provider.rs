use async_trait::async_trait;
use datafusion::catalog::{MemorySchemaProvider, SchemaProvider};
use datafusion::datasource::TableProvider;
use std::sync::Arc;

use super::block_on;
use crate::catalog::CatalogManager;
use crate::datafetch::{deserialize_arrow_schema, DataFetcher};
use crate::source::Source;
use crate::storage::StorageManager;

use super::lazy_table_provider::LazyTableProvider;

/// A schema provider that syncs tables on-demand from remote sources.
/// Wraps MemorySchemaProvider for caching already-loaded tables.
#[derive(Debug)]
pub struct RivetSchemaProvider {
    connection_id: i32,
    #[allow(dead_code)]
    connection_name: String,
    schema_name: String,
    source: Arc<Source>,
    catalog: Arc<dyn CatalogManager>,
    inner: Arc<MemorySchemaProvider>,
    storage: Arc<dyn StorageManager>,
    fetcher: Arc<dyn DataFetcher>,
}

impl RivetSchemaProvider {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        connection_id: i32,
        connection_name: String,
        schema_name: String,
        source: Arc<Source>,
        catalog: Arc<dyn CatalogManager>,
        storage: Arc<dyn StorageManager>,
        fetcher: Arc<dyn DataFetcher>,
    ) -> Self {
        Self {
            connection_id,
            connection_name,
            schema_name,
            source,
            catalog,
            inner: Arc::new(MemorySchemaProvider::new()),
            storage,
            fetcher,
        }
    }
}

#[async_trait]
impl SchemaProvider for RivetSchemaProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        // Return all known tables from the catalog store
        // Uses block_on since SchemaProvider trait methods are sync
        match block_on(self.catalog.list_tables(Some(self.connection_id))) {
            Ok(tables) => tables
                .into_iter()
                .filter(|t| t.schema_name == self.schema_name)
                .map(|t| t.table_name)
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    async fn table(
        &self,
        name: &str,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        // First check: is it already loaded in memory?
        if let Some(table) = self.inner.table(name).await? {
            return Ok(Some(table));
        }

        // Get table info from catalog
        let table_info = match self
            .catalog
            .get_table(self.connection_id, &self.schema_name, name)
            .await
        {
            Ok(Some(info)) => info,
            Ok(None) => return Ok(None), // Table doesn't exist
            Err(e) => {
                return Err(datafusion::common::DataFusionError::External(
                    format!("Failed to get table info: {}", e).into(),
                ))
            }
        };

        // Deserialize the Arrow schema from catalog
        let arrow_schema_json = table_info.arrow_schema_json.ok_or_else(|| {
            datafusion::common::DataFusionError::External(
                format!("Table {} has no arrow_schema_json in catalog", name).into(),
            )
        })?;

        let schema = deserialize_arrow_schema(&arrow_schema_json).map_err(|e| {
            datafusion::common::DataFusionError::External(
                format!("Failed to deserialize arrow schema: {}", e).into(),
            )
        })?;

        // Create LazyTableProvider
        let provider = Arc::new(LazyTableProvider::new(
            schema,
            self.fetcher.clone(),
            self.source.clone(),
            self.catalog.clone(),
            self.storage.clone(),
            self.connection_id,
            self.schema_name.clone(),
            name.to_string(),
        )) as Arc<dyn TableProvider>;

        // Cache it in memory for future queries
        self.inner
            .register_table(name.to_string(), provider.clone())
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

        Ok(Some(provider))
    }

    fn table_exist(&self, name: &str) -> bool {
        // Check the catalog metadata store
        // Uses block_on since SchemaProvider trait methods are sync
        matches!(
            block_on(
                self.catalog
                    .get_table(self.connection_id, &self.schema_name, name)
            ),
            Ok(Some(_))
        )
    }
}
