use async_trait::async_trait;
use datafusion::catalog::{MemorySchemaProvider, SchemaProvider};
use datafusion::datasource::TableProvider;
use std::sync::Arc;

use super::block_on;
use super::lazy_table_provider::LazyTableProvider;
use crate::catalog::CatalogManager;
use crate::datafetch::{deserialize_arrow_schema, FetchOrchestrator};
use crate::source::Source;

/// A schema provider that syncs tables on-demand from remote sources.
/// Wraps MemorySchemaProvider for caching already-loaded tables.
#[derive(Debug)]
pub struct RuntimeSchemaProvider {
    connection_id: i32,
    #[allow(dead_code)]
    connection_name: String,
    schema_name: String,
    source: Arc<Source>,
    catalog: Arc<dyn CatalogManager>,
    orchestrator: Arc<FetchOrchestrator>,
    inner: Arc<MemorySchemaProvider>,
}

impl RuntimeSchemaProvider {
    /// Create a new schema provider for a connection's schema.
    ///
    /// The Source contains the credential reference internally.
    /// Credential resolution happens when tables are accessed via the orchestrator.
    pub fn new(
        connection_id: i32,
        connection_name: String,
        schema_name: String,
        source: Arc<Source>,
        catalog: Arc<dyn CatalogManager>,
        orchestrator: Arc<FetchOrchestrator>,
    ) -> Self {
        Self {
            connection_id,
            connection_name,
            schema_name,
            source,
            catalog,
            orchestrator,
            inner: Arc::new(MemorySchemaProvider::new()),
        }
    }
}

#[async_trait]
impl SchemaProvider for RuntimeSchemaProvider {
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
        // Source contains the credential internally
        let provider = Arc::new(LazyTableProvider::new(
            schema,
            self.source.clone(),
            self.catalog.clone(),
            self.orchestrator.clone(),
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
