use async_trait::async_trait;
use datafusion::catalog::AsyncSchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use std::fmt::Debug;
use std::sync::Arc;

use super::lazy_table_provider::{LazyTableProvider, LazyTableProviderArgs};
use super::ParquetCacheManager;
use crate::catalog::CatalogManager;
use crate::datafetch::{deserialize_arrow_schema, FetchOrchestrator};
use crate::source::Source;

/// An async schema provider for a single schema within a connection.
///
/// Implements `AsyncSchemaProvider` to provide fully async table lookups
/// without blocking. Tables are looked up from CatalogManager on-demand
/// and returned as `LazyTableProvider` instances for deferred data fetching.
#[derive(Debug)]
pub struct AsyncConnectionSchema {
    connection_id: String,
    schema_name: String,
    source: Arc<Source>,
    catalog: Arc<dyn CatalogManager>,
    orchestrator: Arc<FetchOrchestrator>,
    cache_manager: Option<Arc<ParquetCacheManager>>,
}

impl AsyncConnectionSchema {
    pub fn new(
        connection_id: String,
        schema_name: String,
        source: Arc<Source>,
        catalog: Arc<dyn CatalogManager>,
        orchestrator: Arc<FetchOrchestrator>,
        cache_manager: Option<Arc<ParquetCacheManager>>,
    ) -> Self {
        Self {
            connection_id,
            schema_name,
            source,
            catalog,
            orchestrator,
            cache_manager,
        }
    }
}

#[async_trait]
impl AsyncSchemaProvider for AsyncConnectionSchema {
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        // Get table info from catalog
        let table_info = self
            .catalog
            .get_table(&self.connection_id, &self.schema_name, name)
            .await
            .map_err(|e| {
                datafusion::error::DataFusionError::External(
                    format!("Failed to get table info: {}", e).into(),
                )
            })?;

        let table_info = match table_info {
            Some(info) => info,
            None => return Ok(None),
        };

        // Deserialize the Arrow schema from catalog
        let arrow_schema_json = table_info.arrow_schema_json.ok_or_else(|| {
            datafusion::error::DataFusionError::External(
                format!(
                    "Table {}.{} has no arrow_schema_json in catalog",
                    self.schema_name, name
                )
                .into(),
            )
        })?;

        let schema = deserialize_arrow_schema(&arrow_schema_json).map_err(|e| {
            datafusion::error::DataFusionError::External(
                format!("Failed to deserialize arrow schema: {}", e).into(),
            )
        })?;

        // Create LazyTableProvider for on-demand data fetching
        let provider = Arc::new(LazyTableProvider::new(LazyTableProviderArgs {
            schema,
            source: self.source.clone(),
            catalog: self.catalog.clone(),
            orchestrator: self.orchestrator.clone(),
            connection_id: self.connection_id.clone(),
            schema_name: self.schema_name.clone(),
            table_name: name.to_string(),
            cache_manager: self.cache_manager.clone(),
        })) as Arc<dyn TableProvider>;

        Ok(Some(provider))
    }
}
