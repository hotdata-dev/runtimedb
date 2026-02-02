use async_trait::async_trait;
use datafusion::catalog::{AsyncCatalogProvider, AsyncSchemaProvider};
use datafusion::error::Result;
use std::fmt::Debug;
use std::sync::Arc;

use super::async_connection_schema::AsyncConnectionSchema;
use crate::catalog::CatalogManager;
use crate::datafetch::FetchOrchestrator;
use crate::source::Source;

/// An async catalog provider for a single database connection.
///
/// Implements `AsyncCatalogProvider` to provide fully async schema lookups
/// without blocking. Schemas are discovered from CatalogManager on-demand
/// by checking which schemas have tables registered for this connection.
#[derive(Debug)]
pub struct AsyncConnectionCatalog {
    connection_id: String,
    #[allow(dead_code)]
    connection_name: String,
    source: Arc<Source>,
    catalog: Arc<dyn CatalogManager>,
    orchestrator: Arc<FetchOrchestrator>,
}

impl AsyncConnectionCatalog {
    pub fn new(
        connection_id: String,
        connection_name: String,
        source: Arc<Source>,
        catalog: Arc<dyn CatalogManager>,
        orchestrator: Arc<FetchOrchestrator>,
    ) -> Self {
        Self {
            connection_id,
            connection_name,
            source,
            catalog,
            orchestrator,
        }
    }
}

#[async_trait]
impl AsyncCatalogProvider for AsyncConnectionCatalog {
    async fn schema(&self, name: &str) -> Result<Option<Arc<dyn AsyncSchemaProvider>>> {
        // Check if this schema exists by querying CatalogManager for tables
        let tables = self
            .catalog
            .list_tables(Some(&self.connection_id))
            .await
            .map_err(|e| {
                datafusion::error::DataFusionError::External(
                    format!("Failed to list tables: {}", e).into(),
                )
            })?;

        // Check if any table belongs to this schema
        let schema_exists = tables.iter().any(|t| t.schema_name == name);

        if schema_exists {
            Ok(Some(Arc::new(AsyncConnectionSchema::new(
                self.connection_id.clone(),
                name.to_string(),
                self.source.clone(),
                self.catalog.clone(),
                self.orchestrator.clone(),
            ))))
        } else {
            Ok(None)
        }
    }
}
