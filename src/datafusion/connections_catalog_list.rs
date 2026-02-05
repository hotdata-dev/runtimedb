use async_trait::async_trait;
use datafusion::catalog::{AsyncCatalogProvider, AsyncCatalogProviderList};
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use std::fmt::Debug;
use std::sync::Arc;

use super::async_connection_catalog::AsyncConnectionCatalog;
use super::datasets_catalog::DatasetsCatalogProvider;
use super::information_schema::InformationSchemaProvider;
use super::runtimedb_catalog::RuntimeDbCatalogProvider;
use super::ParquetCacheManager;
use crate::catalog::CatalogManager;
use crate::datafetch::FetchOrchestrator;
use crate::source::Source;

/// A unified async catalog provider list for all RuntimeDB catalogs.
///
/// Implements `AsyncCatalogProviderList` to provide fully async catalog lookups
/// for all catalog types:
/// - `runtimedb` - System catalog with results and information_schema
/// - `datasets` - User-uploaded datasets
/// - Connection catalogs - Dynamically resolved from CatalogManager
///
/// The built-in `resolve()` method from the trait handles resolving table
/// references by calling `catalog()` for each unique catalog name found.
pub struct UnifiedCatalogList {
    catalog: Arc<dyn CatalogManager>,
    orchestrator: Arc<FetchOrchestrator>,
    /// Pre-built runtimedb catalog provider
    runtimedb_catalog: Arc<RuntimeDbCatalogProvider>,
    /// Pre-built datasets catalog provider
    datasets_catalog: Arc<DatasetsCatalogProvider>,
    cache_manager: Option<Arc<ParquetCacheManager>>,
}

impl Debug for UnifiedCatalogList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnifiedCatalogList")
            .field("catalog", &"...")
            .field("orchestrator", &"...")
            .finish()
    }
}

impl UnifiedCatalogList {
    /// Create a new UnifiedCatalogList.
    ///
    /// The `ctx` parameter is used to share the RuntimeEnv (including object stores)
    /// with the schema providers for schema inference.
    pub fn new(
        catalog: Arc<dyn CatalogManager>,
        orchestrator: Arc<FetchOrchestrator>,
        ctx: &SessionContext,
        cache_manager: Option<Arc<ParquetCacheManager>>,
    ) -> Self {
        // Build the runtimedb catalog with results and information_schema
        let runtimedb_catalog =
            RuntimeDbCatalogProvider::new(catalog.clone(), ctx, cache_manager.clone())
                .with_information_schema(Arc::new(InformationSchemaProvider::new(catalog.clone())));

        // Build the datasets catalog
        let datasets_catalog =
            DatasetsCatalogProvider::with_runtime_env(catalog.clone(), ctx, cache_manager.clone());

        Self {
            catalog,
            orchestrator,
            runtimedb_catalog: Arc::new(runtimedb_catalog),
            datasets_catalog: Arc::new(datasets_catalog),
            cache_manager,
        }
    }
}

#[async_trait]
impl AsyncCatalogProviderList for UnifiedCatalogList {
    async fn catalog(&self, name: &str) -> Result<Option<Arc<dyn AsyncCatalogProvider>>> {
        // Check for built-in catalogs first
        match name {
            "runtimedb" => return Ok(Some(self.runtimedb_catalog.clone())),
            "datasets" => return Ok(Some(self.datasets_catalog.clone())),
            _ => {}
        }

        // Look up connection by name from CatalogManager
        let conn = self
            .catalog
            .get_connection_by_name(name)
            .await
            .map_err(|e| {
                datafusion::error::DataFusionError::External(
                    format!("Failed to get connection: {}", e).into(),
                )
            })?;

        let conn = match conn {
            Some(c) => c,
            None => return Ok(None),
        };

        // Deserialize Source from config_json
        let source: Source = serde_json::from_str(&conn.config_json).map_err(|e| {
            datafusion::error::DataFusionError::External(
                format!("Failed to deserialize source config: {}", e).into(),
            )
        })?;

        Ok(Some(Arc::new(AsyncConnectionCatalog::new(
            conn.id,
            conn.name,
            Arc::new(source),
            self.catalog.clone(),
            self.orchestrator.clone(),
            self.cache_manager.clone(),
        ))))
    }
}
