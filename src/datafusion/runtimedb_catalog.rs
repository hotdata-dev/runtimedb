use super::results_schema::ResultsSchemaProvider;
use super::ParquetCacheManager;
use crate::catalog::CatalogManager;
use async_trait::async_trait;
use datafusion::catalog::{AsyncCatalogProvider, AsyncSchemaProvider};
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use std::fmt::Debug;
use std::sync::Arc;

/// Async catalog provider for internal RuntimeDB system objects.
///
/// This catalog provides access to system schemas like `results` and `information_schema`
/// that expose internal RuntimeDB data.
pub struct RuntimeDbCatalogProvider {
    results_schema: Arc<ResultsSchemaProvider>,
    information_schema: Option<Arc<dyn AsyncSchemaProvider>>,
}

impl Debug for RuntimeDbCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeDbCatalogProvider")
            .field("results_schema", &"...")
            .field("information_schema", &self.information_schema.is_some())
            .finish()
    }
}

impl RuntimeDbCatalogProvider {
    /// Create a new RuntimeDbCatalogProvider with the results schema.
    ///
    /// The `ctx` parameter is used to share the RuntimeEnv (including object stores like S3)
    /// with the results schema provider for schema inference.
    pub fn new(
        catalog: Arc<dyn CatalogManager>,
        ctx: &SessionContext,
        cache_manager: Option<Arc<ParquetCacheManager>>,
    ) -> Self {
        Self {
            results_schema: Arc::new(ResultsSchemaProvider::with_runtime_env(
                catalog,
                ctx,
                cache_manager,
            )),
            information_schema: None,
        }
    }

    /// Register the information_schema provider.
    pub fn with_information_schema(mut self, schema: Arc<dyn AsyncSchemaProvider>) -> Self {
        self.information_schema = Some(schema);
        self
    }
}

#[async_trait]
impl AsyncCatalogProvider for RuntimeDbCatalogProvider {
    async fn schema(&self, name: &str) -> Result<Option<Arc<dyn AsyncSchemaProvider>>> {
        match name {
            "results" => Ok(Some(self.results_schema.clone())),
            "information_schema" => Ok(self.information_schema.clone()),
            _ => Ok(None),
        }
    }
}
