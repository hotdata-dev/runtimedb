use super::results_schema::ResultsSchemaProvider;
use crate::catalog::CatalogManager;
use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Virtual catalog provider for internal RuntimeDB system objects.
///
/// This catalog provides access to system schemas like `results` and `information_schema`
/// that expose internal RuntimeDB data. Schemas are registered dynamically via
/// `register_schema()`.
#[derive(Debug)]
pub struct RuntimeDbCatalogProvider {
    schemas: RwLock<HashMap<String, Arc<dyn SchemaProvider>>>,
}

impl RuntimeDbCatalogProvider {
    /// Create a new RuntimeDbCatalogProvider with the results schema pre-registered.
    ///
    /// The `ctx` parameter is used to share the RuntimeEnv (including object stores like S3)
    /// with the results schema provider for schema inference.
    pub fn new(catalog: Arc<dyn CatalogManager>, ctx: &SessionContext) -> Self {
        let mut schemas = HashMap::new();
        schemas.insert(
            "results".to_string(),
            Arc::new(ResultsSchemaProvider::with_runtime_env(catalog, ctx))
                as Arc<dyn SchemaProvider>,
        );
        Self {
            schemas: RwLock::new(schemas),
        }
    }
}

#[async_trait]
impl CatalogProvider for RuntimeDbCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas
            .read()
            .expect("schema lock poisoned")
            .keys()
            .cloned()
            .collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas
            .read()
            .expect("schema lock poisoned")
            .get(name)
            .cloned()
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        let mut schemas = self.schemas.write().expect("schema lock poisoned");
        Ok(schemas.insert(name.to_string(), schema))
    }
}
