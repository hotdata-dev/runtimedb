use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::error::Result;
use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Virtual catalog provider for internal RuntimeDB system objects.
///
/// This catalog provides access to system schemas like `information_schema`
/// that expose metadata about the RuntimeDB instance. Schemas are registered
/// dynamically via `register_schema()`.
#[derive(Debug, Default)]
pub struct RuntimeDbCatalogProvider {
    schemas: RwLock<HashMap<String, Arc<dyn SchemaProvider>>>,
}

impl RuntimeDbCatalogProvider {
    pub fn new() -> Self {
        Self {
            schemas: RwLock::new(HashMap::new()),
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
