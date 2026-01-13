use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

/// Virtual catalog provider for internal RuntimeDB system objects.
///
/// This catalog provides access to system schemas like `information_schema`
/// that expose metadata about the RuntimeDB instance. Schemas are registered
/// dynamically via `register_schema()`.
#[derive(Debug, Default)]
pub struct RuntimeDbCatalogProvider {
    schemas: HashMap<String, Arc<dyn SchemaProvider>>,
}

impl RuntimeDbCatalogProvider {
    pub fn new() -> Self {
        Self {
            schemas: HashMap::new(),
        }
    }

    /// Register a schema provider under this catalog.
    pub fn register_schema(&mut self, name: impl Into<String>, provider: Arc<dyn SchemaProvider>) {
        self.schemas.insert(name.into(), provider);
    }
}

#[async_trait]
impl CatalogProvider for RuntimeDbCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).cloned()
    }
}
