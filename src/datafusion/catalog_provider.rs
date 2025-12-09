use super::schema_provider::HotDataSchemaProvider;
use crate::catalog::CatalogManager;
use crate::datafetch::{DataFetcher, NativeFetcher};
use crate::source::Source;
use crate::storage::StorageManager;
use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// A catalog provider that represents a single connection.
/// Lazily creates schema providers as they are accessed.
#[derive(Debug)]
pub struct HotDataCatalogProvider {
    connection_id: i32,
    connection_name: String,
    source: Arc<Source>,
    catalog: Arc<dyn CatalogManager>,
    schemas: Arc<RwLock<HashMap<String, Arc<dyn SchemaProvider>>>>,
    storage: Arc<dyn StorageManager>,
    fetcher: Arc<dyn DataFetcher>,
}

impl HotDataCatalogProvider {
    pub fn new(
        connection_id: i32,
        connection_name: String,
        source: Arc<Source>,
        catalog: Arc<dyn CatalogManager>,
        storage: Arc<dyn StorageManager>,
    ) -> Self {
        Self {
            connection_id,
            connection_name,
            source,
            catalog,
            schemas: Arc::new(RwLock::new(HashMap::new())),
            storage,
            fetcher: Arc::new(NativeFetcher::new()),
        }
    }

    /// Get or create a schema provider for the given schema name.
    fn get_or_create_schema(&self, schema_name: &str) -> Arc<dyn SchemaProvider> {
        // Fast path: check if schema already exists
        {
            let schemas = self.schemas.read().unwrap();
            if let Some(schema) = schemas.get(schema_name) {
                return schema.clone();
            }
        }

        // Slow path: create new schema
        let mut schemas = self.schemas.write().unwrap();

        // Double-check in case another thread created it while we were waiting for write lock
        if let Some(schema) = schemas.get(schema_name) {
            return schema.clone();
        }

        // Create new schema provider
        let schema_provider = Arc::new(HotDataSchemaProvider::new(
            self.connection_id,
            self.connection_name.clone(),
            schema_name.to_string(),
            self.source.clone(),
            self.catalog.clone(),
            self.storage.clone(),
            self.fetcher.clone(),
        )) as Arc<dyn SchemaProvider>;

        schemas.insert(schema_name.to_string(), schema_provider.clone());

        schema_provider
    }
}

#[async_trait]
impl CatalogProvider for HotDataCatalogProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        // Return all known schema names from DuckDB catalog for this connection
        match self.catalog.list_tables(Some(self.connection_id)) {
            Ok(tables) => {
                let mut schemas: Vec<String> = tables
                    .into_iter()
                    .map(|t| t.schema_name)
                    .collect::<std::collections::HashSet<_>>()
                    .into_iter()
                    .collect();
                schemas.sort();
                schemas
            }
            Err(_) => Vec::new(),
        }
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        // Check if this schema exists in our catalog
        let schema_exists = match self.catalog.list_tables(Some(self.connection_id)) {
            Ok(tables) => tables.iter().any(|t| t.schema_name == name),
            Err(_) => false,
        };

        if schema_exists {
            Some(self.get_or_create_schema(name))
        } else {
            None
        }
    }
}
