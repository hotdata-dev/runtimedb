mod columns;
mod tables;

use crate::catalog::CatalogManager;
use async_trait::async_trait;
use columns::ColumnsTableProvider;
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::TableProvider;
use std::any::Any;
use std::sync::Arc;
use tables::TablesTableProvider;

/// Schema provider for `runtimedb.information_schema`.
///
/// Provides virtual tables that expose metadata about all registered
/// connections, schemas, and tables in the RuntimeDB instance.
#[derive(Debug)]
pub struct InformationSchemaProvider {
    tables: Arc<TablesTableProvider>,
    columns: Arc<ColumnsTableProvider>,
}

impl InformationSchemaProvider {
    pub fn new(catalog: Arc<dyn CatalogManager>) -> Self {
        Self {
            tables: Arc::new(TablesTableProvider::new(catalog.clone())),
            columns: Arc::new(ColumnsTableProvider::new(catalog)),
        }
    }
}

#[async_trait]
impl SchemaProvider for InformationSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        vec!["tables".to_string(), "columns".to_string()]
    }

    async fn table(&self, name: &str) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        match name {
            "tables" => Ok(Some(self.tables.clone())),
            "columns" => Ok(Some(self.columns.clone())),
            _ => Ok(None),
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        matches!(name, "tables" | "columns")
    }
}
