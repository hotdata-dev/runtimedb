mod columns;
mod tables;

use crate::catalog::CatalogManager;
use async_trait::async_trait;
use columns::ColumnsTableProvider;
use datafusion::catalog::AsyncSchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use std::fmt::Debug;
use std::sync::Arc;
use tables::TablesTableProvider;

/// Async schema provider for `runtimedb.information_schema`.
///
/// Provides virtual tables that expose metadata about all registered
/// connections, schemas, and tables in the RuntimeDB instance.
pub struct InformationSchemaProvider {
    tables: Arc<TablesTableProvider>,
    columns: Arc<ColumnsTableProvider>,
}

impl Debug for InformationSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InformationSchemaProvider")
            .field("tables", &"TablesTableProvider")
            .field("columns", &"ColumnsTableProvider")
            .finish()
    }
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
impl AsyncSchemaProvider for InformationSchemaProvider {
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        match name {
            "tables" => Ok(Some(self.tables.clone())),
            "columns" => Ok(Some(self.columns.clone())),
            _ => Ok(None),
        }
    }
}
