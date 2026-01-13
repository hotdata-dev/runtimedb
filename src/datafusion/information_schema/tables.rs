use crate::catalog::CatalogManager;
use async_trait::async_trait;
use datafusion::arrow::array::StringBuilder;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use std::any::Any;
use std::sync::Arc;

/// Virtual table provider for `information_schema.tables`.
///
/// Queries the catalog and materializes all table metadata as an in-memory table.
/// DataFusion applies any WHERE filters client-side after materialization.
#[derive(Debug)]
pub struct TablesTableProvider {
    catalog: Arc<dyn CatalogManager>,
}

impl TablesTableProvider {
    pub fn new(catalog: Arc<dyn CatalogManager>) -> Self {
        Self { catalog }
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
        ]))
    }

    async fn build_record_batch(&self) -> Result<RecordBatch> {
        // Get all connections to map connection_id -> name
        let connections = self
            .catalog
            .list_connections()
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        let conn_map: std::collections::HashMap<i32, String> = connections
            .into_iter()
            .map(|c| (c.id, c.name))
            .collect();

        // Get all tables
        let tables = self
            .catalog
            .list_tables(None)
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        // Build arrays
        let mut catalog_builder = StringBuilder::new();
        let mut schema_builder = StringBuilder::new();
        let mut name_builder = StringBuilder::new();
        let mut type_builder = StringBuilder::new();

        for table in tables {
            let catalog_name = conn_map
                .get(&table.connection_id)
                .cloned()
                .unwrap_or_else(|| "unknown".to_string());

            catalog_builder.append_value(&catalog_name);
            schema_builder.append_value(&table.schema_name);
            name_builder.append_value(&table.table_name);
            type_builder.append_value("BASE TABLE");
        }

        let batch = RecordBatch::try_new(
            Self::schema(),
            vec![
                Arc::new(catalog_builder.finish()),
                Arc::new(schema_builder.finish()),
                Arc::new(name_builder.finish()),
                Arc::new(type_builder.finish()),
            ],
        )?;

        Ok(batch)
    }
}

#[async_trait]
impl TableProvider for TablesTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        Self::schema()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let batch = self.build_record_batch().await?;
        let mem_table = MemTable::try_new(Self::schema(), vec![vec![batch]])?;
        mem_table.scan(state, projection, filters, limit).await
    }
}
