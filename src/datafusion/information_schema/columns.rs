use crate::catalog::CatalogManager;
use crate::datafetch::deserialize_arrow_schema;
use async_trait::async_trait;
use datafusion::arrow::array::{Int32Builder, StringBuilder};
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
use tracing::warn;

/// Virtual table provider for `information_schema.columns`.
///
/// Queries the catalog, deserializes Arrow schemas, and materializes
/// column metadata as an in-memory table.
#[derive(Debug)]
pub struct ColumnsTableProvider {
    catalog: Arc<dyn CatalogManager>,
}

impl ColumnsTableProvider {
    pub fn new(catalog: Arc<dyn CatalogManager>) -> Self {
        Self { catalog }
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("ordinal_position", DataType::Int32, false),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("is_nullable", DataType::Utf8, false),
        ]))
    }

    async fn build_record_batch(&self) -> Result<RecordBatch> {
        // Get all connections to map connection_id -> name
        let connections = self
            .catalog
            .list_connections()
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        let conn_map: std::collections::HashMap<String, String> =
            connections.into_iter().map(|c| (c.id, c.name)).collect();

        // Get all tables
        let tables = self
            .catalog
            .list_tables(None)
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        // Build arrays
        let mut catalog_builder = StringBuilder::new();
        let mut schema_builder = StringBuilder::new();
        let mut table_builder = StringBuilder::new();
        let mut column_builder = StringBuilder::new();
        let mut ordinal_builder = Int32Builder::new();
        let mut type_builder = StringBuilder::new();
        let mut nullable_builder = StringBuilder::new();

        for table in tables {
            let catalog_name = conn_map
                .get(&table.connection_id)
                .cloned()
                .unwrap_or_else(|| "unknown".to_string());

            // Deserialize the Arrow schema to get column info
            if let Some(schema_json) = &table.arrow_schema_json {
                match deserialize_arrow_schema(schema_json) {
                    Ok(arrow_schema) => {
                        for (ordinal, field) in arrow_schema.fields().iter().enumerate() {
                            catalog_builder.append_value(&catalog_name);
                            schema_builder.append_value(&table.schema_name);
                            table_builder.append_value(&table.table_name);
                            column_builder.append_value(field.name());
                            ordinal_builder.append_value((ordinal + 1) as i32);
                            type_builder.append_value(format!("{}", field.data_type()));
                            nullable_builder.append_value(if field.is_nullable() {
                                "YES"
                            } else {
                                "NO"
                            });
                        }
                    }
                    Err(e) => {
                        warn!(
                            "Failed to deserialize schema for {}.{}: {}",
                            table.schema_name, table.table_name, e
                        );
                    }
                }
            }
        }

        let batch = RecordBatch::try_new(
            Self::schema(),
            vec![
                Arc::new(catalog_builder.finish()),
                Arc::new(schema_builder.finish()),
                Arc::new(table_builder.finish()),
                Arc::new(column_builder.finish()),
                Arc::new(ordinal_builder.finish()),
                Arc::new(type_builder.finish()),
                Arc::new(nullable_builder.finish()),
            ],
        )?;

        Ok(batch)
    }
}

#[async_trait]
impl TableProvider for ColumnsTableProvider {
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
