//! Direct parquet execution plan construction, bypassing ListingTable.
//!
//! ListingTable makes object store list+head calls to discover files in a directory.
//! Since runtimedb tables are always backed by a single parquet file, these operations
//! are wasteful. This module provides utilities that construct execution plans directly
//! from a known file URL, eliminating the list operation and reducing to a single head.

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::ExecutionPlan;
use std::any::Any;
use std::sync::Arc;

/// Build a `DataSourceExec` for a single parquet file, bypassing ListingTable's
/// list operation. Only performs a single head request to get the file size
/// (required by the parquet reader to locate the footer).
///
/// `file_url` must be the full URL to the parquet file (e.g.,
/// `s3://bucket/.../data.parquet` or `file:///tmp/.../data.parquet`).
///
/// If `sort_columns` is provided, the execution plan will declare that its output
/// is sorted by those columns, allowing DataFusion to skip redundant sorts.
/// Each tuple is (column_name, is_descending).
pub async fn build_parquet_exec(
    file_url: &str,
    file_schema: SchemaRef,
    state: &dyn Session,
    projection: Option<&Vec<usize>>,
    limit: Option<usize>,
    sort_columns: Option<&Vec<(String, bool)>>,
) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
    // Parse URL to extract ObjectStoreUrl and object-store-relative path.
    // ListingTableUrl handles both file:// and s3:// schemes.
    let table_url = ListingTableUrl::parse(file_url)
        .map_err(|e| DataFusionError::External(format!("Failed to parse URL: {}", e).into()))?;
    let object_store_url = table_url.object_store();
    let file_path = table_url.prefix().clone();

    // Get file metadata via head request. The parquet reader requires the file size
    // to locate the footer at the end of the file.
    let store = state.runtime_env().object_store(&object_store_url)?;
    let object_meta = store.head(&file_path).await.map_err(|e| {
        DataFusionError::External(
            format!("Failed to get file metadata for {}: {}", file_url, e).into(),
        )
    })?;

    // In DataFusion 52+, ParquetSource takes the schema, and FileScanConfigBuilder takes 2 args
    let parquet_source: Arc<dyn datafusion::datasource::physical_plan::FileSource> =
        Arc::new(ParquetSource::new(file_schema.clone()));

    let mut config_builder = FileScanConfigBuilder::new(object_store_url, parquet_source)
        .with_file(object_meta.into())
        .with_projection_indices(projection.cloned())?
        .with_limit(limit);

    // If sort columns are provided, declare output ordering so DataFusion can skip sorts
    if let Some(cols) = sort_columns {
        let sort_exprs: Vec<PhysicalSortExpr> = cols
            .iter()
            .filter_map(|(col_name, is_descending)| {
                // Find the column in the schema and create a sort expression
                datafusion::physical_expr::expressions::col(col_name, &file_schema)
                    .ok()
                    .map(|expr| PhysicalSortExpr {
                        expr,
                        options: datafusion::arrow::compute::SortOptions {
                            descending: *is_descending,
                            nulls_first: true,
                        },
                    })
            })
            .collect();

        if !sort_exprs.is_empty() {
            if let Some(ordering) = LexOrdering::new(sort_exprs) {
                config_builder = config_builder.with_output_ordering(vec![ordering]);
            }
        }
    }

    let config = config_builder.build();
    Ok(DataSourceExec::from_data_source(config))
}

/// A lightweight `TableProvider` for a single parquet file.
///
/// Unlike `ListingTable`, this provider knows the exact file location and schema,
/// avoiding the object store list operation during scan. Only performs a single
/// head request to get the file size needed by the parquet reader.
#[derive(Debug)]
pub struct SingleFileParquetProvider {
    schema: SchemaRef,
    file_url: String,
}

impl SingleFileParquetProvider {
    pub fn new(schema: SchemaRef, file_url: String) -> Self {
        Self { schema, file_url }
    }
}

#[async_trait]
impl TableProvider for SingleFileParquetProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        build_parquet_exec(
            &self.file_url,
            self.schema.clone(),
            state,
            projection,
            limit,
            None, // No sort ordering for SingleFileParquetProvider
        )
        .await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        // Parquet supports filter pushdown via the physical optimizer on DataSourceExec
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::prelude::SessionContext;
    use tempfile::TempDir;

    /// Create a test parquet file and return (file_url, schema).
    fn create_test_parquet(dir: &std::path::Path) -> (String, SchemaRef) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["alice", "bob", "charlie"])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        let file_path = dir.join("test.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let file_url = format!("file://{}", file_path.display());
        (file_url, schema)
    }

    #[tokio::test]
    async fn test_build_parquet_exec_reads_all_rows() {
        let tmp = TempDir::new().unwrap();
        let (file_url, schema) = create_test_parquet(tmp.path());

        let ctx = SessionContext::new();
        let state = ctx.state();

        let plan = build_parquet_exec(&file_url, schema, &state, None, None, None)
            .await
            .unwrap();

        let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn test_build_parquet_exec_with_projection() {
        let tmp = TempDir::new().unwrap();
        let (file_url, schema) = create_test_parquet(tmp.path());

        let ctx = SessionContext::new();
        let state = ctx.state();

        // Project only "value" column (index 1)
        let projection = vec![1];
        let plan = build_parquet_exec(&file_url, schema, &state, Some(&projection), None, None)
            .await
            .unwrap();

        let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .unwrap();

        assert_eq!(batches[0].num_columns(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "value");
    }

    #[tokio::test]
    async fn test_build_parquet_exec_with_limit() {
        let tmp = TempDir::new().unwrap();
        let (file_url, schema) = create_test_parquet(tmp.path());

        let ctx = SessionContext::new();
        let state = ctx.state();

        let plan = build_parquet_exec(&file_url, schema, &state, None, Some(1), None)
            .await
            .unwrap();

        let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }

    #[tokio::test]
    async fn test_build_parquet_exec_nonexistent_file() {
        let ctx = SessionContext::new();
        let state = ctx.state();
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));

        let result = build_parquet_exec(
            "file:///nonexistent/path.parquet",
            schema,
            &state,
            None,
            None,
            None,
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Failed to get file metadata"),
            "Expected metadata error, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_single_file_provider_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Int32, false),
        ]));

        let provider =
            SingleFileParquetProvider::new(schema.clone(), "file:///fake.parquet".to_string());

        assert_eq!(provider.schema(), schema);
        assert_eq!(provider.table_type(), TableType::Base);
    }

    #[tokio::test]
    async fn test_single_file_provider_scan() {
        let tmp = TempDir::new().unwrap();
        let (file_url, schema) = create_test_parquet(tmp.path());

        let provider = SingleFileParquetProvider::new(schema, file_url);

        let ctx = SessionContext::new();
        ctx.register_table("test_table", Arc::new(provider))
            .unwrap();

        let df = ctx
            .sql("SELECT name, value FROM test_table ORDER BY value")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);

        let names = batches[0]
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "alice");
        assert_eq!(names.value(1), "bob");
        assert_eq!(names.value(2), "charlie");
    }

    #[tokio::test]
    async fn test_single_file_provider_filter_pushdown() {
        let tmp = TempDir::new().unwrap();
        let (file_url, schema) = create_test_parquet(tmp.path());

        let provider = SingleFileParquetProvider::new(schema, file_url);

        let ctx = SessionContext::new();
        ctx.register_table("test_table", Arc::new(provider))
            .unwrap();

        // Filter should be pushed down (inexact) and applied
        let df = ctx
            .sql("SELECT name FROM test_table WHERE value > 15")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2); // bob (20) and charlie (30)
    }

    #[tokio::test]
    async fn test_single_file_provider_supports_filters_pushdown() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let provider = SingleFileParquetProvider::new(schema, "file:///fake.parquet".to_string());

        let col_expr = datafusion::logical_expr::col("x").gt(datafusion::logical_expr::lit(5));
        let filters = vec![&col_expr];
        let result = provider.supports_filters_pushdown(&filters).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0], TableProviderFilterPushDown::Inexact);
    }
}
