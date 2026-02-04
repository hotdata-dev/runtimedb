use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;

/// Load a parquet file and return an ExecutionPlan.
/// Uses the provided schema to ensure consistency with schema()
/// and proper projection index alignment.
#[tracing::instrument(
    name = "load_parquet_exec",
    skip(state, projection, filters, limit),
    fields(runtimedb.parquet_url = %parquet_url)
)]
pub async fn scan_parquet_exec(
    parquet_url: &str,
    schema: SchemaRef,
    state: &dyn Session,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
    // Parse the URL - DataFusion handles both file:// and s3://
    let table_path = ListingTableUrl::parse(parquet_url)
        .map_err(|e| DataFusionError::External(format!("Failed to parse URL: {}", e).into()))?;

    let file_format = ParquetFormat::default();
    let listing_options = ListingOptions::new(Arc::new(file_format));

    // The Parquet reader will match columns by name.
    let config = ListingTableConfig::new(table_path)
        .with_listing_options(listing_options)
        .with_schema(schema);

    let table = ListingTable::try_new(config).map_err(|e| {
        DataFusionError::External(format!("Failed to create ListingTable: {}", e).into())
    })?;

    // Create the scan execution plan with projection, filter, and limit pushdown
    table.scan(state, projection, filters, limit).await
}
