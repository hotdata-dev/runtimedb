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
use datafusion::datasource::physical_plan::parquet::CachedParquetFileReaderFactory;
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use std::any::Any;
use std::sync::Arc;

/// Build a `DataSourceExec` for a single parquet file, bypassing ListingTable's
/// list operation. Only performs a single head request to get the file size
/// (required by the parquet reader to locate the footer).
///
/// `file_url` must be the full URL to the parquet file (e.g.,
/// `s3://bucket/.../data.parquet` or `file:///tmp/.../data.parquet`).
pub async fn build_parquet_exec(
    file_url: &str,
    file_schema: SchemaRef,
    state: &dyn Session,
    projection: Option<&Vec<usize>>,
    limit: Option<usize>,
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

    // Use CachedParquetFileReaderFactory so parsed parquet footer metadata is
    // cached across queries, avoiding repeated I/O + deserialization.
    let metadata_cache = state.runtime_env().cache_manager.get_file_metadata_cache();
    let cached_reader_factory = Arc::new(CachedParquetFileReaderFactory::new(
        Arc::clone(&store),
        metadata_cache,
    ));
    let parquet_source: Arc<dyn datafusion::datasource::physical_plan::FileSource> =
        Arc::new(ParquetSource::default().with_parquet_file_reader_factory(cached_reader_factory));

    let config = FileScanConfigBuilder::new(object_store_url, file_schema, parquet_source)
        .with_file(object_meta.into())
        .with_projection_indices(projection.cloned())
        .with_limit(limit)
        .build();

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
