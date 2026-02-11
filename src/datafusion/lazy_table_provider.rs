use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use std::any::Any;
use std::sync::Arc;

use crate::catalog::CatalogManager;
use crate::datafetch::{FetchOrchestrator, IndexPresetRegistry};
use crate::source::Source;

/// A lazy table provider that defers data fetching until scan() is called.
///
/// This provider stores the table schema from catalog metadata and only triggers
/// the actual data fetch when a query scan is executed. This enables efficient
/// query planning without unnecessary I/O operations.
///
/// Supports index preset selection: if index presets are configured, the provider
/// analyzes query filters and selects the best preset index based on range filters.
#[derive(Debug)]
pub struct LazyTableProvider {
    schema: SchemaRef,
    source: Arc<Source>,
    catalog: Arc<dyn CatalogManager>,
    orchestrator: Arc<FetchOrchestrator>,
    connection_id: String,
    schema_name: String,
    table_name: String,
    index_preset_registry: Option<Arc<IndexPresetRegistry>>,
}

impl LazyTableProvider {
    pub fn new(
        schema: SchemaRef,
        source: Arc<Source>,
        catalog: Arc<dyn CatalogManager>,
        orchestrator: Arc<FetchOrchestrator>,
        connection_id: String,
        schema_name: String,
        table_name: String,
    ) -> Self {
        Self {
            schema,
            source,
            catalog,
            orchestrator,
            connection_id,
            schema_name,
            table_name,
            index_preset_registry: None,
        }
    }

    /// Create a provider with index preset support.
    pub fn with_index_presets(
        schema: SchemaRef,
        source: Arc<Source>,
        catalog: Arc<dyn CatalogManager>,
        orchestrator: Arc<FetchOrchestrator>,
        connection_id: String,
        schema_name: String,
        table_name: String,
        index_preset_registry: Arc<IndexPresetRegistry>,
    ) -> Self {
        Self {
            schema,
            source,
            catalog,
            orchestrator,
            connection_id,
            schema_name,
            table_name,
            index_preset_registry: Some(index_preset_registry),
        }
    }

    /// Select the best preset index for the given filters.
    /// Returns the preset name and its sort columns (with direction) if a beneficial preset is found.
    ///
    /// Index presets help with range/equality filters (>, <, >=, <=, =, BETWEEN) —
    /// sorted data has tight row group statistics enabling effective pruning.
    fn select_preset_index(&self, filters: &[Expr]) -> Option<(String, Vec<(String, bool)>)> {
        let registry = self.index_preset_registry.as_ref()?;
        let presets = registry.get(&self.schema_name, &self.table_name)?;

        // Extract column names that have range or equality filters
        let filter_columns = Self::extract_filter_columns(filters);

        // First try: match a preset index to a filter column
        for preset in presets {
            if let Some(first_col) = preset.sort_columns.first() {
                let first_col_lower: String = first_col.to_lowercase();

                if filter_columns
                    .iter()
                    .any(|c: &String| c.to_lowercase() == first_col_lower)
                {
                    tracing::debug!(
                        preset = %preset.name,
                        column = %first_col,
                        "Selected preset index for filter match"
                    );
                    // Preset indexes default to ASC
                    let cols_with_dir: Vec<(String, bool)> = preset.sort_columns.iter()
                        .map(|c| (c.clone(), false))
                        .collect();
                    return Some((preset.name.clone(), cols_with_dir));
                }
            }
        }

        None
    }

    /// Select the best catalog index for the given filters.
    /// Returns the index parquet URL and sort columns (with direction) if a beneficial index is found.
    ///
    /// This checks indexes created via CREATE INDEX SQL commands.
    /// Selection priority:
    /// 1. Index whose sort column matches a filter column (best for range pruning)
    /// 2. Any available index when no filters exist (enables sort elimination for ORDER BY queries)
    async fn select_catalog_index(
        &self,
        filters: &[Expr],
    ) -> Option<(String, Vec<(String, bool)>)> {
        // Query catalog for indexes on this table
        let indexes = match self
            .catalog
            .list_indexes(&self.connection_id, &self.schema_name, &self.table_name)
            .await
        {
            Ok(idx) => idx,
            Err(e) => {
                tracing::warn!("Failed to list indexes: {}", e);
                return None;
            }
        };

        tracing::debug!(
            index_count = indexes.len(),
            filter_count = filters.len(),
            "select_catalog_index: checking indexes"
        );

        if indexes.is_empty() {
            return None;
        }

        // Extract column names that have range or equality filters
        let filter_columns = Self::extract_filter_columns(filters);
        tracing::debug!(
            ?filter_columns,
            "select_catalog_index: extracted filter columns"
        );

        // First try: match an index to a filter column
        for index in &indexes {
            // Skip indexes without a parquet path (not yet built)
            let Some(parquet_path) = index.parquet_path.as_ref() else {
                continue;
            };
            let sort_columns = index.sort_columns_with_direction();

            if let Some((first_col, _)) = sort_columns.first() {
                let first_col_lower = first_col.to_lowercase();

                if filter_columns
                    .iter()
                    .any(|c| c.to_lowercase() == first_col_lower)
                {
                    tracing::debug!(
                        index = %index.index_name,
                        column = %first_col,
                        "Selected catalog index for filter match"
                    );
                    return Some((parquet_path.clone(), sort_columns));
                }
            }
        }

        // Fallback: when no filters are pushed down at all, use any available index.
        // The index parquet has the same data as the base, just pre-sorted.
        // Declaring the output ordering enables DataFusion to eliminate sorts
        // for ORDER BY queries on the index column, with no downside since
        // there are no filters that would benefit from base parquet statistics.
        // We check filters.is_empty() (not filter_columns) to avoid using the
        // index when ANY filter exists — even types we don't extract (IN, LIKE, etc.)
        // — since the base parquet may have better row group statistics for those.
        if filters.is_empty() {
            for index in &indexes {
                if let Some(parquet_path) = &index.parquet_path {
                    let sort_columns = index.sort_columns_with_direction();
                    if !sort_columns.is_empty() {
                        tracing::debug!(
                            index = %index.index_name,
                            "Selected catalog index for sort elimination"
                        );
                        return Some((parquet_path.clone(), sort_columns));
                    }
                }
            }
        }

        None
    }

    /// Extract column names that have range or equality filters applied.
    fn extract_filter_columns(filters: &[Expr]) -> Vec<String> {
        let mut columns = Vec::new();

        for filter in filters {
            Self::collect_filter_columns(filter, &mut columns);
        }

        columns
    }

    /// Recursively collect column names from range and equality filter expressions.
    fn collect_filter_columns(expr: &Expr, columns: &mut Vec<String>) {
        match expr {
            Expr::BinaryExpr(binary) => {
                // Check if this is a range or equality comparison
                let is_filter_op = matches!(
                    binary.op,
                    Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq | Operator::Eq
                );

                if is_filter_op {
                    // Try to extract column name from left side
                    if let Some(col_name) = Self::extract_column_name(&binary.left) {
                        columns.push(col_name);
                    }
                    // Also check right side (for cases like "10 < col")
                    if let Some(col_name) = Self::extract_column_name(&binary.right) {
                        columns.push(col_name);
                    }
                }

                // Recurse into AND expressions
                if matches!(binary.op, Operator::And) {
                    Self::collect_filter_columns(&binary.left, columns);
                    Self::collect_filter_columns(&binary.right, columns);
                }
            }
            Expr::Between(between) => {
                if let Some(col_name) = Self::extract_column_name(&between.expr) {
                    columns.push(col_name);
                }
            }
            _ => {}
        }
    }

    /// Extract column name from an expression if it's a column reference.
    fn extract_column_name(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Column(col) => Some(col.name.clone()),
            Expr::Cast(cast) => Self::extract_column_name(&cast.expr),
            _ => None,
        }
    }

    /// Load a parquet file and return an ExecutionPlan.
    /// Uses the catalog schema (self.schema) to ensure consistency with schema()
    /// and proper projection index alignment.
    ///
    /// Constructs a DataSourceExec directly from the known file path, bypassing
    /// ListingTable's list operation. Only performs a single head request (vs
    /// head+list with ListingTable). Filter pushdown is handled by the physical
    /// optimizer on DataSourceExec.
    #[tracing::instrument(
        name = "load_parquet_exec",
        skip(self, state, projection, limit, sort_columns),
        fields(runtimedb.parquet_url = %parquet_url)
    )]
    async fn load_parquet_exec(
        &self,
        parquet_url: &str,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
        sort_columns: Option<&Vec<(String, bool)>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        // parquet_url for cache tables is a directory URL (e.g., s3://bucket/.../version/)
        // Append data.parquet to get the actual file URL.
        let file_url = format!("{}/data.parquet", parquet_url.trim_end_matches('/'));

        super::parquet_exec::build_parquet_exec(
            &file_url,
            self.schema.clone(),
            state,
            projection,
            limit,
            sort_columns,
        )
        .await
    }

    /// Fetch the table data and update catalog
    async fn fetch_and_cache(&self) -> Result<String, DataFusionError> {
        let (url, _row_count) = self
            .orchestrator
            .cache_table(
                &self.source,
                &self.connection_id,
                &self.schema_name,
                &self.table_name,
            )
            .await
            .map_err(|e| {
                DataFusionError::External(format!("Failed to cache table: {}", e).into())
            })?;
        Ok(url)
    }
}

#[async_trait]
impl TableProvider for LazyTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        // Return schema immediately without any I/O
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    #[tracing::instrument(
        name = "lazy_table_scan",
        skip(self, state, projection, filters, limit),
        fields(
            runtimedb.connection_id = %self.connection_id,
            runtimedb.schema = %self.schema_name,
            runtimedb.table = %self.table_name,
            runtimedb.cache_hit = tracing::field::Empty,
            runtimedb.index_used = tracing::field::Empty,
        )
    )]
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        // Check if table is already cached
        let table_info = self
            .catalog
            .get_table(&self.connection_id, &self.schema_name, &self.table_name)
            .await
            .map_err(|e| {
                DataFusionError::External(format!("Failed to get table info: {}", e).into())
            })?
            .ok_or_else(|| {
                DataFusionError::External("Table not found in catalog".to_string().into())
            })?;

        let (parquet_url, cache_hit) = if let Some(path) = table_info.parquet_path {
            // Already cached, use existing path
            let url = if path.starts_with("file://") || path.starts_with("s3://") {
                path
            } else {
                format!("file://{}", path)
            };
            (url, true)
        } else {
            // Not cached, fetch now
            let url = self.fetch_and_cache().await?;
            (url, false)
        };

        let span = tracing::Span::current();
        span.record("runtimedb.cache_hit", cache_hit);

        // Check if we should use a sorted index for better row group pruning or ORDER BY
        // Priority: 1. Catalog indexes (user-created via SQL), 2. Index presets (hardcoded)
        let (final_url, sort_columns) = if let Some((index_url, sort_cols)) =
            self.select_catalog_index(filters).await
        {
            // Use catalog index - index_url is the full path to the index directory
            span.record("runtimedb.index_used", format!("catalog:{}", index_url).as_str());
            (index_url, Some(sort_cols))
        } else if let Some((preset_name, sort_cols)) = self.select_preset_index(filters) {
            // Use preset index path: {base_url}/presets/{name}
            let preset_url = format!(
                "{}/presets/{}",
                parquet_url.trim_end_matches('/'),
                preset_name
            );
            span.record("runtimedb.index_used", format!("preset:{}", preset_name).as_str());
            (preset_url, Some(sort_cols))
        } else {
            span.record("runtimedb.index_used", "none");
            (parquet_url, None)
        };

        // Load the parquet file and create execution plan with projection and limit pushdown.
        // Filter pushdown is handled by the physical optimizer on DataSourceExec.
        // If using a sorted index, pass sort columns so output ordering can be declared.
        self.load_parquet_exec(&final_url, state, projection, limit, sort_columns.as_ref())
            .await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        // Parquet supports filter pushdown
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datafetch::IndexPreset;
    use datafusion::common::Column;
    use datafusion::logical_expr::{lit, BinaryExpr};

    fn make_provider_with_presets(
        schema_name: &str,
        table_name: &str,
        presets: Vec<IndexPreset>,
    ) -> LazyTableProvider {
        use crate::catalog::MockCatalog;
        use crate::datafetch::NativeFetcher;
        use crate::secrets::SecretManager;
        use crate::storage::FilesystemStorage;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("ship_date", DataType::Date32, true),
        ]));

        let source = Arc::new(Source::Duckdb {
            path: ":memory:".to_string(),
        });

        // Create minimal mocks
        let storage = Arc::new(FilesystemStorage::new("/tmp"));
        let catalog: Arc<dyn CatalogManager> = Arc::new(MockCatalog::new());
        let fetcher = Arc::new(NativeFetcher::new());

        // Create a dummy SecretManager - just for the test, won't be used
        let secret_manager = {
            use crate::secrets::{EncryptedCatalogBackend, ENCRYPTED_PROVIDER_TYPE};
            let key = [0x42u8; 32];
            let backend = Arc::new(EncryptedCatalogBackend::new(key, catalog.clone()));
            Arc::new(SecretManager::new(
                backend,
                catalog.clone(),
                ENCRYPTED_PROVIDER_TYPE,
            ))
        };

        let orchestrator = Arc::new(crate::datafetch::FetchOrchestrator::new(
            fetcher,
            storage,
            catalog.clone(),
            secret_manager,
        ));

        let mut registry = IndexPresetRegistry::new();
        registry.register(schema_name, table_name, presets);

        LazyTableProvider::with_index_presets(
            schema,
            source,
            catalog,
            orchestrator,
            "conn".to_string(),
            schema_name.to_string(),
            table_name.to_string(),
            Arc::new(registry),
        )
    }

    #[test]
    fn test_extract_filter_columns_gt() {
        let filter = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column(Column::from_name("ship_date"))),
            Operator::Gt,
            Box::new(lit("2024-01-01")),
        ));

        let columns = LazyTableProvider::extract_filter_columns(&[filter]);
        assert_eq!(columns, vec!["ship_date"]);
    }

    #[test]
    fn test_extract_filter_columns_between() {
        let filter = Expr::Between(datafusion::logical_expr::Between::new(
            Box::new(Expr::Column(Column::from_name("order_date"))),
            false,
            Box::new(lit("2024-01-01")),
            Box::new(lit("2024-12-31")),
        ));

        let columns = LazyTableProvider::extract_filter_columns(&[filter]);
        assert_eq!(columns, vec!["order_date"]);
    }

    #[test]
    fn test_extract_filter_columns_equality_included() {
        let filter = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column(Column::from_name("id"))),
            Operator::Eq,
            Box::new(lit(42)),
        ));

        let columns = LazyTableProvider::extract_filter_columns(&[filter]);
        assert_eq!(columns, vec!["id"]);
    }

    #[test]
    fn test_select_preset_index_matches() {
        let provider = make_provider_with_presets(
            "main",
            "lineitem",
            vec![IndexPreset::new("by_shipdate", vec!["ship_date"])],
        );

        let filter = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column(Column::from_name("ship_date"))),
            Operator::GtEq,
            Box::new(lit("2024-01-01")),
        ));

        let result = provider.select_preset_index(&[filter]);
        assert_eq!(
            result,
            Some((
                "by_shipdate".to_string(),
                vec![("ship_date".to_string(), false)]
            ))
        );
    }

    #[test]
    fn test_select_preset_index_no_match() {
        let provider = make_provider_with_presets(
            "main",
            "lineitem",
            vec![IndexPreset::new("by_shipdate", vec!["ship_date"])],
        );

        // Filter on a column that doesn't have a preset index
        let filter = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column(Column::from_name("id"))),
            Operator::Gt,
            Box::new(lit(100)),
        ));

        let result = provider.select_preset_index(&[filter]);
        assert_eq!(result, None);
    }

    #[test]
    fn test_select_preset_index_case_insensitive() {
        let provider = make_provider_with_presets(
            "MAIN",
            "LINEITEM",
            vec![IndexPreset::new("by_shipdate", vec!["SHIP_DATE"])],
        );

        let filter = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column(Column::from_name("ship_date"))),
            Operator::Lt,
            Box::new(lit("2024-12-31")),
        ));

        let result = provider.select_preset_index(&[filter]);
        assert_eq!(
            result,
            Some((
                "by_shipdate".to_string(),
                vec![("SHIP_DATE".to_string(), false)]
            ))
        );
    }
}
