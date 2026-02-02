use crate::catalog::CatalogManager;
use async_trait::async_trait;
use datafusion::catalog::AsyncSchemaProvider;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::TableProvider;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::execution::SessionState;
use std::fmt::Debug;
use std::sync::Arc;

/// Async schema provider for the `runtimedb.results` schema.
/// Resolves result IDs to their parquet files.
///
/// Implements `AsyncSchemaProvider` for fully async table lookups without blocking.
/// Tables are looked up from CatalogManager on-demand.
pub struct ResultsSchemaProvider {
    catalog: Arc<dyn CatalogManager>,
    /// Session state for schema inference - shares the RuntimeEnv (and object stores) with the main session
    session_state: Arc<SessionState>,
}

impl Debug for ResultsSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResultsSchemaProvider")
            .field("catalog", &"...")
            .field("session_state", &"...")
            .finish()
    }
}

impl ResultsSchemaProvider {
    /// Create a new ResultsSchemaProvider using the RuntimeEnv from the given SessionContext.
    ///
    /// This creates a minimal session state that shares the RuntimeEnv (and thus object stores)
    /// with the provided context, avoiding the need to re-register storage backends.
    pub fn with_runtime_env(
        catalog: Arc<dyn CatalogManager>,
        ctx: &datafusion::prelude::SessionContext,
    ) -> Self {
        // Create a minimal session state that shares the RuntimeEnv
        let session_state = SessionStateBuilder::new()
            .with_runtime_env(ctx.runtime_env())
            .build();
        Self {
            catalog,
            session_state: Arc::new(session_state),
        }
    }
}

#[async_trait]
impl AsyncSchemaProvider for ResultsSchemaProvider {
    async fn table(&self, name: &str) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        // Look up result by ID - only returns ready results
        let result = match self.catalog.get_queryable_result(name).await {
            Ok(Some(r)) => r,
            Ok(None) => return Ok(None),
            Err(e) => {
                return Err(datafusion::error::DataFusionError::External(Box::new(
                    std::io::Error::other(e.to_string()),
                )));
            }
        };

        // Create listing table for the parquet file
        let parquet_path = result.parquet_path.ok_or_else(|| {
            datafusion::error::DataFusionError::External(Box::new(std::io::Error::other(
                "Result has no parquet path",
            )))
        })?;
        let table_path = ListingTableUrl::parse(&parquet_path)?;

        // Set up parquet format and listing options
        let file_format = ParquetFormat::default();
        let listing_options = ListingOptions::new(Arc::new(file_format));

        // Infer schema using the shared session state (which has access to object stores like S3)
        let schema = listing_options
            .infer_schema(self.session_state.as_ref(), &table_path)
            .await?;

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .with_schema(schema);

        let table: Arc<dyn TableProvider> = Arc::new(ListingTable::try_new(config)?);

        Ok(Some(table))
    }
}
