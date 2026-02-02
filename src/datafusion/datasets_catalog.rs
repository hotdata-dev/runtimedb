//! Catalog and schema providers for user-uploaded datasets.
//!
//! The `datasets` catalog provides access to datasets created via the datasets API.
//! Each dataset is stored as a parquet file and can be queried via SQL.

use crate::catalog::CatalogManager;
use crate::datasets::DEFAULT_SCHEMA;
use async_trait::async_trait;
use datafusion::catalog::{AsyncCatalogProvider, AsyncSchemaProvider};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::execution::SessionState;
use std::fmt::Debug;
use std::sync::Arc;

/// An async catalog provider for the "datasets" catalog.
/// Provides a single "main" schema containing all user-uploaded datasets.
#[derive(Debug)]
pub struct DatasetsCatalogProvider {
    schema: Arc<DatasetsSchemaProvider>,
}

impl DatasetsCatalogProvider {
    /// Create a new DatasetsCatalogProvider using the RuntimeEnv from the given SessionContext.
    ///
    /// This creates a minimal session state that shares the RuntimeEnv (and thus object stores)
    /// with the provided context, enabling fallback schema inference from parquet files.
    pub fn with_runtime_env(
        catalog: Arc<dyn CatalogManager>,
        ctx: &datafusion::prelude::SessionContext,
    ) -> Self {
        Self {
            schema: Arc::new(DatasetsSchemaProvider::with_runtime_env(catalog, ctx)),
        }
    }
}

#[async_trait]
impl AsyncCatalogProvider for DatasetsCatalogProvider {
    async fn schema(&self, name: &str) -> Result<Option<Arc<dyn AsyncSchemaProvider>>> {
        if name == DEFAULT_SCHEMA {
            Ok(Some(self.schema.clone()))
        } else {
            Ok(None)
        }
    }
}

/// Async schema provider for datasets.
///
/// Implements `AsyncSchemaProvider` for fully async table lookups without blocking.
/// Tables are looked up from CatalogManager on-demand.
pub struct DatasetsSchemaProvider {
    catalog: Arc<dyn CatalogManager>,
    /// Session state for fallback schema inference - shares the RuntimeEnv (and object stores)
    session_state: Arc<SessionState>,
}

impl Debug for DatasetsSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatasetsSchemaProvider")
            .field("catalog", &"...")
            .field("session_state", &"...")
            .finish()
    }
}

impl DatasetsSchemaProvider {
    /// Create a new DatasetsSchemaProvider using the RuntimeEnv from the given SessionContext.
    ///
    /// This creates a minimal session state that shares the RuntimeEnv (and thus object stores)
    /// with the provided context, enabling fallback schema inference from parquet files.
    pub fn with_runtime_env(
        catalog: Arc<dyn CatalogManager>,
        ctx: &datafusion::prelude::SessionContext,
    ) -> Self {
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
impl AsyncSchemaProvider for DatasetsSchemaProvider {
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        // Look up dataset by table_name
        let dataset = self
            .catalog
            .get_dataset_by_table_name(DEFAULT_SCHEMA, name)
            .await
            .map_err(|e| {
                DataFusionError::External(Box::new(std::io::Error::other(e.to_string())))
            })?;

        let info = match dataset {
            Some(info) => info,
            None => return Ok(None),
        };

        // Create listing table for the parquet file
        let table_path = ListingTableUrl::parse(&info.parquet_url)?;

        // Set up parquet format and listing options
        let file_format = ParquetFormat::default();
        let listing_options = ListingOptions::new(Arc::new(file_format));

        // Try to use stored schema from catalog, fall back to parquet inference if corrupted
        let schema: datafusion::arrow::datatypes::SchemaRef =
            match serde_json::from_str(&info.arrow_schema_json) {
                Ok(schema) => schema,
                Err(e) => {
                    // Log warning and fall back to schema inference from parquet file
                    tracing::warn!(
                        dataset = %name,
                        error = %e,
                        "Stored schema is corrupted, inferring from parquet file"
                    );
                    listing_options
                        .infer_schema(self.session_state.as_ref(), &table_path)
                        .await?
                }
            };

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .with_schema(schema);

        let table: Arc<dyn TableProvider> = Arc::new(ListingTable::try_new(config)?);

        Ok(Some(table))
    }
}
