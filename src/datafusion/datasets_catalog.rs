//! Catalog and schema providers for user-uploaded datasets.
//!
//! The `datasets` catalog provides access to datasets created via the datasets API.
//! Each dataset is stored as a parquet file and can be queried via SQL.

use crate::catalog::CatalogManager;
use crate::datasets::DEFAULT_SCHEMA;
use async_trait::async_trait;
use datafusion::catalog::{AsyncCatalogProvider, AsyncSchemaProvider};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTableUrl};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::execution::SessionState;
use std::fmt::Debug;
use std::sync::{Arc, OnceLock};

use super::parquet_exec::SingleFileParquetProvider;

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
/// Tables are looked up from CatalogManager on-demand and served as single-file
/// parquet providers, avoiding ListingTable's list+head overhead.
pub struct DatasetsSchemaProvider {
    catalog: Arc<dyn CatalogManager>,
    /// Lazily initialized session state for fallback schema inference.
    /// Only constructed if a stored schema is corrupted and we need to infer from parquet.
    /// Shares the RuntimeEnv (and object stores) with the main session.
    session_state: OnceLock<Arc<SessionState>>,
    runtime_env: Arc<RuntimeEnv>,
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
    pub fn with_runtime_env(
        catalog: Arc<dyn CatalogManager>,
        ctx: &datafusion::prelude::SessionContext,
    ) -> Self {
        Self {
            catalog,
            session_state: OnceLock::new(),
            runtime_env: ctx.runtime_env(),
        }
    }

    /// Get or lazily initialize the session state for schema inference.
    fn session_state(&self) -> &Arc<SessionState> {
        self.session_state.get_or_init(|| {
            Arc::new(
                SessionStateBuilder::new()
                    .with_runtime_env(self.runtime_env.clone())
                    .build(),
            )
        })
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
                    let table_path = ListingTableUrl::parse(&info.parquet_url)?;
                    let file_format = ParquetFormat::default();
                    let listing_options = ListingOptions::new(Arc::new(file_format));
                    listing_options
                        .infer_schema(self.session_state().as_ref(), &table_path)
                        .await?
                }
            };

        let table: Arc<dyn TableProvider> =
            Arc::new(SingleFileParquetProvider::new(schema, info.parquet_url));

        Ok(Some(table))
    }
}
