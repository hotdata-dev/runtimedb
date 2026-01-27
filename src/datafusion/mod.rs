mod bounded_cache;
mod catalog_provider;
mod datasets_catalog;
mod information_schema;
mod lazy_table_provider;
mod results_schema;
mod runtimedb_catalog;
mod schema_provider;

use tokio::task::block_in_place;

/// Blocking helper for async operations.
///
/// Uses `block_in_place` to avoid blocking the tokio runtime when calling
/// async code from a sync context. This is needed when async catalog methods
/// must be called from sync trait implementations (e.g., DataFusion's CatalogProvider).
pub(crate) fn block_on<F, T>(f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    block_in_place(|| tokio::runtime::Handle::current().block_on(f))
}

pub use catalog_provider::RuntimeCatalogProvider;
pub use datasets_catalog::{DatasetsCatalogProvider, DatasetsSchemaProvider};
pub use information_schema::InformationSchemaProvider;
pub use lazy_table_provider::LazyTableProvider;
pub use runtimedb_catalog::RuntimeDbCatalogProvider;
pub use schema_provider::RuntimeSchemaProvider;
