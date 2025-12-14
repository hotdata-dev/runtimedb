mod catalog_provider;
mod engine;
mod lazy_table_provider;
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

pub use catalog_provider::HotDataCatalogProvider;
pub use engine::HotDataEngine;
pub use engine::HotDataEngineBuilder;
pub use engine::QueryResponse;
pub use lazy_table_provider::LazyTableProvider;
pub use schema_provider::HotDataSchemaProvider;
