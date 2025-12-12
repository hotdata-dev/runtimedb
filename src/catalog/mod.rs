mod duckdb_manager;
mod migrations;
mod postgres_manager;
mod sqlite_manager;
mod store;

mod manager;

pub use duckdb_manager::DuckdbCatalogManager;
pub use manager::{CatalogManager, ConnectionInfo, TableInfo};
pub use postgres_manager::PostgresCatalogManager;
pub use sqlite_manager::SqliteCatalogManager;
