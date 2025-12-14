mod backend;
mod migrations;
mod postgres_manager;
mod sqlite_manager;

mod manager;

pub use manager::{CatalogManager, ConnectionInfo, TableInfo};
pub use postgres_manager::PostgresCatalogManager;
pub use sqlite_manager::SqliteCatalogManager;
