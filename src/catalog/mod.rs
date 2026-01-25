mod backend;
mod migrations;
mod postgres_manager;
mod sqlite_manager;

mod manager;

pub use manager::{
    CatalogManager, ConnectionInfo, DatasetInfo, OptimisticLock, PendingDeletion, QueryResult,
    TableInfo, UploadInfo,
};
pub use postgres_manager::PostgresCatalogManager;
pub use sqlite_manager::SqliteCatalogManager;

/// Check if an error is a unique constraint violation on the dataset table_name.
///
/// Uses structured error inspection instead of string matching to avoid
/// misclassifying unrelated database errors.
///
/// Returns true if:
/// - Postgres: error code 23505 (unique_violation) with constraint "datasets_schema_name_table_name_key"
/// - SQLite: error code 2067 (SQLITE_CONSTRAINT_UNIQUE) or 1555 (SQLITE_CONSTRAINT_PRIMARYKEY)
pub fn is_dataset_table_name_conflict(err: &anyhow::Error) -> bool {
    if let Some(sqlx_err) = err.downcast_ref::<sqlx::Error>() {
        if let sqlx::Error::Database(db_err) = sqlx_err {
            let code = db_err.code().map(|c| c.to_string());

            // Check for unique constraint violation error codes
            let is_unique_violation = matches!(
                code.as_deref(),
                Some("23505") |  // Postgres unique_violation
                Some("2067") |   // SQLite SQLITE_CONSTRAINT_UNIQUE
                Some("1555") // SQLite SQLITE_CONSTRAINT_PRIMARYKEY
            );

            if is_unique_violation {
                // For Postgres, verify it's the table_name constraint specifically
                if let Some(constraint) = db_err.constraint() {
                    return constraint == "datasets_schema_name_table_name_key";
                }
                // For SQLite, constraint() returns None, so check the message
                // to ensure it's about the datasets table
                let msg = db_err.message().to_lowercase();
                return msg.contains("datasets") && msg.contains("table_name");
            }
        }
    }
    false
}
