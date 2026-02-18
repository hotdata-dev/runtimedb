mod backend;
mod migrations;
mod postgres_manager;
mod sqlite_manager;

mod manager;

mod caching_manager;
pub use caching_manager::CachingCatalogManager;

#[cfg(test)]
mod mock_catalog;
#[cfg(test)]
pub use mock_catalog::MockCatalog;

pub use manager::{
    sql_hash, CatalogManager, ConnectionInfo, CreateQueryRun, DatasetInfo, OptimisticLock,
    PendingDeletion, QueryResult, QueryResultRow, QueryRun, QueryRunCursor, QueryRunRow,
    QueryRunRowPg, QueryRunStatus, QueryRunUpdate, ResultStatus, ResultUpdate, SavedQuery,
    SavedQueryVersion, SqlSnapshot, TableInfo, UploadInfo,
};
pub use postgres_manager::PostgresCatalogManager;
pub use sqlite_manager::SqliteCatalogManager;

/// Check if an error is a unique constraint violation on the dataset table_name.
///
/// Uses structured error inspection for Postgres (constraint name) and a follow-up
/// query for SQLite (since SQLite doesn't expose constraint names).
///
/// # Arguments
/// * `catalog` - The catalog manager to query for conflict verification
/// * `err` - The error to check
/// * `schema` - The schema name for the dataset
/// * `table_name` - The table name that may be in conflict
/// * `current_id` - For updates, the ID of the dataset being updated (to exclude from conflict check)
///
/// # Returns
/// * `true` if the error is a table_name unique constraint violation
/// * `false` otherwise
pub async fn is_dataset_table_name_conflict(
    catalog: &dyn CatalogManager,
    err: &anyhow::Error,
    schema: &str,
    table_name: &str,
    current_id: Option<&str>,
) -> bool {
    let Some(sqlx_err) = err.downcast_ref::<sqlx::Error>() else {
        return false;
    };

    let sqlx::Error::Database(db_err) = sqlx_err else {
        return false;
    };

    let code = db_err.code().map(|c| c.to_string());

    // Check for unique constraint violation error codes
    let is_unique_violation = matches!(
        code.as_deref(),
        Some("23505") |  // Postgres unique_violation
        Some("2067") |   // SQLite SQLITE_CONSTRAINT_UNIQUE
        Some("1555") // SQLite SQLITE_CONSTRAINT_PRIMARYKEY
    );

    if !is_unique_violation {
        return false;
    }

    // For Postgres, we can check the constraint name directly (fast path)
    if let Some(constraint) = db_err.constraint() {
        return constraint == "datasets_schema_name_table_name_key";
    }

    // For SQLite, constraint() returns None, so verify by querying
    // if a dataset with this table_name exists (and isn't the one being updated)
    match catalog.get_dataset_by_table_name(schema, table_name).await {
        Ok(Some(existing)) => {
            // If updating, check if conflict is with a different dataset
            match current_id {
                Some(id) => existing.id != id,
                None => true, // Creating - any existing dataset is a conflict
            }
        }
        _ => false, // Query failed or no conflict found - not a table_name conflict
    }
}
