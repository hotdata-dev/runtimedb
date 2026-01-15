# Refresh Endpoint Implementation Plan

## Overview

A unified `POST /refresh` endpoint that handles schema discovery and data refresh. Replaces the existing `POST /connections/{connection_id}/discover` endpoint.

**Key behaviors:**
- Schema refresh re-discovers tables, preserving cached data, and removes stale tables
- Data refresh uses atomic swap pattern - queries continue using old cache until new data is ready
- Old cache files tracked in database for deletion after 60-second grace period (survives restarts)

## API

### Endpoint

```
POST /refresh
```

### Request Body

```json
{
  "connection_id": 5,        // Optional: scope to specific connection
  "schema_name": "public",   // Optional: requires connection_id
  "table_name": "users",     // Optional: requires connection_id + schema_name
  "data": false              // Optional: refresh cached data (default: false)
}
```

### Validation Rules

- `schema_name` requires `connection_id`
- `table_name` requires both `connection_id` and `schema_name`
- `data: true` requires `connection_id` (no bulk data refresh across all connections)
- `schema_name` without `table_name` is rejected (schema-level refresh not supported)

### Response Types

**Schema refresh:**
```json
{
  "connections_refreshed": 3,
  "tables_discovered": 45,
  "tables_added": 2,
  "tables_removed": 1,
  "tables_modified": 3
}
```

**Table data refresh:**
```json
{
  "connection_id": 5,
  "schema_name": "public",
  "table_name": "users",
  "rows_synced": 15000,
  "duration_ms": 2340
}
```

**Connection data refresh:**
```json
{
  "connection_id": 5,
  "tables_refreshed": 12,
  "tables_failed": 1,
  "total_rows": 150000,
  "duration_ms": 45000,
  "errors": [
    {
      "schema_name": "public",
      "table_name": "large_table",
      "error": "Connection timeout"
    }
  ]
}
```

---

## Implementation

### Phase 1: New Types and Catalog Methods

**File: `src/http/models.rs`** - Add request/response types:

```rust
#[derive(Debug, Deserialize)]
pub struct RefreshRequest {
    pub connection_id: Option<i32>,
    pub schema_name: Option<String>,
    pub table_name: Option<String>,
    #[serde(default)]
    pub data: bool,
}

#[derive(Debug, Serialize)]
pub struct SchemaRefreshResult {
    pub connections_refreshed: usize,
    pub tables_discovered: usize,
    pub tables_added: usize,
    pub tables_removed: usize,
    pub tables_modified: usize,
}

#[derive(Debug, Serialize)]
pub struct TableRefreshResult {
    pub connection_id: i32,
    pub schema_name: String,
    pub table_name: String,
    pub rows_synced: usize,
    pub duration_ms: u64,
}

#[derive(Debug, Serialize)]
pub struct TableRefreshError {
    pub schema_name: String,
    pub table_name: String,
    pub error: String,
}

#[derive(Debug, Serialize)]
pub struct ConnectionRefreshResult {
    pub connection_id: i32,
    pub tables_refreshed: usize,
    pub tables_failed: usize,
    pub total_rows: usize,
    pub duration_ms: u64,
    pub errors: Vec<TableRefreshError>,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum RefreshResponse {
    Schema(SchemaRefreshResult),
    Table(TableRefreshResult),
    Connection(ConnectionRefreshResult),
}
```

**File: `src/catalog/manager.rs`** - Add methods to trait:

```rust
/// Delete tables for a connection that are NOT in the provided list.
/// Used during schema refresh to remove tables that no longer exist in the remote.
/// Returns the list of deleted table infos (for cache cleanup).
async fn delete_stale_tables(
    &self,
    connection_id: i32,
    current_tables: &[(String, String)], // (schema_name, table_name) pairs
) -> Result<Vec<TableInfo>>;

/// Get connection by internal ID.
async fn get_connection_by_id(&self, id: i32) -> Result<Option<ConnectionInfo>>;

/// Schedule a file path for deletion after a grace period.
/// Tracked in database to survive restarts.
async fn schedule_file_deletion(&self, path: &str, delete_after: DateTime<Utc>) -> Result<()>;

/// Get all pending file deletions that are due.
async fn get_due_deletions(&self) -> Result<Vec<PendingDeletion>>;

/// Remove a pending deletion record (after successful delete).
async fn remove_pending_deletion(&self, id: i32) -> Result<()>;
```

**New type for pending deletions:**

```rust
#[derive(Debug, Clone, FromRow)]
pub struct PendingDeletion {
    pub id: i32,
    pub path: String,
    pub delete_after: DateTime<Utc>,
}
```

**File: `src/catalog/backend.rs`** - Implement for generic backend:

```rust
pub async fn delete_stale_tables(
    &self,
    connection_id: i32,
    current_tables: &[(String, String)],
) -> Result<Vec<TableInfo>> {
    // 1. Get all existing tables for connection
    // 2. Find tables not in current_tables list
    // 3. Delete them and return the deleted TableInfo for cache cleanup
}

pub async fn get_connection_by_id(&self, id: i32) -> Result<Option<ConnectionInfo>> {
    let sql = format!(
        "SELECT id, external_id, name, source_type, config_json FROM connections WHERE id = {}",
        DB::bind_param(1)
    );
    let row = query_as::<_, ConnectionInfo>(&sql)
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
    Ok(row)
}

pub async fn schedule_file_deletion(&self, path: &str, delete_after: DateTime<Utc>) -> Result<()> {
    let sql = format!(
        "INSERT INTO pending_deletions (path, delete_after) VALUES ({}, {})",
        DB::bind_param(1),
        DB::bind_param(2)
    );
    query(&sql)
        .bind(path)
        .bind(delete_after)
        .execute(&self.pool)
        .await?;
    Ok(())
}

pub async fn get_due_deletions(&self) -> Result<Vec<PendingDeletion>> {
    let sql = format!(
        "SELECT id, path, delete_after FROM pending_deletions WHERE delete_after <= {}",
        DB::bind_param(1)
    );
    let rows = query_as::<_, PendingDeletion>(&sql)
        .bind(Utc::now())
        .fetch_all(&self.pool)
        .await?;
    Ok(rows)
}

pub async fn remove_pending_deletion(&self, id: i32) -> Result<()> {
    let sql = format!(
        "DELETE FROM pending_deletions WHERE id = {}",
        DB::bind_param(1)
    );
    query(&sql).bind(id).execute(&self.pool).await?;
    Ok(())
}
```

**Database migration** - Add `pending_deletions` table:

```sql
CREATE TABLE IF NOT EXISTS pending_deletions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    path TEXT NOT NULL,
    delete_after TIMESTAMP NOT NULL
);

CREATE INDEX idx_pending_deletions_due ON pending_deletions(delete_after);
```

### Phase 2: Schema Refresh Logic

**File: `src/datafetch/orchestrator.rs`** - Add discovery method:

```rust
/// Discover tables from a remote source.
/// Delegates to the underlying fetcher.
pub async fn discover_tables(
    &self,
    source: &Source,
) -> Result<Vec<TableMetadata>> {
    self.fetcher
        .discover_tables(source, &self.secret_manager)
        .await
}
```

**File: `src/engine.rs`** - Add `refresh_schema` method:

```rust
/// Refresh schema for a connection. Re-discovers tables from remote,
/// preserving cached data for existing tables, and removes stale tables.
///
/// Returns counts of tables added, removed, and modified.
pub async fn refresh_schema(
    &self,
    connection_id: i32,
) -> Result<(usize, usize, usize)> {
    // 1. Get connection info
    let conn = self.catalog.get_connection_by_id(connection_id).await?
        .ok_or_else(|| anyhow!("Connection not found"))?;

    // 2. Get existing tables (for comparison)
    let existing_tables = self.catalog.list_tables(Some(connection_id)).await?;
    let existing_set: HashSet<(String, String)> = existing_tables.iter()
        .map(|t| (t.schema_name.clone(), t.table_name.clone()))
        .collect();

    // 3. Discover current tables from remote (reuse orchestrator's fetcher)
    let source: Source = serde_json::from_str(&conn.config_json)?;
    let discovered = self.orchestrator.discover_tables(&source).await?;

    // 4. Build current table set
    let current_set: HashSet<(String, String)> = discovered.iter()
        .map(|t| (t.schema_name.clone(), t.table_name.clone()))
        .collect();

    // 5. Calculate changes
    let added: Vec<_> = current_set.difference(&existing_set).collect();
    let removed: Vec<_> = existing_set.difference(&current_set).collect();

    // 6. Upsert all discovered tables (add_table is already an upsert)
    let mut modified = 0;
    for table in &discovered {
        let schema_json = serde_json::to_string(&table.to_arrow_schema())?;

        // Check if schema changed for existing tables
        if let Some(existing) = existing_tables.iter()
            .find(|t| t.schema_name == table.schema_name && t.table_name == table.table_name)
        {
            if existing.arrow_schema_json.as_ref() != Some(&schema_json) {
                modified += 1;
            }
        }

        self.catalog.add_table(
            connection_id,
            &table.schema_name,
            &table.table_name,
            &schema_json,
        ).await?;
    }

    // 7. Delete stale tables from catalog and get their paths for cleanup
    let current_table_list: Vec<_> = current_set.into_iter().collect();
    let deleted_tables = self.catalog
        .delete_stale_tables(connection_id, &current_table_list)
        .await?;

    // 8. Schedule cache file deletion for removed tables
    for table_info in deleted_tables {
        if let Some(path) = table_info.parquet_path {
            self.schedule_file_deletion(&path).await?;
        }
    }

    Ok((added.len(), removed.len(), modified))
}

/// Refresh schema for all connections.
pub async fn refresh_all_schemas(&self) -> Result<SchemaRefreshResult> {
    let connections = self.catalog.list_connections().await?;
    let mut result = SchemaRefreshResult {
        connections_refreshed: 0,
        tables_discovered: 0,
        tables_added: 0,
        tables_removed: 0,
        tables_modified: 0,
    };

    for conn in connections {
        let (added, removed, modified) = self.refresh_schema(conn.id).await?;
        result.connections_refreshed += 1;
        result.tables_added += added;
        result.tables_removed += removed;
        result.tables_modified += modified;
    }

    // Count total discovered tables
    result.tables_discovered = self.catalog.list_tables(None).await?.len();

    Ok(result)
}
```

**Remove `discover_connection()`** - Delete the existing method and update callers to use `refresh_schema()`.

### Phase 3: Data Refresh with Atomic Swap

**File: `src/storage/mod.rs`** - Add versioned path method to trait:

```rust
/// Prepare a versioned cache write path for atomic refresh.
/// Returns a local path with unique suffix to avoid overwriting existing cache.
fn prepare_versioned_cache_write(
    &self,
    connection_id: i32,
    schema_name: &str,
    table_name: &str,
) -> PathBuf;
```

**File: `src/storage/filesystem.rs`**:

```rust
fn prepare_versioned_cache_write(
    &self,
    connection_id: i32,
    schema_name: &str,
    table_name: &str,
) -> PathBuf {
    let version = nanoid::nanoid!(8);
    self.cache_dir
        .join(connection_id.to_string())
        .join(schema_name)
        .join(table_name)
        .join(format!("{}_v{}.parquet", table_name, version))
}
```

**File: `src/storage/s3.rs`**:

```rust
fn prepare_versioned_cache_write(
    &self,
    connection_id: i32,
    schema_name: &str,
    table_name: &str,
) -> PathBuf {
    let version = nanoid::nanoid!(8);
    // S3 storage uses local temp dir for writing, then uploads
    self.temp_dir
        .join(connection_id.to_string())
        .join(schema_name)
        .join(table_name)
        .join(format!("{}_v{}.parquet", table_name, version))
}
```

**File: `src/datafetch/orchestrator.rs`** - Add refresh method:

```rust
/// Refresh table data with atomic swap semantics.
/// Writes to new versioned path, then atomically updates catalog.
/// Returns (new_path, old_path, row_count) for cleanup scheduling.
pub async fn refresh_table(
    &self,
    source: &Source,
    connection_id: i32,
    schema_name: &str,
    table_name: &str,
) -> Result<(String, Option<String>, usize)> {
    // 1. Get current path (will be deleted after grace period)
    let old_info = self.catalog
        .get_table(connection_id, schema_name, table_name)
        .await?;
    let old_path = old_info.as_ref().and_then(|i| i.parquet_path.clone());

    // 2. Generate versioned new path
    let new_local_path = self.storage
        .prepare_versioned_cache_write(connection_id, schema_name, table_name);

    // 3. Ensure parent directory exists
    if let Some(parent) = new_local_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // 4. Fetch and write to new path
    let mut writer = StreamingParquetWriter::new(&new_local_path)?;
    self.fetcher
        .fetch_table(source, &self.secret_manager, None, schema_name, table_name, &mut writer)
        .await?;
    let row_count = writer.close()?;

    // 5. Finalize (upload to S3 if needed)
    let new_url = self.storage
        .finalize_cache_write(&new_local_path, connection_id, schema_name, table_name)
        .await?;

    // 6. Atomic catalog update
    if let Some(info) = self.catalog
        .get_table(connection_id, schema_name, table_name)
        .await?
    {
        self.catalog.update_table_sync(info.id, &new_url).await?;
    }

    Ok((new_url, old_path, row_count))
}
```

**File: `src/engine.rs`** - Add data refresh methods:

```rust
/// Refresh data for a single table using atomic swap.
pub async fn refresh_table_data(
    &self,
    connection_id: i32,
    schema_name: &str,
    table_name: &str,
) -> Result<TableRefreshResult> {
    let start = Instant::now();

    let conn = self.catalog.get_connection_by_id(connection_id).await?
        .ok_or_else(|| anyhow!("Connection not found"))?;
    let source: Source = serde_json::from_str(&conn.config_json)?;

    let (_, old_path, rows) = self.orchestrator
        .refresh_table(&source, connection_id, schema_name, table_name)
        .await?;

    // Schedule old file deletion after grace period
    if let Some(path) = old_path {
        self.schedule_file_deletion(&path).await?;
    }

    Ok(TableRefreshResult {
        connection_id,
        schema_name: schema_name.to_string(),
        table_name: table_name.to_string(),
        rows_synced: rows,
        duration_ms: start.elapsed().as_millis() as u64,
    })
}

/// Refresh data for all tables in a connection.
pub async fn refresh_connection_data(
    &self,
    connection_id: i32,
) -> Result<ConnectionRefreshResult> {
    let start = Instant::now();
    let tables = self.catalog.list_tables(Some(connection_id)).await?;

    let conn = self.catalog.get_connection_by_id(connection_id).await?
        .ok_or_else(|| anyhow!("Connection not found"))?;
    let source: Source = serde_json::from_str(&conn.config_json)?;

    let mut result = ConnectionRefreshResult {
        connection_id,
        tables_refreshed: 0,
        tables_failed: 0,
        total_rows: 0,
        duration_ms: 0,
        errors: vec![],
    };

    // Refresh tables with concurrency limit
    let semaphore = Arc::new(Semaphore::new(4));
    let mut handles = vec![];

    for table in tables {
        let permit = semaphore.clone().acquire_owned().await?;
        let orchestrator = self.orchestrator.clone();
        let source = source.clone();

        let handle = tokio::spawn(async move {
            let result = orchestrator
                .refresh_table(&source, connection_id, &table.schema_name, &table.table_name)
                .await;
            drop(permit);
            (table, result)
        });
        handles.push(handle);
    }

    for handle in handles {
        let (table, refresh_result) = handle.await?;
        match refresh_result {
            Ok((_, old_path, rows)) => {
                result.tables_refreshed += 1;
                result.total_rows += rows;
                if let Some(path) = old_path {
                    self.schedule_file_deletion(&path).await?;
                }
            }
            Err(e) => {
                result.tables_failed += 1;
                result.errors.push(TableRefreshError {
                    schema_name: table.schema_name,
                    table_name: table.table_name,
                    error: e.to_string(),
                });
            }
        }
    }

    result.duration_ms = start.elapsed().as_millis() as u64;
    Ok(result)
}

/// Schedule file deletion after grace period (persisted to database).
async fn schedule_file_deletion(&self, path: &str) -> Result<()> {
    let delete_after = Utc::now() + chrono::Duration::seconds(60);
    self.catalog.schedule_file_deletion(path, delete_after).await
}

/// Process any pending file deletions that are due.
/// Called on startup and periodically.
pub async fn process_pending_deletions(&self) -> Result<usize> {
    let pending = self.catalog.get_due_deletions().await?;
    let mut deleted = 0;

    for deletion in pending {
        match self.storage.delete(&deletion.path).await {
            Ok(_) => {
                self.catalog.remove_pending_deletion(deletion.id).await?;
                deleted += 1;
            }
            Err(e) => {
                // Log but don't fail - will retry next time
                warn!("Failed to delete {}: {}", deletion.path, e);
            }
        }
    }

    Ok(deleted)
}
```

### Phase 4: Background Deletion Worker

**File: `src/engine.rs`** - Add startup hook and background task:

```rust
impl RuntimeEngineBuilder {
    pub async fn build(self) -> Result<RuntimeEngine> {
        // ... existing build logic ...

        // Process any pending deletions from previous runs
        engine.process_pending_deletions().await?;

        // Start background deletion worker
        engine.start_deletion_worker();

        Ok(engine)
    }
}

impl RuntimeEngine {
    /// Start background task that processes pending deletions every 30 seconds.
    fn start_deletion_worker(&self) {
        let catalog = self.catalog.clone();
        let storage = self.storage.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            loop {
                interval.tick().await;

                let pending = match catalog.get_due_deletions().await {
                    Ok(p) => p,
                    Err(e) => {
                        warn!("Failed to get pending deletions: {}", e);
                        continue;
                    }
                };

                for deletion in pending {
                    if let Err(e) = storage.delete(&deletion.path).await {
                        warn!("Failed to delete {}: {}", deletion.path, e);
                        continue;
                    }
                    if let Err(e) = catalog.remove_pending_deletion(deletion.id).await {
                        warn!("Failed to remove deletion record {}: {}", deletion.id, e);
                    }
                }
            }
        });
    }
}
```

### Phase 5: HTTP Handler and Route

**File: `src/http/handlers.rs`** - Add handler:

```rust
pub async fn refresh_handler(
    State(engine): State<Arc<RuntimeEngine>>,
    Json(request): Json<RefreshRequest>,
) -> Result<Json<RefreshResponse>, ApiError> {
    // Validate request
    if request.schema_name.is_some() && request.connection_id.is_none() {
        return Err(ApiError::bad_request("schema_name requires connection_id"));
    }
    if request.table_name.is_some() && request.schema_name.is_none() {
        return Err(ApiError::bad_request("table_name requires schema_name"));
    }
    if request.data && request.connection_id.is_none() {
        return Err(ApiError::bad_request("data refresh requires connection_id"));
    }

    let response = match (request.connection_id, request.schema_name, request.table_name, request.data) {
        // Schema refresh: all connections
        (None, None, None, false) => {
            let result = engine.refresh_all_schemas().await?;
            RefreshResponse::Schema(result)
        }

        // Schema refresh: single connection
        (Some(conn_id), None, None, false) => {
            let (added, removed, modified) = engine.refresh_schema(conn_id).await?;
            let tables = engine.catalog().list_tables(Some(conn_id)).await?;
            RefreshResponse::Schema(SchemaRefreshResult {
                connections_refreshed: 1,
                tables_discovered: tables.len(),
                tables_added: added,
                tables_removed: removed,
                tables_modified: modified,
            })
        }

        // Data refresh: single table
        (Some(conn_id), Some(schema), Some(table), true) => {
            let result = engine.refresh_table_data(conn_id, &schema, &table).await?;
            RefreshResponse::Table(result)
        }

        // Data refresh: all tables in connection
        (Some(conn_id), None, None, true) => {
            let result = engine.refresh_connection_data(conn_id).await?;
            RefreshResponse::Connection(result)
        }

        // Invalid: schema-level refresh not supported
        (Some(_), Some(_), None, false) => {
            return Err(ApiError::bad_request(
                "schema-level refresh not supported; omit schema_name to refresh entire connection"
            ));
        }

        // Invalid: data refresh with schema but no table
        (Some(_), Some(_), None, true) => {
            return Err(ApiError::bad_request(
                "data refresh with schema_name requires table_name"
            ));
        }

        // Invalid: schema refresh cannot target specific table
        (Some(_), Some(_), Some(_), false) => {
            return Err(ApiError::bad_request(
                "schema refresh cannot target specific table; use data: true for table data refresh"
            ));
        }

        _ => unreachable!(),
    };

    Ok(Json(response))
}
```

**File: `src/http/app_server.rs`** - Update routes:

```rust
// Remove old discover route
// - .route("/connections/{connection_id}/discover", post(discover_handler))

// Add new refresh route
.route("/refresh", post(refresh_handler))
```

### Phase 6: Remove `discover_connection()` and Update Tests

**File: `src/engine.rs`** - Remove `discover_connection()` method.

**File: `tests/integration_tests.rs`** - Update tests:
- Replace calls to `discover_connection()` with `refresh_schema()`
- Update assertions to use new return type `(added, removed, modified)`

---

## Testing

### Unit Tests

**File: `src/catalog/backend.rs`** - Test `delete_stale_tables`:

```rust
#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_delete_stale_tables_removes_missing() {
        // Setup: connection with tables A, B, C
        // Call delete_stale_tables with [A, B]
        // Assert: C is deleted, A and B remain
    }

    #[tokio::test]
    async fn test_delete_stale_tables_preserves_cache_paths() {
        // Setup: tables with parquet_path set
        // Call delete_stale_tables
        // Assert: returned TableInfo includes parquet_path for cleanup
    }

    #[tokio::test]
    async fn test_delete_stale_tables_empty_current_list() {
        // Setup: connection with tables
        // Call with empty current_tables list
        // Assert: all tables deleted
    }

    #[tokio::test]
    async fn test_delete_stale_tables_no_deletions() {
        // Setup: tables A, B
        // Call with [A, B]
        // Assert: returns empty vec, tables unchanged
    }

    #[tokio::test]
    async fn test_pending_deletions_crud() {
        // Test schedule, get_due, and remove operations
    }

    #[tokio::test]
    async fn test_pending_deletions_respects_time() {
        // Schedule deletion for future
        // get_due_deletions returns empty
        // Advance time past delete_after
        // get_due_deletions returns the record
    }
}
```

**File: `src/engine.rs`** - Test schema refresh logic:

```rust
#[cfg(test)]
mod refresh_tests {
    #[tokio::test]
    async fn test_refresh_schema_detects_added_tables() {
        // Setup: connection with 2 tables
        // Add new table to source
        // Call refresh_schema
        // Assert: added count = 1
    }

    #[tokio::test]
    async fn test_refresh_schema_detects_removed_tables() {
        // Setup: connection with 3 tables (one cached)
        // Drop table from source
        // Call refresh_schema
        // Assert: removed count = 1, deletion scheduled
    }

    #[tokio::test]
    async fn test_refresh_schema_detects_modified_schemas() {
        // Setup: table with schema {id: int}
        // Alter table to add column
        // Call refresh_schema
        // Assert: modified count = 1
    }

    #[tokio::test]
    async fn test_refresh_schema_preserves_cache_paths() {
        // Setup: table with parquet_path
        // Refresh schema (table still exists)
        // Assert: parquet_path unchanged
    }
}
```

**File: `src/storage/filesystem.rs`** - Test versioned paths:

```rust
#[cfg(test)]
mod tests {
    #[test]
    fn test_versioned_cache_path_unique() {
        // Call prepare_versioned_cache_write twice
        // Assert: paths are different
    }

    #[test]
    fn test_versioned_cache_path_structure() {
        // Call prepare_versioned_cache_write
        // Assert: path contains connection_id, schema, table, version suffix
    }
}
```

### Integration Tests

**File: `tests/refresh_tests.rs`**:

```rust
mod fixtures;
use fixtures::duckdb_fixtures;

// ============================================================================
// Schema Refresh Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_schema_all_connections() {
    // Setup: 2 connections with tables
    // POST /refresh {}
    // Assert: response includes both connections
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_schema_single_connection() {
    // Setup: connection with 3 tables
    // POST /refresh { connection_id: 1 }
    // Assert: tables_discovered = 3
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_schema_detects_new_table() {
    // Setup: connection, discover tables
    // Add new table to DuckDB
    // POST /refresh { connection_id: 1 }
    // Assert: tables_added = 1
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_schema_detects_removed_table() {
    // Setup: connection, discover tables, query table (creates cache)
    // Drop table from DuckDB
    // POST /refresh { connection_id: 1 }
    // Assert: tables_removed = 1
    // Assert: deletion scheduled in pending_deletions table
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_schema_detects_column_change() {
    // Setup: connection with table {id: int}
    // ALTER TABLE ADD COLUMN name VARCHAR
    // POST /refresh { connection_id: 1 }
    // Assert: tables_modified = 1
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_schema_preserves_cached_data() {
    // Setup: connection, query table (creates cache)
    // POST /refresh { connection_id: 1 }
    // Assert: parquet_path unchanged, query still works
}

// ============================================================================
// Data Refresh Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_table_data_creates_new_cache() {
    // Setup: connection, query table
    // Insert new rows into source
    // POST /refresh { connection_id: 1, schema_name: "main", table_name: "orders", data: true }
    // Query again
    // Assert: new rows visible
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_table_data_atomic_swap() {
    // Setup: table with cached data
    // Note original parquet_path
    // POST /refresh { ..., data: true }
    // Assert: parquet_path changed (versioned)
    // Assert: old path scheduled for deletion
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_connection_data_parallel() {
    // Setup: connection with 5 tables
    // POST /refresh { connection_id: 1, data: true }
    // Assert: all tables refreshed
    // Assert: duration < 5 * single_table_time (parallelism working)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_connection_data_partial_failure() {
    // Setup: connection with 3 tables, one will fail (e.g., dropped during refresh)
    // POST /refresh { connection_id: 1, data: true }
    // Assert: tables_refreshed = 2, tables_failed = 1
    // Assert: errors array contains failure details
}

// ============================================================================
// Pending Deletion Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_pending_deletion_created_on_refresh() {
    // Setup: table with cached data
    // POST /refresh { ..., data: true }
    // Query pending_deletions table
    // Assert: old path is in pending_deletions with future delete_after
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pending_deletion_processed_after_grace_period() {
    // Setup: table with cached data
    // POST /refresh { ..., data: true }
    // Note old file path
    // Assert: old file still exists
    // Call process_pending_deletions (simulating time passage)
    // Assert: old file deleted, pending_deletions record removed
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pending_deletions_processed_on_startup() {
    // Setup: insert old pending_deletion record (past delete_after)
    // Create new engine (simulates restart)
    // Assert: old file deleted during startup
}

// ============================================================================
// Validation Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_validation_schema_requires_connection() {
    // POST /refresh { schema_name: "public" }
    // Assert: 400 Bad Request
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_validation_table_requires_schema() {
    // POST /refresh { connection_id: 1, table_name: "users" }
    // Assert: 400 Bad Request
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_validation_data_requires_connection() {
    // POST /refresh { data: true }
    // Assert: 400 Bad Request
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_validation_schema_level_not_supported() {
    // POST /refresh { connection_id: 1, schema_name: "public" }
    // Assert: 400 Bad Request with clear message
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_nonexistent_connection() {
    // POST /refresh { connection_id: 999 }
    // Assert: 404 Not Found
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_nonexistent_table() {
    // POST /refresh { connection_id: 1, schema_name: "main", table_name: "nope", data: true }
    // Assert: 404 Not Found
}

// ============================================================================
// HTTP API Tests (using TestExecutor pattern)
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_api_refresh_schema_response_format() {
    // POST /refresh {}
    // Assert JSON structure matches SchemaRefreshResult
}

#[tokio::test(flavor = "multi_thread")]
async fn test_api_refresh_table_response_format() {
    // POST /refresh { connection_id, schema_name, table_name, data: true }
    // Assert JSON structure matches TableRefreshResult
}

#[tokio::test(flavor = "multi_thread")]
async fn test_api_refresh_connection_response_format() {
    // POST /refresh { connection_id, data: true }
    // Assert JSON structure matches ConnectionRefreshResult
}
```

**File: `tests/integration_tests.rs`** - Add to existing test harness:

```rust
// Add to TestExecutor trait
async fn refresh_schema(&self, connection_id: Option<i32>) -> Result<SchemaRefreshResult>;
async fn refresh_table_data(&self, connection_id: i32, schema: &str, table: &str) -> Result<TableRefreshResult>;
async fn refresh_connection_data(&self, connection_id: i32) -> Result<ConnectionRefreshResult>;

// Implement for EngineExecutor (direct calls to engine methods)
// Implement for ApiExecutor (HTTP POST /refresh)

// Add unified tests that run against both executors
async fn run_refresh_schema_test(executor: &dyn TestExecutor) { ... }
async fn run_refresh_data_test(executor: &dyn TestExecutor) { ... }
```

---

## Files Changed

### New Files
- `tests/refresh_tests.rs` - Integration tests for refresh endpoint
- Database migration for `pending_deletions` table

### Modified Files
- `src/catalog/manager.rs` - Add `delete_stale_tables`, `get_connection_by_id`, pending deletion methods
- `src/catalog/backend.rs` - Implement new methods
- `src/catalog/sqlite_manager.rs` - Delegate to backend
- `src/catalog/postgres_manager.rs` - Delegate to backend
- `src/storage/mod.rs` - Add `prepare_versioned_cache_write` to trait
- `src/storage/filesystem.rs` - Implement versioned paths
- `src/storage/s3.rs` - Implement versioned paths
- `src/datafetch/orchestrator.rs` - Add `discover_tables` and `refresh_table` methods
- `src/engine.rs` - Add refresh methods, remove `discover_connection()`, add deletion worker
- `src/http/models.rs` - Add request/response types
- `src/http/handlers.rs` - Add `refresh_handler`
- `src/http/app_server.rs` - Replace discover route with refresh route
- `tests/integration_tests.rs` - Update to use refresh_schema, add to TestExecutor trait

### Removed
- `POST /connections/{connection_id}/discover` route and handler
- `engine.discover_connection()` method

---

## Migration Notes

The old `POST /connections/{connection_id}/discover` endpoint is removed. Clients should migrate to:

```
POST /refresh
{ "connection_id": <id> }
```

This is functionally equivalent but returns more detailed information about what changed.
