# Async Result Persistence Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make result persistence async and move it entirely into the engine, keeping the query handler lightweight.

**Architecture:** Generate result_id upfront, insert a "processing" catalog entry, spawn a background task to write parquet and update status to "ready" or "failed". The query handler just calls one engine method and returns immediately.

**Tech Stack:** Rust, tokio (async/spawn), SQLx (migrations), DataFusion

---

## Task 1: Add status column to results table via migration v4

**Files:**
- Create: `migrations/sqlite/v4.sql`
- Create: `migrations/postgres/v4.sql`

**Step 1: Create SQLite migration**

Create `migrations/sqlite/v4.sql`:

```sql
-- Add status column to results table for async persistence tracking
-- Status values: 'processing', 'ready', 'failed'
ALTER TABLE results ADD COLUMN status TEXT NOT NULL DEFAULT 'ready';

-- Add index for filtering by status (e.g., finding processing results)
CREATE INDEX idx_results_status ON results(status);
```

**Step 2: Create PostgreSQL migration**

Create `migrations/postgres/v4.sql`:

```sql
-- Add status column to results table for async persistence tracking
-- Status values: 'processing', 'ready', 'failed'
ALTER TABLE results ADD COLUMN status TEXT NOT NULL DEFAULT 'ready';

-- Add index for filtering by status (e.g., finding processing results)
CREATE INDEX idx_results_status ON results(status);
```

**Step 3: Verify migrations compile**

Run: `cargo build`
Expected: Build succeeds (build.rs processes migrations)

**Step 4: Commit**

```bash
git add migrations/sqlite/v4.sql migrations/postgres/v4.sql
git commit -m "feat(catalog): add status column to results table"
```

---

## Task 2: Update QueryResult struct and CatalogManager trait

**Files:**
- Modify: `src/catalog/manager.rs:53-59` (QueryResult struct)
- Modify: `src/catalog/manager.rs:215-226` (trait methods)

**Step 1: Add status field to QueryResult struct**

In `src/catalog/manager.rs`, update the `QueryResult` struct:

```rust
/// A persisted query result.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct QueryResult {
    pub id: String, // nanoid
    pub parquet_path: Option<String>, // Now optional - null during processing
    pub status: String, // "processing", "ready", "failed"
    pub created_at: DateTime<Utc>,
}
```

**Step 2: Add new trait methods to CatalogManager**

Add these methods to the `CatalogManager` trait after the existing result methods:

```rust
    /// Store a result with "processing" status (no parquet path yet).
    async fn store_result_pending(&self, id: &str, created_at: DateTime<Utc>) -> Result<()>;

    /// Finalize a result: set status to "ready" and store the parquet path.
    async fn finalize_result(&self, id: &str, parquet_path: &str) -> Result<()>;

    /// Mark a result as failed.
    async fn fail_result(&self, id: &str) -> Result<()>;

    /// Get a queryable result (status = 'ready' only).
    /// Used by ResultsSchemaProvider for SQL queries over results.
    async fn get_queryable_result(&self, id: &str) -> Result<Option<QueryResult>>;
```

**Step 3: Verify it compiles (will fail due to missing impls)**

Run: `cargo build 2>&1 | head -50`
Expected: Errors about missing trait implementations

**Step 4: Commit**

```bash
git add src/catalog/manager.rs
git commit -m "feat(catalog): add status field and async persistence methods to trait"
```

---

## Task 3: Implement new catalog methods for SqliteCatalogManager

**Files:**
- Modify: `src/catalog/sqlite_manager.rs`

**Step 1: Update store_result to include status**

Find the existing `store_result` implementation and update the SQL:

```rust
    async fn store_result(&self, result: &QueryResult) -> Result<()> {
        sqlx::query(
            "INSERT INTO results (id, parquet_path, status, created_at)
             VALUES (?, ?, ?, ?)",
        )
        .bind(&result.id)
        .bind(&result.parquet_path)
        .bind(&result.status)
        .bind(result.created_at)
        .execute(self.backend.pool())
        .await?;
        Ok(())
    }
```

**Step 2: Update get_result query to include status**

```rust
    async fn get_result(&self, id: &str) -> Result<Option<QueryResult>> {
        let result = sqlx::query_as::<_, QueryResult>(
            "SELECT id, parquet_path, status, created_at FROM results WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(self.backend.pool())
        .await?;
        Ok(result)
    }
```

**Step 3: Add store_result_pending implementation**

```rust
    #[tracing::instrument(
        name = "catalog_store_result_pending",
        skip(self),
        fields(runtimedb.result_id = %id)
    )]
    async fn store_result_pending(&self, id: &str, created_at: DateTime<Utc>) -> Result<()> {
        sqlx::query(
            "INSERT INTO results (id, parquet_path, status, created_at)
             VALUES (?, NULL, 'processing', ?)",
        )
        .bind(id)
        .bind(created_at)
        .execute(self.backend.pool())
        .await?;
        Ok(())
    }
```

**Step 4: Add finalize_result implementation**

```rust
    #[tracing::instrument(
        name = "catalog_finalize_result",
        skip(self),
        fields(runtimedb.result_id = %id)
    )]
    async fn finalize_result(&self, id: &str, parquet_path: &str) -> Result<()> {
        sqlx::query(
            "UPDATE results SET parquet_path = ?, status = 'ready' WHERE id = ?",
        )
        .bind(parquet_path)
        .bind(id)
        .execute(self.backend.pool())
        .await?;
        Ok(())
    }
```

**Step 5: Add fail_result implementation**

```rust
    #[tracing::instrument(
        name = "catalog_fail_result",
        skip(self),
        fields(runtimedb.result_id = %id)
    )]
    async fn fail_result(&self, id: &str) -> Result<()> {
        sqlx::query(
            "UPDATE results SET status = 'failed' WHERE id = ?",
        )
        .bind(id)
        .execute(self.backend.pool())
        .await?;
        Ok(())
    }
```

**Step 6: Add get_queryable_result implementation**

```rust
    #[tracing::instrument(
        name = "catalog_get_queryable_result",
        skip(self),
        fields(runtimedb.result_id = %id)
    )]
    async fn get_queryable_result(&self, id: &str) -> Result<Option<QueryResult>> {
        let result = sqlx::query_as::<_, QueryResult>(
            "SELECT id, parquet_path, status, created_at FROM results WHERE id = ? AND status = 'ready'",
        )
        .bind(id)
        .fetch_optional(self.backend.pool())
        .await?;
        Ok(result)
    }
```

**Step 7: Update list_results to include status**

```rust
    async fn list_results(&self, limit: usize, offset: usize) -> Result<(Vec<QueryResult>, bool)> {
        // Fetch one extra to determine has_more
        let fetch_limit = limit + 1;
        let results = sqlx::query_as::<_, QueryResult>(
            "SELECT id, parquet_path, status, created_at FROM results ORDER BY created_at DESC LIMIT ? OFFSET ?",
        )
        .bind(fetch_limit as i64)
        .bind(offset as i64)
        .fetch_all(self.backend.pool())
        .await?;

        let has_more = results.len() > limit;
        let results = results.into_iter().take(limit).collect();
        Ok((results, has_more))
    }
```

**Step 8: Build and verify**

Run: `cargo build`
Expected: Build succeeds (postgres manager will fail, that's Task 4)

**Step 9: Commit**

```bash
git add src/catalog/sqlite_manager.rs
git commit -m "feat(catalog): implement async persistence methods for SQLite"
```

---

## Task 4: Implement new catalog methods for PostgresCatalogManager

**Files:**
- Modify: `src/catalog/postgres_manager.rs`

**Step 1: Update store_result to include status**

```rust
    async fn store_result(&self, result: &QueryResult) -> Result<()> {
        sqlx::query(
            "INSERT INTO results (id, parquet_path, status, created_at)
             VALUES ($1, $2, $3, $4)",
        )
        .bind(&result.id)
        .bind(&result.parquet_path)
        .bind(&result.status)
        .bind(result.created_at)
        .execute(self.backend.pool())
        .await?;
        Ok(())
    }
```

**Step 2: Update get_result query to include status**

```rust
    async fn get_result(&self, id: &str) -> Result<Option<QueryResult>> {
        let result = sqlx::query_as::<_, QueryResult>(
            "SELECT id, parquet_path, status, created_at FROM results WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(self.backend.pool())
        .await?;
        Ok(result)
    }
```

**Step 3: Add store_result_pending implementation**

```rust
    #[tracing::instrument(
        name = "catalog_store_result_pending",
        skip(self),
        fields(runtimedb.result_id = %id)
    )]
    async fn store_result_pending(&self, id: &str, created_at: DateTime<Utc>) -> Result<()> {
        sqlx::query(
            "INSERT INTO results (id, parquet_path, status, created_at)
             VALUES ($1, NULL, 'processing', $2)",
        )
        .bind(id)
        .bind(created_at)
        .execute(self.backend.pool())
        .await?;
        Ok(())
    }
```

**Step 4: Add finalize_result implementation**

```rust
    #[tracing::instrument(
        name = "catalog_finalize_result",
        skip(self),
        fields(runtimedb.result_id = %id)
    )]
    async fn finalize_result(&self, id: &str, parquet_path: &str) -> Result<()> {
        sqlx::query(
            "UPDATE results SET parquet_path = $1, status = 'ready' WHERE id = $2",
        )
        .bind(parquet_path)
        .bind(id)
        .execute(self.backend.pool())
        .await?;
        Ok(())
    }
```

**Step 5: Add fail_result implementation**

```rust
    #[tracing::instrument(
        name = "catalog_fail_result",
        skip(self),
        fields(runtimedb.result_id = %id)
    )]
    async fn fail_result(&self, id: &str) -> Result<()> {
        sqlx::query(
            "UPDATE results SET status = 'failed' WHERE id = $1",
        )
        .bind(id)
        .execute(self.backend.pool())
        .await?;
        Ok(())
    }
```

**Step 6: Add get_queryable_result implementation**

```rust
    #[tracing::instrument(
        name = "catalog_get_queryable_result",
        skip(self),
        fields(runtimedb.result_id = %id)
    )]
    async fn get_queryable_result(&self, id: &str) -> Result<Option<QueryResult>> {
        let result = sqlx::query_as::<_, QueryResult>(
            "SELECT id, parquet_path, status, created_at FROM results WHERE id = $1 AND status = 'ready'",
        )
        .bind(id)
        .fetch_optional(self.backend.pool())
        .await?;
        Ok(result)
    }
```

**Step 7: Update list_results to include status**

```rust
    async fn list_results(&self, limit: usize, offset: usize) -> Result<(Vec<QueryResult>, bool)> {
        let fetch_limit = limit + 1;
        let results = sqlx::query_as::<_, QueryResult>(
            "SELECT id, parquet_path, status, created_at FROM results ORDER BY created_at DESC LIMIT $1 OFFSET $2",
        )
        .bind(fetch_limit as i64)
        .bind(offset as i64)
        .fetch_all(self.backend.pool())
        .await?;

        let has_more = results.len() > limit;
        let results = results.into_iter().take(limit).collect();
        Ok((results, has_more))
    }
```

**Step 8: Build and verify**

Run: `cargo build`
Expected: Build succeeds

**Step 9: Commit**

```bash
git add src/catalog/postgres_manager.rs
git commit -m "feat(catalog): implement async persistence methods for PostgreSQL"
```

---

## Task 5: Update MockCatalog and CachingCatalogManager

**Files:**
- Modify: `src/catalog/mock_catalog.rs`
- Modify: `src/catalog/caching_manager.rs`

**Step 1: Add methods to MockCatalog**

Find the MockCatalog implementation and add the new methods (these are pass-through or no-ops for tests):

```rust
    async fn store_result_pending(&self, id: &str, created_at: DateTime<Utc>) -> Result<()> {
        // For mock, just store with processing status
        let result = QueryResult {
            id: id.to_string(),
            parquet_path: None,
            status: "processing".to_string(),
            created_at,
        };
        self.results.write().unwrap().insert(id.to_string(), result);
        Ok(())
    }

    async fn finalize_result(&self, id: &str, parquet_path: &str) -> Result<()> {
        if let Some(result) = self.results.write().unwrap().get_mut(id) {
            result.parquet_path = Some(parquet_path.to_string());
            result.status = "ready".to_string();
        }
        Ok(())
    }

    async fn fail_result(&self, id: &str) -> Result<()> {
        if let Some(result) = self.results.write().unwrap().get_mut(id) {
            result.status = "failed".to_string();
        }
        Ok(())
    }

    async fn get_queryable_result(&self, id: &str) -> Result<Option<QueryResult>> {
        let results = self.results.read().unwrap();
        Ok(results.get(id).filter(|r| r.status == "ready").cloned())
    }
```

**Step 2: Add methods to CachingCatalogManager (delegate to inner)**

```rust
    async fn store_result_pending(&self, id: &str, created_at: DateTime<Utc>) -> Result<()> {
        self.inner.store_result_pending(id, created_at).await
    }

    async fn finalize_result(&self, id: &str, parquet_path: &str) -> Result<()> {
        self.inner.finalize_result(id, parquet_path).await
    }

    async fn fail_result(&self, id: &str) -> Result<()> {
        self.inner.fail_result(id).await
    }

    async fn get_queryable_result(&self, id: &str) -> Result<Option<QueryResult>> {
        self.inner.get_queryable_result(id).await
    }
```

**Step 3: Build and verify**

Run: `cargo build`
Expected: Build succeeds

**Step 4: Commit**

```bash
git add src/catalog/mock_catalog.rs src/catalog/caching_manager.rs
git commit -m "feat(catalog): implement async persistence methods for mock and caching"
```

---

## Task 6: Update ResultsSchemaProvider to use get_queryable_result

**Files:**
- Modify: `src/datafusion/results_schema.rs:79-130`

**Step 1: Change get_result to get_queryable_result in table() method**

In the `table()` method, change line ~89:

```rust
        // Look up result by ID - only returns ready results
        let result = match self.catalog.get_queryable_result(name).await {
```

**Step 2: Change get_result to get_queryable_result in table_exist() method**

In the `table_exist()` method, change line ~128:

```rust
    fn table_exist(&self, name: &str) -> bool {
        // This is a sync trait method, so block_on is required here
        // Only returns true for ready results
        matches!(block_on(self.catalog.get_queryable_result(name)), Ok(Some(_)))
    }
```

**Step 3: Build and verify**

Run: `cargo build`
Expected: Build succeeds

**Step 4: Commit**

```bash
git add src/datafusion/results_schema.rs
git commit -m "refactor(datafusion): use get_queryable_result for SQL access"
```

---

## Task 7: Add execute_query_with_persistence to engine

**Files:**
- Modify: `src/engine.rs`

**Step 1: Add the new method after execute_query**

Add this method to the `RuntimeEngine` impl block (after the existing `execute_query` method around line 455):

```rust
    /// Execute a SQL query and persist results asynchronously.
    ///
    /// This method:
    /// 1. Executes the query
    /// 2. Generates a result ID
    /// 3. Inserts a catalog entry with status "processing"
    /// 4. Spawns a background task to write parquet and update status
    /// 5. Returns immediately with the result ID
    ///
    /// The result can be retrieved via GET /results/{id}. If status is "processing",
    /// the client should poll again later.
    #[tracing::instrument(
        name = "execute_query_with_persistence",
        skip(self, sql),
        fields(
            runtimedb.result_id = tracing::field::Empty,
        )
    )]
    pub async fn execute_query_with_persistence(&self, sql: &str) -> Result<QueryResponseWithId> {
        // Execute the query first
        let query_response = self.execute_query(sql).await?;

        // Generate result ID
        let result_id = crate::id::generate_result_id();
        let created_at = Utc::now();

        tracing::Span::current().record("runtimedb.result_id", &result_id);

        // Insert pending result into catalog
        self.catalog.store_result_pending(&result_id, created_at).await?;

        // Clone what we need for the background task
        let storage = Arc::clone(&self.storage);
        let catalog = Arc::clone(&self.catalog);
        let result_id_clone = result_id.clone();
        let schema = Arc::clone(&query_response.schema);
        let batches = query_response.results.clone();

        // Spawn background persistence task
        tokio::spawn(async move {
            let span = tracing::info_span!(
                "persist_result_async",
                runtimedb.result_id = %result_id_clone,
            );

            async {
                if let Err(e) = persist_result_background(
                    &result_id_clone,
                    &schema,
                    &batches,
                    storage,
                    catalog,
                ).await {
                    tracing::warn!(
                        result_id = %result_id_clone,
                        error = %e,
                        "Background result persistence failed"
                    );
                }
            }
            .instrument(span)
            .await
        });

        Ok(QueryResponseWithId {
            result_id,
            schema: query_response.schema,
            results: query_response.results,
            execution_time: query_response.execution_time,
        })
    }
```

**Step 2: Add QueryResponseWithId struct**

Add this struct near the existing `QueryResponse` struct (around line 57):

```rust
/// Result of a query execution with async persistence.
/// The result_id is returned immediately; persistence happens in background.
pub struct QueryResponseWithId {
    pub result_id: String,
    pub schema: Arc<Schema>,
    pub results: Vec<RecordBatch>,
    pub execution_time: Duration,
}
```

**Step 3: Add the background persistence function**

Add this helper function (can be at the end of the file or in a suitable location):

```rust
/// Background task to persist query results to parquet.
/// Updates catalog status to "ready" on success or "failed" on error.
async fn persist_result_background(
    result_id: &str,
    schema: &Arc<Schema>,
    batches: &[RecordBatch],
    storage: Arc<dyn StorageManager>,
    catalog: Arc<dyn CatalogManager>,
) -> Result<()> {
    // Prepare write location
    let handle = storage.prepare_cache_write(
        INTERNAL_CONNECTION_ID,
        "runtimedb_results",
        result_id,
    );

    // Write parquet file (sync I/O in blocking task to avoid blocking the runtime)
    let local_path = handle.local_path.clone();
    let schema_clone = Arc::clone(schema);
    let batches_clone: Vec<RecordBatch> = batches.to_vec();

    let write_result = tokio::task::spawn_blocking(move || {
        let mut writer: Box<dyn BatchWriter> =
            Box::new(StreamingParquetWriter::new(local_path));
        writer.init(&schema_clone)?;
        for batch in &batches_clone {
            writer.write_batch(batch)?;
        }
        writer.close()
    })
    .await
    .map_err(|e| anyhow::anyhow!("Join error: {}", e))?;

    if let Err(e) = write_result {
        // Mark as failed and return error
        let _ = catalog.fail_result(result_id).await;
        return Err(e);
    }

    // Finalize storage (uploads to S3 if needed)
    let dir_url = match storage.finalize_cache_write(&handle).await {
        Ok(url) => url,
        Err(e) => {
            let _ = catalog.fail_result(result_id).await;
            return Err(e);
        }
    };

    let file_url = format!("{}/data.parquet", dir_url);

    // Update catalog to ready
    catalog.finalize_result(result_id, &file_url).await?;

    Ok(())
}
```

**Step 4: Add necessary imports at top of engine.rs**

Make sure these are imported (some may already exist):

```rust
use crate::datafetch::native::StreamingParquetWriter;
use crate::datafetch::BatchWriter;
```

**Step 5: Build and verify**

Run: `cargo build`
Expected: Build succeeds

**Step 6: Commit**

```bash
git add src/engine.rs
git commit -m "feat(engine): add execute_query_with_persistence method"
```

---

## Task 8: Update query_handler to use new engine method

**Files:**
- Modify: `src/http/controllers/query_controller.rs`

**Step 1: Simplify query_handler**

Replace the handler implementation:

```rust
/// Handler for POST /query
#[tracing::instrument(
    name = "handler_query",
    skip(engine, request),
    fields(
        runtimedb.row_count = tracing::field::Empty,
        runtimedb.result_id = tracing::field::Empty,
    )
)]
pub async fn query_handler(
    State(engine): State<Arc<RuntimeEngine>>,
    Json(request): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, ApiError> {
    // Validate SQL is not empty
    if request.sql.trim().is_empty() {
        return Err(ApiError::bad_request("SQL query cannot be empty"));
    }

    // Execute query with async persistence
    let start = Instant::now();
    let result = engine.execute_query_with_persistence(&request.sql).await?;
    let execution_time_ms = start.elapsed().as_millis() as u64;

    let batches = &result.results;
    let schema = &result.schema;

    // Serialize results for HTTP response
    let (columns, nullable, rows) = serialize_batches(schema, batches)?;
    let row_count = rows.len();

    tracing::Span::current()
        .record("runtimedb.row_count", row_count)
        .record("runtimedb.result_id", &result.result_id);

    Ok(Json(QueryResponse {
        result_id: Some(result.result_id),
        columns,
        nullable,
        rows,
        row_count,
        execution_time_ms,
        warning: None,
    }))
}
```

**Step 2: Remove unused import**

Remove `use tracing::warn;` if no longer used.

**Step 3: Build and verify**

Run: `cargo build`
Expected: Build succeeds

**Step 4: Commit**

```bash
git add src/http/controllers/query_controller.rs
git commit -m "refactor(http): simplify query_handler to use async persistence"
```

---

## Task 9: Add status to GET /results/{id} response

**Files:**
- Modify: `src/http/models.rs` (add GetResultResponse)
- Modify: `src/http/controllers/results_controller.rs`

**Step 1: Add GetResultResponse model**

In `src/http/models.rs`, add after `QueryResponse`:

```rust
/// Response body for GET /results/{id}
/// Returns status and optionally the result data
#[derive(Debug, Serialize)]
pub struct GetResultResponse {
    pub result_id: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nullable: Option<Vec<bool>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows: Option<Vec<Vec<serde_json::Value>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_time_ms: Option<u64>,
}
```

**Step 2: Update get_result_handler**

In `src/http/controllers/results_controller.rs`:

```rust
use crate::http::models::{GetResultResponse, ListResultsResponse, ResultInfo};

/// Handler for GET /results/{id}
#[tracing::instrument(
    name = "handler_get_result",
    skip(engine),
    fields(runtimedb.result_id = %id)
)]
pub async fn get_result_handler(
    State(engine): State<Arc<RuntimeEngine>>,
    Path(id): Path<String>,
) -> Result<Json<GetResultResponse>, ApiError> {
    // First check the result status in catalog
    let result_meta = engine
        .get_result_metadata(&id)
        .await
        .map_err(|e| ApiError::internal_error(format!("Failed to lookup result: {}", e)))?
        .ok_or_else(|| ApiError::not_found(format!("Result '{}' not found", id)))?;

    // If not ready, return status only
    if result_meta.status != "ready" {
        return Ok(Json(GetResultResponse {
            result_id: id,
            status: result_meta.status,
            columns: None,
            nullable: None,
            rows: None,
            row_count: None,
            execution_time_ms: None,
        }));
    }

    // Status is ready, load the data
    let start = Instant::now();

    let (schema, batches) = engine
        .get_result(&id)
        .await
        .map_err(|e| ApiError::internal_error(format!("Failed to load result: {}", e)))?
        .ok_or_else(|| ApiError::internal_error("Result metadata exists but data not found"))?;

    let (columns, nullable, rows) = serialize_batches(&schema, &batches)?;
    let row_count = rows.len();
    let execution_time_ms = start.elapsed().as_millis() as u64;

    Ok(Json(GetResultResponse {
        result_id: id,
        status: "ready".to_string(),
        columns: Some(columns),
        nullable: Some(nullable),
        rows: Some(rows),
        row_count: Some(row_count),
        execution_time_ms: Some(execution_time_ms),
    }))
}
```

**Step 3: Add get_result_metadata to engine**

In `src/engine.rs`, add this method:

```rust
    /// Get result metadata (status, etc.) without loading parquet data.
    pub async fn get_result_metadata(&self, id: &str) -> Result<Option<crate::catalog::QueryResult>> {
        self.catalog.get_result(id).await
    }
```

**Step 4: Build and verify**

Run: `cargo build`
Expected: Build succeeds

**Step 5: Commit**

```bash
git add src/http/models.rs src/http/controllers/results_controller.rs src/engine.rs
git commit -m "feat(http): add status field to GET /results/{id} response"
```

---

## Task 10: Update list_results to include status

**Files:**
- Modify: `src/http/models.rs`
- Modify: `src/http/controllers/results_controller.rs`

**Step 1: Add status to ResultInfo**

In `src/http/models.rs`, update `ResultInfo`:

```rust
/// Summary of a persisted query result for listing
#[derive(Debug, Serialize)]
pub struct ResultInfo {
    pub id: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
}
```

**Step 2: Update list_results_handler**

In `src/http/controllers/results_controller.rs`, update the mapping:

```rust
    let results = results
        .into_iter()
        .map(|r| ResultInfo {
            id: r.id,
            status: r.status,
            created_at: r.created_at,
        })
        .collect();
```

**Step 3: Build and verify**

Run: `cargo build`
Expected: Build succeeds

**Step 4: Commit**

```bash
git add src/http/models.rs src/http/controllers/results_controller.rs
git commit -m "feat(http): add status to list results response"
```

---

## Task 11: Update tests for async persistence

**Files:**
- Modify: `tests/result_persistence_tests.rs`

**Step 1: Add helper to wait for result to be ready**

Add this helper function at the top of the test file:

```rust
/// Wait for a result to transition from "processing" to "ready" or "failed".
/// Returns the final status.
async fn wait_for_result_ready(app: &AppServer, result_id: &str, timeout_ms: u64) -> Result<String> {
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_millis(timeout_ms);

    loop {
        let get_uri = PATH_RESULT.replace("{id}", result_id);
        let response = app
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(&get_uri)
                    .body(Body::empty())?,
            )
            .await?;

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
        let json: serde_json::Value = serde_json::from_slice(&body)?;

        let status = json["status"].as_str().unwrap_or("unknown");
        if status != "processing" {
            return Ok(status.to_string());
        }

        if start.elapsed() > timeout {
            anyhow::bail!("Timeout waiting for result to be ready");
        }

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}
```

**Step 2: Update test_query_returns_result_id**

The query now returns immediately with result_id, but we need to wait for ready status:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn test_query_returns_result_id() -> Result<()> {
    let (app, _temp) = setup_test().await?;

    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(json!({"sql": "SELECT 1 as num"}).to_string()))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    // Should have result_id immediately
    assert!(json["result_id"].is_string());
    let result_id = json["result_id"].as_str().unwrap();
    assert!(!result_id.is_empty());

    // Should have expected data
    assert_eq!(json["row_count"], 1);
    assert_eq!(json["rows"][0][0], 1);

    // Wait for result to be ready
    let status = wait_for_result_ready(&app, result_id, 5000).await?;
    assert_eq!(status, "ready");

    Ok(())
}
```

**Step 3: Add test for processing status**

```rust
#[tokio::test(flavor = "multi_thread")]
async fn test_get_result_returns_processing_status() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let cache_dir = temp_dir.path().join("cache");

    // Create storage that delays finalization
    let slow_storage = Arc::new(SlowStorage::new(&cache_dir, 500)); // 500ms delay

    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .storage(slow_storage)
        .secret_key(generate_test_secret_key())
        .build()
        .await?;

    let app = AppServer::new(engine);

    // Execute query
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(json!({"sql": "SELECT 1 as num"}).to_string()))?,
        )
        .await?;

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;
    let result_id = json["result_id"].as_str().unwrap();

    // Immediately check result - should be processing
    let get_uri = PATH_RESULT.replace("{id}", result_id);
    let get_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&get_uri)
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(get_response.status(), StatusCode::OK);
    let get_body = axum::body::to_bytes(get_response.into_body(), usize::MAX).await?;
    let get_json: serde_json::Value = serde_json::from_slice(&get_body)?;

    // Should be processing (or ready if fast enough)
    let status = get_json["status"].as_str().unwrap();
    assert!(status == "processing" || status == "ready");

    // rows should be absent if processing
    if status == "processing" {
        assert!(get_json.get("rows").is_none() || get_json["rows"].is_null());
    }

    Ok(())
}
```

**Step 4: Add SlowStorage wrapper for testing**

Add this test helper struct:

```rust
/// Storage wrapper that adds artificial delay to finalization for testing async behavior.
#[derive(Debug)]
struct SlowStorage {
    inner: FilesystemStorage,
    delay_ms: u64,
}

impl SlowStorage {
    fn new(base_dir: &std::path::Path, delay_ms: u64) -> Self {
        Self {
            inner: FilesystemStorage::new(base_dir.to_str().expect("valid UTF-8 path")),
            delay_ms,
        }
    }
}

#[async_trait]
impl StorageManager for SlowStorage {
    // ... delegate all methods to inner, but add delay to finalize_cache_write:

    async fn finalize_cache_write(&self, handle: &CacheWriteHandle) -> Result<String> {
        tokio::time::sleep(std::time::Duration::from_millis(self.delay_ms)).await;
        self.inner.finalize_cache_write(handle).await
    }

    // ... rest delegate directly to self.inner
}
```

**Step 5: Update failure tests**

Update `test_persistence_failure_returns_null_result_id_with_warning` - with async persistence, failures are reflected in status, not in the query response:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn test_persistence_failure_results_in_failed_status() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let cache_dir = temp_dir.path().join("cache");

    let failing_storage = Arc::new(FailingStorage::new(&cache_dir));
    failing_storage.set_fail_finalize(true);

    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .storage(failing_storage)
        .secret_key(generate_test_secret_key())
        .build()
        .await?;

    let app = AppServer::new(engine);

    // Query should succeed with result_id
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(json!({"sql": "SELECT 1 as num"}).to_string()))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    // Should have result_id (persistence is async, so ID is returned immediately)
    assert!(json["result_id"].is_string());
    let result_id = json["result_id"].as_str().unwrap();

    // Should have query results
    assert_eq!(json["row_count"], 1);

    // Wait and check that status becomes failed
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let get_uri = PATH_RESULT.replace("{id}", result_id);
    let get_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&get_uri)
                .body(Body::empty())?,
        )
        .await?;

    let get_body = axum::body::to_bytes(get_response.into_body(), usize::MAX).await?;
    let get_json: serde_json::Value = serde_json::from_slice(&get_body)?;

    assert_eq!(get_json["status"], "failed");

    Ok(())
}
```

**Step 6: Update SQL query tests**

Update `test_query_nonexistent_result_via_sql` and similar tests to ensure processing results are not queryable:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn test_processing_result_not_queryable_via_sql() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let cache_dir = temp_dir.path().join("cache");

    // Use slow storage to keep result in processing state
    let slow_storage = Arc::new(SlowStorage::new(&cache_dir, 2000));

    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .storage(slow_storage)
        .secret_key(generate_test_secret_key())
        .build()
        .await?;

    let app = AppServer::new(engine);

    // Create result
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(json!({"sql": "SELECT 1 as x"}).to_string()))?,
        )
        .await?;

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;
    let result_id = json["result_id"].as_str().unwrap();

    // Immediately try to query via SQL - should fail (table not found)
    let sql_query = format!("SELECT * FROM runtimedb.results.\"{}\"", result_id);
    let query_response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(json!({"sql": sql_query}).to_string()))?,
        )
        .await?;

    // Should return error (table not found while processing)
    assert!(query_response.status().is_client_error() || query_response.status().is_server_error());

    Ok(())
}
```

**Step 7: Run tests**

Run: `cargo test --test result_persistence_tests`
Expected: Tests pass (some may need adjustment based on actual behavior)

**Step 8: Commit**

```bash
git add tests/result_persistence_tests.rs
git commit -m "test: update result persistence tests for async behavior"
```

---

## Task 12: Clean up old persist_result method

**Files:**
- Modify: `src/engine.rs`

**Step 1: Remove or deprecate old persist_result method**

The old `persist_result` method is no longer called by the query handler. Either remove it or mark it as deprecated:

```rust
    /// Persist query results to storage.
    ///
    /// DEPRECATED: Use execute_query_with_persistence instead.
    /// This method is kept for backwards compatibility but may be removed in a future version.
    #[deprecated(note = "Use execute_query_with_persistence instead")]
    pub async fn persist_result(
        &self,
        schema: &Arc<Schema>,
        batches: &[RecordBatch],
    ) -> Result<String> {
        // ... existing implementation
    }
```

**Step 2: Build and verify**

Run: `cargo build`
Expected: Build succeeds (may have deprecation warnings, which is fine)

**Step 3: Run full test suite**

Run: `cargo test`
Expected: All tests pass

**Step 4: Commit**

```bash
git add src/engine.rs
git commit -m "refactor(engine): deprecate old persist_result method"
```

---

## Summary

The implementation follows these key changes:

1. **Migration v4** adds `status` column to results table
2. **Catalog trait** gains new methods for async lifecycle
3. **Engine** gets `execute_query_with_persistence` that spawns background task
4. **Query handler** becomes simpler - just calls one method
5. **GET /results/{id}** returns status, handling processing/failed states
6. **ResultsSchemaProvider** only resolves ready results for SQL queries
7. **Tests** updated to handle async behavior with polling/waiting
