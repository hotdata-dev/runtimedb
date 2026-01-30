# Async Result Persistence Design

## Overview

Make result persistence async and move it entirely into the engine, keeping the query handler lightweight.

## Key Changes

### 1. Catalog Schema

Add `status` field to result metadata:
- `processing` - persistence in progress
- `ready` - persistence complete, result available
- `failed` - persistence failed

### 2. Query Execution Flow

1. Query handler calls `engine.execute_query_with_persistence(sql)`
2. Engine executes query, generates result_id
3. Engine inserts catalog row with `status = processing`
4. Engine spawns async persist task
5. Engine returns immediately with result_id
6. Handler returns response to client
7. Async task writes parquet, updates catalog to `ready` (or `failed` on error)

### 3. Result Queryability

- Results with `status = ready` are queryable via SQL (e.g., `SELECT * FROM runtimedb.results.{id}`)
- Results with `status = processing` or `failed` are **not** registered as tables
- Attempting to query a non-ready result returns "table not found"

### 4. GET `/results/{id}` Behavior

- If `status = processing` → return `{"status": "processing"}`
- If `status = ready` → return result data with `{"status": "ready", "rows": [...]}`
- If `status = failed` → return `{"status": "failed"}`

## API Changes

### Query Handler

Simplified to just execute and return:

```rust
let response = engine.execute_query_with_persistence(&sql).await?;
return Ok(Json { result_id: response.result_id, rows, ... });
```

### Engine

New method:
- `execute_query_with_persistence(&self, sql: &str) -> Result<QueryResponse>`

Existing `execute_query()` stays unchanged for internal use.

### QueryResponse

Add `result_id: Option<String>` field.

### GET `/results/{id}` Response

Add `status` field. When status is `processing` or `failed`, rows are omitted.

## Async Persistence Task

Spawned via `tokio::spawn`, holds:
- `Arc<StorageManager>`
- `Arc<CatalogManager>`
- result_id
- `Vec<RecordBatch>` to persist

Task responsibilities:
1. Write batches to parquet
2. Finalize storage (upload if S3)
3. Update catalog: `status = ready` with row_count, path
4. On error: update catalog with `status = failed`, log warning

No retries—failures are marked and logged.

## Catalog Changes

### New/Modified Methods

1. `store_result_pending(result_id, schema, ...)` - Insert with `status = processing`
2. `finalize_result(result_id, path, row_count, ...)` - Set `status = ready`
3. `fail_result(result_id)` - Set `status = failed`
4. `get_result(result_id)` - Returns status along with metadata
5. `get_queryable_result(result_id)` - Only returns if `status = ready`

### Migration

Existing results migrated to `status = ready`.

## Files to Change

1. `src/catalog/manager.rs` - Status field, new methods
2. `src/engine.rs` - New `execute_query_with_persistence()`, async task
3. `src/http/controllers/query_controller.rs` - Simplify handler
4. `src/http/controllers/results_controller.rs` - Return status, handle states
5. Catalog schema/migration - Add `status` column
6. `tests/result_persistence_tests.rs` - Test async flow, statuses, queryability
