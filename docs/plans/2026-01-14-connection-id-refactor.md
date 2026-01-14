# Connection ID Refactor Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace connection names with stable nanoid-based external IDs in all API endpoints.

**Architecture:** Add `external_id` field to connections (format: `con` + 22-char nanoid = 25 chars total). Update all HTTP routes to use `{connection_id}` instead of `{name}`. Keep `name` as a display field only.

**Tech Stack:** Rust, axum, SQLite/Postgres, nanoid crate

---

### Task 1: Add nanoid Dependency

**Files:**
- Modify: `Cargo.toml`

**Step 1: Add nanoid crate to dependencies**

In `Cargo.toml`, add to `[dependencies]`:

```toml
nanoid = "0.4"
```

**Step 2: Verify it compiles**

Run: `cargo check`
Expected: Compiles successfully

**Step 3: Commit**

```bash
git add Cargo.toml Cargo.lock
git commit -m "chore: add nanoid dependency"
```

---

### Task 2: Add external_id to ConnectionInfo Struct

**Files:**
- Modify: `src/catalog/manager.rs`

**Step 1: Update ConnectionInfo struct**

In `src/catalog/manager.rs`, update the struct (around line 21-27):

```rust
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ConnectionInfo {
    pub id: i32,
    pub external_id: String,
    pub name: String,
    pub source_type: String,
    pub config_json: String,
}
```

**Step 2: Add helper function to generate external IDs**

Add after the struct definition:

```rust
/// Generates a connection external ID: "con" + 22-char nanoid (25 chars total)
pub fn generate_connection_id() -> String {
    format!("con{}", nanoid::nanoid!(22))
}
```

**Step 3: Verify it compiles**

Run: `cargo check`
Expected: Compilation errors (expected - database queries don't include external_id yet)

**Step 4: Commit**

```bash
git add src/catalog/manager.rs
git commit -m "feat(catalog): add external_id field to ConnectionInfo"
```

---

### Task 3: Update Database Schema

**Files:**
- Modify: `src/catalog/sqlite_manager.rs`

**Step 1: Update schema creation**

In `src/catalog/sqlite_manager.rs`, update the connections table schema (around line 63-76):

```rust
CREATE TABLE IF NOT EXISTS connections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    external_id TEXT UNIQUE NOT NULL,
    name TEXT UNIQUE NOT NULL,
    source_type TEXT NOT NULL,
    config_json TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```

**Step 2: Add migration for existing databases**

After the CREATE TABLE statement, add a migration check:

```rust
// Migration: add external_id column if it doesn't exist
let has_external_id: bool = sqlx::query_scalar(
    "SELECT COUNT(*) > 0 FROM pragma_table_info('connections') WHERE name = 'external_id'"
)
.fetch_one(&pool)
.await?;

if !has_external_id {
    sqlx::query("ALTER TABLE connections ADD COLUMN external_id TEXT")
        .execute(&pool)
        .await?;

    // Backfill existing connections with generated IDs
    let connections: Vec<(i32,)> = sqlx::query_as("SELECT id FROM connections WHERE external_id IS NULL")
        .fetch_all(&pool)
        .await?;

    for (conn_id,) in connections {
        let external_id = crate::catalog::manager::generate_connection_id();
        sqlx::query("UPDATE connections SET external_id = ? WHERE id = ?")
            .bind(&external_id)
            .bind(conn_id)
            .execute(&pool)
            .await?;
    }

    // Now make it NOT NULL (SQLite doesn't support this directly, but new inserts will require it)
}
```

**Step 3: Verify it compiles**

Run: `cargo check`
Expected: Compiles (may have warnings)

**Step 4: Commit**

```bash
git add src/catalog/sqlite_manager.rs
git commit -m "feat(catalog): add external_id column with migration"
```

---

### Task 4: Update Backend Queries

**Files:**
- Modify: `src/catalog/backend.rs`

**Step 1: Update add_connection to generate and store external_id**

Find `add_connection` method and update:

```rust
pub async fn add_connection(
    &self,
    name: &str,
    source_type: &str,
    config_json: &str,
) -> Result<i32> {
    let external_id = crate::catalog::manager::generate_connection_id();
    let sql = format!(
        "INSERT INTO connections (external_id, name, source_type, config_json) VALUES ({}, {}, {}, {})",
        DB::bind_param(1),
        DB::bind_param(2),
        DB::bind_param(3),
        DB::bind_param(4)
    );

    let result = query(&sql)
        .bind(&external_id)
        .bind(name)
        .bind(source_type)
        .bind(config_json)
        .execute(&self.pool)
        .await?;

    Ok(result.last_insert_id() as i32)
}
```

**Step 2: Update get_connection query to include external_id**

```rust
pub async fn get_connection(&self, name: &str) -> Result<Option<ConnectionInfo>> {
    let sql = format!(
        "SELECT id, external_id, name, source_type, config_json FROM connections WHERE name = {}",
        DB::bind_param(1)
    );

    query_as::<DB, ConnectionInfo>(&sql)
        .bind(name)
        .fetch_optional(&self.pool)
        .await
        .map_err(Into::into)
}
```

**Step 3: Add get_connection_by_external_id method**

```rust
pub async fn get_connection_by_external_id(&self, external_id: &str) -> Result<Option<ConnectionInfo>> {
    let sql = format!(
        "SELECT id, external_id, name, source_type, config_json FROM connections WHERE external_id = {}",
        DB::bind_param(1)
    );

    query_as::<DB, ConnectionInfo>(&sql)
        .bind(external_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(Into::into)
}
```

**Step 4: Update list_connections query**

```rust
pub async fn list_connections(&self) -> Result<Vec<ConnectionInfo>> {
    let sql = "SELECT id, external_id, name, source_type, config_json FROM connections";

    query_as::<DB, ConnectionInfo>(sql)
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
}
```

**Step 5: Verify it compiles**

Run: `cargo check`
Expected: Compiles successfully

**Step 6: Commit**

```bash
git add src/catalog/backend.rs
git commit -m "feat(catalog): update queries for external_id"
```

---

### Task 5: Update CatalogManager Trait

**Files:**
- Modify: `src/catalog/manager.rs`

**Step 1: Add get_connection_by_external_id to trait**

In the `CatalogManager` trait, add:

```rust
async fn get_connection_by_external_id(&self, external_id: &str) -> Result<Option<ConnectionInfo>>;
```

**Step 2: Verify it compiles**

Run: `cargo check`
Expected: Compilation errors (implementations don't have this method yet)

**Step 3: Commit**

```bash
git add src/catalog/manager.rs
git commit -m "feat(catalog): add get_connection_by_external_id to trait"
```

---

### Task 6: Implement Trait Method in SqliteCatalogManager

**Files:**
- Modify: `src/catalog/sqlite_manager.rs`

**Step 1: Implement get_connection_by_external_id**

Add the implementation in the `impl CatalogManager for SqliteCatalogManager` block:

```rust
async fn get_connection_by_external_id(&self, external_id: &str) -> Result<Option<ConnectionInfo>> {
    self.backend.get_connection_by_external_id(external_id).await
}
```

**Step 2: Verify it compiles**

Run: `cargo check`
Expected: Compiles successfully

**Step 3: Commit**

```bash
git add src/catalog/sqlite_manager.rs
git commit -m "feat(catalog): implement get_connection_by_external_id for sqlite"
```

---

### Task 7: Update HTTP Models

**Files:**
- Modify: `src/http/models.rs`

**Step 1: Update ConnectionInfo response model**

```rust
#[derive(Debug, Serialize)]
pub struct ConnectionInfo {
    pub id: String,  // Changed from i32 to String (external_id)
    pub name: String,
    pub source_type: String,
}
```

**Step 2: Update GetConnectionResponse**

```rust
#[derive(Debug, Serialize)]
pub struct GetConnectionResponse {
    pub id: String,  // Changed from i32 to String (external_id)
    pub name: String,
    pub source_type: String,
    pub table_count: usize,
    pub synced_table_count: usize,
}
```

**Step 3: Update CreateConnectionResponse**

```rust
#[derive(Debug, Serialize)]
pub struct CreateConnectionResponse {
    pub id: String,  // Add this field
    pub name: String,
    pub source_type: String,
    pub tables_discovered: usize,
    pub discovery_status: DiscoveryStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub discovery_error: Option<String>,
}
```

**Step 4: Verify it compiles**

Run: `cargo check`
Expected: Compilation errors (handlers build these structs differently)

**Step 5: Commit**

```bash
git add src/http/models.rs
git commit -m "feat(http): update models to use external_id"
```

---

### Task 8: Update Route Definitions

**Files:**
- Modify: `src/http/app_server.rs`

**Step 1: Update path constants**

```rust
pub const PATH_CONNECTION: &str = "/connections/{connection_id}";
pub const PATH_CONNECTION_DISCOVER: &str = "/connections/{connection_id}/discover";
pub const PATH_CONNECTION_CACHE: &str = "/connections/{connection_id}/cache";
pub const PATH_TABLE_CACHE: &str = "/connections/{connection_id}/tables/{schema}/{table}/cache";
```

**Step 2: Verify it compiles**

Run: `cargo check`
Expected: Compiles (path strings are just constants)

**Step 3: Commit**

```bash
git add src/http/app_server.rs
git commit -m "refactor(http): rename path params to connection_id"
```

---

### Task 9: Update Handlers - Part 1 (Create and List)

**Files:**
- Modify: `src/http/handlers.rs`

**Step 1: Update create_connection_handler response**

Find where `CreateConnectionResponse` is built and update to include `id`:

```rust
Ok((
    StatusCode::CREATED,
    Json(CreateConnectionResponse {
        id: conn.external_id.clone(),
        name: request.name.clone(),
        source_type: request.source_type.clone(),
        tables_discovered,
        discovery_status,
        discovery_error,
    }),
))
```

Note: You'll need to fetch the connection after creation to get the external_id:

```rust
let conn = engine
    .catalog()
    .get_connection(&request.name)
    .await?
    .ok_or_else(|| ApiError::internal("Failed to retrieve created connection"))?;
```

**Step 2: Update list_connections_handler**

Update the mapping to use external_id:

```rust
let connections = conns
    .into_iter()
    .map(|c| models::ConnectionInfo {
        id: c.external_id,
        name: c.name,
        source_type: c.source_type,
    })
    .collect();
```

**Step 3: Verify it compiles**

Run: `cargo check`
Expected: Compilation errors (other handlers still wrong)

**Step 4: Commit**

```bash
git add src/http/handlers.rs
git commit -m "feat(http): update create and list handlers for external_id"
```

---

### Task 10: Update Handlers - Part 2 (Get, Delete, Discover)

**Files:**
- Modify: `src/http/handlers.rs`

**Step 1: Update get_connection_handler**

Change parameter extraction and lookup:

```rust
pub async fn get_connection_handler(
    State(engine): State<Arc<RuntimeEngine>>,
    Path(connection_id): Path<String>,
) -> Result<Json<GetConnectionResponse>, ApiError> {
    let conn = engine
        .catalog()
        .get_connection_by_external_id(&connection_id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("Connection '{}' not found", connection_id)))?;

    let tables = engine.catalog().list_tables_for_connection(conn.id).await?;
    let synced_count = tables.iter().filter(|t| t.is_synced).count();

    Ok(Json(GetConnectionResponse {
        id: conn.external_id,
        name: conn.name,
        source_type: conn.source_type,
        table_count: tables.len(),
        synced_table_count: synced_count,
    }))
}
```

**Step 2: Update delete_connection_handler**

```rust
pub async fn delete_connection_handler(
    State(engine): State<Arc<RuntimeEngine>>,
    Path(connection_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let conn = engine
        .catalog()
        .get_connection_by_external_id(&connection_id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("Connection '{}' not found", connection_id)))?;

    engine.remove_connection(&conn.name).await?;
    Ok(StatusCode::NO_CONTENT)
}
```

**Step 3: Update discover_connection_handler**

```rust
pub async fn discover_connection_handler(
    State(engine): State<Arc<RuntimeEngine>>,
    Path(connection_id): Path<String>,
) -> Result<Json<DiscoverConnectionResponse>, ApiError> {
    let conn = engine
        .catalog()
        .get_connection_by_external_id(&connection_id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("Connection '{}' not found", connection_id)))?;

    let tables_discovered = engine.discover_connection(&conn.name).await?;
    Ok(Json(DiscoverConnectionResponse { tables_discovered }))
}
```

**Step 4: Verify it compiles**

Run: `cargo check`
Expected: Compilation errors (cache handlers still wrong)

**Step 5: Commit**

```bash
git add src/http/handlers.rs
git commit -m "feat(http): update get/delete/discover handlers for external_id"
```

---

### Task 11: Update Handlers - Part 3 (Cache Handlers)

**Files:**
- Modify: `src/http/handlers.rs`

**Step 1: Update purge_connection_cache_handler**

```rust
pub async fn purge_connection_cache_handler(
    State(engine): State<Arc<RuntimeEngine>>,
    Path(connection_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let conn = engine
        .catalog()
        .get_connection_by_external_id(&connection_id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("Connection '{}' not found", connection_id)))?;

    engine.purge_connection(&conn.name).await?;
    Ok(StatusCode::NO_CONTENT)
}
```

**Step 2: Update TableCachePath struct**

```rust
#[derive(Deserialize)]
pub struct TableCachePath {
    connection_id: String,
    schema: String,
    table: String,
}
```

**Step 3: Update purge_table_cache_handler**

```rust
pub async fn purge_table_cache_handler(
    State(engine): State<Arc<RuntimeEngine>>,
    Path(params): Path<TableCachePath>,
) -> Result<StatusCode, ApiError> {
    let conn = engine
        .catalog()
        .get_connection_by_external_id(&params.connection_id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("Connection '{}' not found", params.connection_id)))?;

    engine
        .purge_table(&conn.name, &params.schema, &params.table)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
```

**Step 4: Verify it compiles**

Run: `cargo check`
Expected: Compiles successfully

**Step 5: Commit**

```bash
git add src/http/handlers.rs
git commit -m "feat(http): update cache handlers for external_id"
```

---

### Task 12: Update Tests

**Files:**
- Modify: `tests/` (any integration tests)

**Step 1: Find and update tests**

Search for tests that use connection endpoints:

Run: `rg "connections/" tests/`

Update any tests that:
- Use `{name}` in URL paths to use `{connection_id}`
- Assert on response `id` field (now a string, not i32)

**Step 2: Run tests**

Run: `cargo test`
Expected: All tests pass

**Step 3: Commit**

```bash
git add tests/
git commit -m "test: update integration tests for connection_id"
```

---

### Task 13: Final Verification

**Step 1: Run full test suite**

Run: `cargo test`
Expected: All tests pass

**Step 2: Run clippy**

Run: `cargo clippy`
Expected: No warnings

**Step 3: Test manually (optional)**

Start the server and test the new endpoints:

```bash
# Create a connection
curl -X POST http://localhost:3000/connections \
  -H "Content-Type: application/json" \
  -d '{"name": "test", "source_type": "postgres", "config": {...}}'

# Response should include "id": "conXXXXXXXXXXXXXXXXXXXXXX"

# Get connection by ID
curl http://localhost:3000/connections/conXXXXXXXXXXXXXXXXXXXXXX
```

**Step 4: Final commit (if any cleanup needed)**

```bash
git add -A
git commit -m "chore: cleanup connection_id refactor"
```

---

## Summary of Changes

| File | Change |
|------|--------|
| `Cargo.toml` | Add nanoid dependency |
| `src/catalog/manager.rs` | Add external_id field, generate_connection_id() |
| `src/catalog/sqlite_manager.rs` | Schema migration, new trait impl |
| `src/catalog/backend.rs` | Update queries, add get_by_external_id |
| `src/http/app_server.rs` | Rename path params |
| `src/http/handlers.rs` | Lookup by external_id |
| `src/http/models.rs` | id field is now String |
