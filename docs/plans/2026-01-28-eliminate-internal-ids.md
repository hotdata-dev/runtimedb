# Eliminate Connection Internal IDs

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace all connection `i32` internal IDs with `String` external IDs (`conn_*` nanoid) as the single identifier. No code path may reference the connection's internal autoincrement ID. Table internal IDs (`TableInfo.id: i32`) are out of scope.

**Architecture:** DB schema migrates `tables.connection_id` from `INTEGER` FK to `TEXT` FK referencing `connections.external_id`. `ConnectionInfo` drops its `id: i32` field, renames `external_id` to `id`. All trait methods switch from `i32`/`name` to `&str` external_id. Backend SQL uses JOINs/subqueries to resolve external_id→internal row when needed.

**Tech Stack:** Rust, sqlx, async-trait, DataFusion

**Scope:** Connections only. `TableInfo.id: i32`, `PendingDeletion.id: i32`, and `update_table_sync(table_id: i32)` remain unchanged.

---

### Task 1: Add v3 database migration

**Files:**
- Create: `migrations/sqlite/v3.sql`
- Create: `migrations/postgres/v3.sql`
- Modify: `src/catalog/migrations.rs` (add v3 to migration list)

**Step 1: Write SQLite v3 migration**

Migrate `tables.connection_id` from INTEGER (FK to `connections.id`) to TEXT (FK to `connections.external_id`):

```sql
-- Migrate tables.connection_id from internal integer ID to external text ID
-- SQLite doesn't support ALTER COLUMN, so we recreate the table

CREATE TABLE tables_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    connection_id TEXT NOT NULL,
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    parquet_path TEXT,
    last_sync TIMESTAMP,
    arrow_schema_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (connection_id) REFERENCES connections(external_id),
    UNIQUE (connection_id, schema_name, table_name)
);

INSERT INTO tables_new (id, connection_id, schema_name, table_name, parquet_path, last_sync, arrow_schema_json, created_at)
SELECT t.id, c.external_id, t.schema_name, t.table_name, t.parquet_path, t.last_sync, t.arrow_schema_json, t.created_at
FROM tables t
JOIN connections c ON t.connection_id = c.id;

DROP TABLE tables;
ALTER TABLE tables_new RENAME TO tables;
```

**Step 2: Write Postgres v3 migration**

```sql
-- Migrate tables.connection_id from internal integer ID to external text ID
ALTER TABLE tables DROP CONSTRAINT tables_connection_id_fkey;
ALTER TABLE tables DROP CONSTRAINT tables_connection_id_schema_name_table_name_key;

ALTER TABLE tables ALTER COLUMN connection_id TYPE TEXT
USING (SELECT external_id FROM connections WHERE connections.id = tables.connection_id);

ALTER TABLE tables ADD CONSTRAINT tables_connection_id_fkey
    FOREIGN KEY (connection_id) REFERENCES connections(external_id);
ALTER TABLE tables ADD CONSTRAINT tables_connection_id_schema_name_table_name_key
    UNIQUE (connection_id, schema_name, table_name);
```

**Step 3: Update migrations.rs to include v3**

Add the v3 migration to both `SQLITE_MIGRATIONS` and `POSTGRES_MIGRATIONS` arrays.

**Step 4: Commit**

```
feat(catalog): add v3 migration for text connection IDs
```

---

### Task 2: Update `ConnectionInfo` and `TableInfo` structs

**Files:**
- Modify: `src/catalog/manager.rs`

**Step 1: Update `ConnectionInfo`**

Remove `id: i32`, rename `external_id` to `id`:

```rust
pub struct ConnectionInfo {
    pub id: String,           // the conn_* nanoid (was external_id)
    pub name: String,
    pub source_type: String,
    pub config_json: String,
    pub secret_id: Option<String>,
}
```

**Step 2: Update `TableInfo`**

Change `connection_id` from `i32` to `String`:

```rust
pub struct TableInfo {
    pub id: i32,              // table's own internal ID (stays)
    pub connection_id: String, // was i32, now external_id string
    pub schema_name: String,
    pub table_name: String,
    pub parquet_path: Option<String>,
    pub last_sync: Option<String>,
    pub arrow_schema_json: Option<String>,
}
```

**Step 3: Commit**

```
refactor(catalog): update structs for string connection ID
```

---

### Task 3: Update `CatalogManager` trait

**Files:**
- Modify: `src/catalog/manager.rs`

**Step 1: Update trait methods**

Remove:
- `get_connection_by_external_id(external_id: &str)`
- `get_connection_by_id(id: i32)`

Change `get_connection(name: &str)` to `get_connection(id: &str)` (now looks up by external_id).

Add:
- `get_connection_by_name(name: &str)` (needed for conflict checking and engine's name-based lookup)

Change parameter types:
- `add_table(connection_id: &str, ...)` (was `i32`)
- `list_tables(connection_id: Option<&str>)` (was `Option<i32>`)
- `get_table(connection_id: &str, ...)` (was `i32`)
- `clear_table_cache_metadata(connection_id: &str, ...)` (was `i32`)
- `clear_connection_cache_metadata(connection_id: &str)` (was `name: &str`)
- `delete_connection(connection_id: &str)` (was `name: &str`)

**Step 2: Compile check (expect errors everywhere)**

Run: `cargo check 2>&1 | head -50`

**Step 3: Commit**

```
refactor(catalog): update trait to use external_id only
```

---

### Task 4: Update `CatalogBackend`

**Files:**
- Modify: `src/catalog/backend.rs`

**Step 1: Update all connection queries**

`list_connections`: SELECT `external_id as id` instead of `id, external_id`.

`get_connection(id: &str)`: Query by `external_id = $1`, return `external_id as id`.

Add `get_connection_by_name(name: &str)`: Query by `name = $1`, return `external_id as id`.

Remove `get_connection_by_external_id` and `get_connection_by_id`.

**Step 2: Update table queries — now `connection_id` is TEXT in DB**

Since the v3 migration changed `tables.connection_id` to TEXT storing the external_id directly, queries become simpler — no JOINs needed:

`add_table(connection_id: &str, ...)`: INSERT with the string directly.

`list_tables(connection_id: Option<&str>)`: WHERE `connection_id = $1` (already text).

`get_table(connection_id: &str, ...)`: WHERE `connection_id = $1`.

`clear_table_cache_metadata(connection_id: &str, ...)`: WHERE `connection_id = $1`.

**Step 3: Update `clear_connection_cache_metadata(connection_id: &str)`**

Was using `name` to look up the connection, then using the internal id. Now just: `WHERE connection_id = $1` directly on the tables table.

**Step 4: Update `delete_connection(connection_id: &str)`**

Delete tables: `WHERE connection_id = $1` (text match).
Delete connection: `WHERE external_id = $1`.

**Step 5: Compile check**

Run: `cargo check 2>&1 | head -50`

**Step 6: Commit**

```
refactor(catalog): update backend for text connection IDs
```

---

### Task 5: Update SQLite and Postgres manager implementations

**Files:**
- Modify: `src/catalog/sqlite_manager.rs`
- Modify: `src/catalog/postgres_manager.rs`

**Step 1: Update all method signatures and delegates**

Both are thin delegates to `CatalogBackend`. Update signatures to match new trait. Remove `get_connection_by_external_id`, `get_connection_by_id`. Add `get_connection_by_name`.

**Step 2: Compile check**

Run: `cargo check 2>&1 | head -50`

**Step 3: Commit**

```
refactor(catalog): update sqlite and postgres managers
```

---

### Task 6: Update `MockCatalog`

**Files:**
- Modify: `src/catalog/mock_catalog.rs`

**Step 1: Update mock**

Change `add_table(&self, connection_id: i32, ...)` to `add_table(&self, connection_id: &str, ...)`.

Update internal storage to use `String` connection IDs.

Update all trait method implementations. Remove `get_connection_by_external_id`, `get_connection_by_id`. Add `get_connection_by_name`.

**Step 2: Compile check**

Run: `cargo check 2>&1 | head -50`

**Step 3: Commit**

```
refactor(catalog): update mock catalog
```

---

### Task 7: Update `StorageManager` trait and implementations

**Files:**
- Modify: `src/storage/mod.rs`
- Modify: `src/storage/filesystem.rs`
- Modify: `src/storage/s3.rs`

**Step 1: Update `CacheWriteHandle`**

```rust
pub struct CacheWriteHandle {
    pub local_path: std::path::PathBuf,
    pub version: String,
    pub connection_id: String,  // was i32
    pub schema: String,
    pub table: String,
}
```

**Step 2: Update `StorageManager` trait**

```rust
fn cache_url(&self, connection_id: &str, schema: &str, table: &str) -> String;
fn cache_prefix(&self, connection_id: &str) -> String;
fn prepare_cache_write(&self, connection_id: &str, schema: &str, table: &str) -> CacheWriteHandle;
```

**Step 3: Update `FilesystemStorage`**

Change `connection_id: i32` to `connection_id: &str` in all methods. Path building changes from `.join(connection_id.to_string())` to `.join(connection_id)`.

**Step 4: Update `S3Storage`**

Same changes. Format strings change from `"{connection_id}"` (i32) to `"{connection_id}"` (&str) — same format but different type.

**Step 5: Compile check**

Run: `cargo check 2>&1 | head -50`

**Step 6: Commit**

```
refactor(storage): use string connection IDs
```

---

### Task 8: Update `DataFetchError` and `FetchOrchestrator`

**Files:**
- Modify: `src/datafetch/error.rs`
- Modify: `src/datafetch/orchestrator.rs`

**Step 1: Update `DataFetchError::TableNotFound`**

```rust
TableNotFound {
    connection_id: String,  // was i32
    schema: String,
    table: String,
},
```

**Step 2: Update orchestrator**

`cache_table`: `connection_id: i32` → `connection_id: &str`
`refresh_table`: `connection_id: i32` → `connection_id: &str`
`refresh_table_catalog_update`: `connection_id: i32` → `connection_id: &str`

All internal calls to storage/catalog methods already expect `&str` from previous tasks.

**Step 3: Compile check**

Run: `cargo check 2>&1 | head -50`

**Step 4: Commit**

```
refactor(datafetch): use string connection IDs
```

---

### Task 9: Update DataFusion providers

**Files:**
- Modify: `src/datafusion/catalog_provider.rs`
- Modify: `src/datafusion/schema_provider.rs`
- Modify: `src/datafusion/lazy_table_provider.rs`
- Modify: `src/datafusion/information_schema/tables.rs`
- Modify: `src/datafusion/information_schema/columns.rs`

**Step 1: Update `RuntimeCatalogProvider`**

Change `connection_id: i32` to `connection_id: String`. Update constructor and usage.

**Step 2: Update `RuntimeSchemaProvider`**

Change `connection_id: i32` to `connection_id: String`. Pass `&self.connection_id` to catalog/orchestrator calls.

**Step 3: Update `LazyTableProvider`**

Change `connection_id: i32` to `connection_id: String`. Pass `&self.connection_id` to `orchestrator.cache_table()` and `catalog.get_table()`.

**Step 4: Update information_schema providers**

`tables.rs` and `columns.rs`: Change `HashMap<i32, String>` to `HashMap<String, String>`. The `c.id` is now `String`, so `(c.id, c.name)` still works.

**Step 5: Compile check**

Run: `cargo check 2>&1 | head -50`

**Step 6: Commit**

```
refactor(datafusion): use string connection IDs
```

---

### Task 10: Update `engine.rs`

**Files:**
- Modify: `src/engine.rs`

**Step 1: Replace `get_connection_by_external_id` calls with `get_connection`**

All places that call `self.catalog.get_connection_by_external_id(id)` become `self.catalog.get_connection(id)`.

**Step 2: Replace `conn.id` (formerly i32) usages**

`conn.id` is now the String external_id. Everywhere that extracted `conn.id` (i32) to pass to catalog/storage/orchestrator — now pass `&conn.id` (String) or the original `connection_id: &str` parameter directly.

**Step 3: Replace name-based calls**

`self.catalog.clear_connection_cache_metadata(conn.name)` → `self.catalog.clear_connection_cache_metadata(connection_id)` (or `&conn.id`)
`self.catalog.delete_connection(conn.name)` → `self.catalog.delete_connection(connection_id)` (or `&conn.id`)

**Step 4: Update `delete_connection_files`**

Change `connection_id: i32` to `connection_id: &str`.

**Step 5: Compile check**

Run: `cargo check 2>&1 | head -50`

**Step 6: Commit**

```
refactor(engine): use external_id only for connections
```

---

### Task 11: Update HTTP controllers

**Files:**
- Modify: `src/http/controllers/connections_controller.rs`
- Modify: `src/http/controllers/information_schema_controller.rs`

**Step 1: Simplify connections_controller**

Where it maps `conn.external_id` to response `id` — now `conn.id` IS the external_id. Simplify serialization.

**Step 2: Update information_schema_controller**

Change `HashMap<i32, String>` to `HashMap<String, String>`. Update `.get(&t.connection_id)` which now takes `&String`.

**Step 3: Compile check**

Run: `cargo check 2>&1 | head -50`

**Step 4: Commit**

```
refactor(http): simplify connection ID handling
```

---

### Task 12: Update all tests

**Files:**
- Modify: `src/datafetch/orchestrator.rs` (test module)
- Modify: `src/storage/filesystem.rs` (test module)
- Modify: Any other test files

**Step 1: Update orchestrator tests**

Change `connection_id: 1` to `"conn_test123"`. Update `MockStorage` in tests. Update URL pattern assertions from `"/1/test/orders/"` to `"/conn_test123/test/orders/"`.

**Step 2: Update filesystem storage tests**

Change `prepare_cache_write(1, ...)` to `prepare_cache_write("conn_test", ...)`. Update assertions like `contains("/42/")` to `contains("/conn_test/")`.

**Step 3: Update mock catalog test usages**

`catalog.add_table(1, "test", "orders")` → `catalog.add_table("conn_test123", "test", "orders")`.

**Step 4: Run full test suite**

Run: `cargo test`
Expected: All pass.

**Step 5: Commit**

```
test: update tests for string connection IDs
```

---

### Task 13: Final verification

**Step 1: Grep for any remaining i32 connection ID references**

Run: `rg "connection_id.*i32|connection_id:\s*i32" src/ --glob '!**/pending_deletion*'`
Expected: Zero matches. (`TableInfo.id: i32` and `PendingDeletion.id: i32` are fine — they're table/deletion IDs, not connection IDs.)

**Step 2: Grep for removed methods**

Run: `rg "get_connection_by_external_id|get_connection_by_id\b" src/`
Expected: Zero matches.

**Step 3: Full build and test**

Run: `cargo build && cargo test`
Expected: Clean.

**Step 4: Commit any cleanup**

```
chore: final cleanup
```
