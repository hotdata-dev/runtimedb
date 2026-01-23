# Dataset Upload Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement direct dataset upload (CSV/JSON/Parquet) with two-phase creation flow.

**Architecture:** New `uploads` and `datasets` tables in catalog, new HTTP endpoints, `DatasetsProvider` for DataFusion integration. Uploads land in staging storage, get converted to Parquet on dataset creation.

**Tech Stack:** Rust, Axum, DataFusion, Arrow, SQLx, existing StorageManager abstraction

---

## Task 1: Add Resource ID Prefixes

**Files:**
- Modify: `src/id.rs`

**Step 1: Add new resource ID variants**

Add `Upload` and `Dataset` variants to the `define_resource_ids!` macro:

```rust
define_resource_ids! {
    Connection => "conn",
    Result => "rslt",
    Secret => "secr",
    Upload => "upld",
    Dataset => "data",
}
```

**Step 2: Add helper functions**

```rust
/// Generate an upload ID (prefix: "upld").
pub fn generate_upload_id() -> String {
    generate_id(ResourceId::Upload)
}

/// Generate a dataset ID (prefix: "data").
pub fn generate_dataset_id() -> String {
    generate_id(ResourceId::Dataset)
}
```

**Step 3: Add tests**

```rust
#[test]
fn test_upload_id_format() {
    let id = generate_upload_id();
    assert_eq!(id.len(), 30);
    assert!(id.starts_with("upld"));
    assert!(id.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()));
}

#[test]
fn test_dataset_id_format() {
    let id = generate_dataset_id();
    assert_eq!(id.len(), 30);
    assert!(id.starts_with("data"));
    assert!(id.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()));
}
```

**Step 4: Run tests**

Run: `cargo test id::tests`
Expected: All tests pass

**Step 5: Commit**

```bash
git add src/id.rs
git commit -m "feat(id): add upload and dataset resource ID prefixes"
```

---

## Task 2: Add Database Migrations

**Files:**
- Create: `migrations/sqlite/v2.sql`
- Create: `migrations/postgres/v2.sql`

**Step 1: Create SQLite migration**

Create `migrations/sqlite/v2.sql`:

```sql
-- Uploads table for pending file uploads
CREATE TABLE uploads (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'pending',
    storage_url TEXT NOT NULL,
    content_type TEXT,
    content_encoding TEXT,
    size_bytes INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    consumed_at TIMESTAMP
);

CREATE INDEX idx_uploads_status ON uploads(status);

-- Datasets table for user-curated data
CREATE TABLE datasets (
    id TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    schema_name TEXT NOT NULL DEFAULT 'default',
    table_name TEXT NOT NULL,
    parquet_url TEXT NOT NULL,
    arrow_schema_json TEXT NOT NULL,
    source_type TEXT NOT NULL,
    source_config TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(schema_name, table_name)
);

CREATE INDEX idx_datasets_table_name ON datasets(table_name);
```

**Step 2: Create Postgres migration**

Create `migrations/postgres/v2.sql`:

```sql
-- Uploads table for pending file uploads
CREATE TABLE uploads (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'pending',
    storage_url TEXT NOT NULL,
    content_type TEXT,
    content_encoding TEXT,
    size_bytes BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    consumed_at TIMESTAMPTZ
);

CREATE INDEX idx_uploads_status ON uploads(status);

-- Datasets table for user-curated data
CREATE TABLE datasets (
    id TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    schema_name TEXT NOT NULL DEFAULT 'default',
    table_name TEXT NOT NULL,
    parquet_url TEXT NOT NULL,
    arrow_schema_json TEXT NOT NULL,
    source_type TEXT NOT NULL,
    source_config TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(schema_name, table_name)
);

CREATE INDEX idx_datasets_table_name ON datasets(table_name);
```

**Step 3: Verify build compiles (migrations are hashed at build time)**

Run: `cargo build`
Expected: Build succeeds

**Step 4: Commit**

```bash
git add migrations/
git commit -m "feat(catalog): add uploads and datasets tables migration"
```

---

## Task 3: Add Catalog Types for Uploads and Datasets

**Files:**
- Modify: `src/catalog/manager.rs`

**Step 1: Add UploadInfo struct**

Add after `QueryResult` struct:

```rust
/// A pending file upload.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct UploadInfo {
    pub id: String,
    pub status: String,
    pub storage_url: String,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub size_bytes: i64,
    pub created_at: DateTime<Utc>,
    pub consumed_at: Option<DateTime<Utc>>,
}
```

**Step 2: Add DatasetInfo struct**

```rust
/// A user-curated dataset.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct DatasetInfo {
    pub id: String,
    pub label: String,
    pub schema_name: String,
    pub table_name: String,
    pub parquet_url: String,
    pub arrow_schema_json: String,
    pub source_type: String,
    pub source_config: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
```

**Step 3: Add trait methods for uploads**

Add to `CatalogManager` trait:

```rust
// Upload management methods

/// Create a new upload record.
async fn create_upload(&self, upload: &UploadInfo) -> Result<()>;

/// Get an upload by ID.
async fn get_upload(&self, id: &str) -> Result<Option<UploadInfo>>;

/// List uploads, optionally filtered by status.
async fn list_uploads(&self, status: Option<&str>) -> Result<Vec<UploadInfo>>;

/// Mark an upload as consumed.
async fn consume_upload(&self, id: &str) -> Result<bool>;
```

**Step 4: Add trait methods for datasets**

```rust
// Dataset management methods

/// Create a new dataset record.
async fn create_dataset(&self, dataset: &DatasetInfo) -> Result<()>;

/// Get a dataset by ID.
async fn get_dataset(&self, id: &str) -> Result<Option<DatasetInfo>>;

/// Get a dataset by table_name (within default schema).
async fn get_dataset_by_table_name(&self, table_name: &str) -> Result<Option<DatasetInfo>>;

/// List all datasets.
async fn list_datasets(&self) -> Result<Vec<DatasetInfo>>;

/// Update dataset metadata (label, table_name).
async fn update_dataset(&self, id: &str, label: &str, table_name: &str) -> Result<bool>;

/// Delete a dataset by ID. Returns the deleted dataset info for cleanup.
async fn delete_dataset(&self, id: &str) -> Result<Option<DatasetInfo>>;
```

**Step 5: Commit**

```bash
git add src/catalog/manager.rs
git commit -m "feat(catalog): add upload and dataset types and trait methods"
```

---

## Task 4: Implement Catalog Backend Methods

**Files:**
- Modify: `src/catalog/backend.rs`

**Step 1: Implement create_upload**

Add to the generic backend implementation:

```rust
async fn create_upload(&self, upload: &UploadInfo) -> Result<()> {
    let query = format!(
        "INSERT INTO uploads (id, status, storage_url, content_type, content_encoding, size_bytes, created_at, consumed_at)
         VALUES ({p}1, {p}2, {p}3, {p}4, {p}5, {p}6, {p}7, {p}8)",
        p = D::param_prefix()
    );

    sqlx::query(&query)
        .bind(&upload.id)
        .bind(&upload.status)
        .bind(&upload.storage_url)
        .bind(&upload.content_type)
        .bind(&upload.content_encoding)
        .bind(upload.size_bytes)
        .bind(upload.created_at)
        .bind(upload.consumed_at)
        .execute(self.pool())
        .await?;

    Ok(())
}
```

**Step 2: Implement get_upload**

```rust
async fn get_upload(&self, id: &str) -> Result<Option<UploadInfo>> {
    let query = format!(
        "SELECT id, status, storage_url, content_type, content_encoding, size_bytes, created_at, consumed_at
         FROM uploads WHERE id = {p}1",
        p = D::param_prefix()
    );

    let upload = sqlx::query_as::<_, UploadInfo>(&query)
        .bind(id)
        .fetch_optional(self.pool())
        .await?;

    Ok(upload)
}
```

**Step 3: Implement list_uploads**

```rust
async fn list_uploads(&self, status: Option<&str>) -> Result<Vec<UploadInfo>> {
    let uploads = if let Some(status) = status {
        let query = format!(
            "SELECT id, status, storage_url, content_type, content_encoding, size_bytes, created_at, consumed_at
             FROM uploads WHERE status = {p}1 ORDER BY created_at DESC",
            p = D::param_prefix()
        );
        sqlx::query_as::<_, UploadInfo>(&query)
            .bind(status)
            .fetch_all(self.pool())
            .await?
    } else {
        sqlx::query_as::<_, UploadInfo>(
            "SELECT id, status, storage_url, content_type, content_encoding, size_bytes, created_at, consumed_at
             FROM uploads ORDER BY created_at DESC"
        )
        .fetch_all(self.pool())
        .await?
    };

    Ok(uploads)
}
```

**Step 4: Implement consume_upload**

```rust
async fn consume_upload(&self, id: &str) -> Result<bool> {
    let query = format!(
        "UPDATE uploads SET status = 'consumed', consumed_at = {p}1 WHERE id = {p}2 AND status = 'pending'",
        p = D::param_prefix()
    );

    let result = sqlx::query(&query)
        .bind(Utc::now())
        .bind(id)
        .execute(self.pool())
        .await?;

    Ok(result.rows_affected() > 0)
}
```

**Step 5: Implement create_dataset**

```rust
async fn create_dataset(&self, dataset: &DatasetInfo) -> Result<()> {
    let query = format!(
        "INSERT INTO datasets (id, label, schema_name, table_name, parquet_url, arrow_schema_json, source_type, source_config, created_at, updated_at)
         VALUES ({p}1, {p}2, {p}3, {p}4, {p}5, {p}6, {p}7, {p}8, {p}9, {p}10)",
        p = D::param_prefix()
    );

    sqlx::query(&query)
        .bind(&dataset.id)
        .bind(&dataset.label)
        .bind(&dataset.schema_name)
        .bind(&dataset.table_name)
        .bind(&dataset.parquet_url)
        .bind(&dataset.arrow_schema_json)
        .bind(&dataset.source_type)
        .bind(&dataset.source_config)
        .bind(dataset.created_at)
        .bind(dataset.updated_at)
        .execute(self.pool())
        .await?;

    Ok(())
}
```

**Step 6: Implement remaining dataset methods**

```rust
async fn get_dataset(&self, id: &str) -> Result<Option<DatasetInfo>> {
    let query = format!(
        "SELECT id, label, schema_name, table_name, parquet_url, arrow_schema_json, source_type, source_config, created_at, updated_at
         FROM datasets WHERE id = {p}1",
        p = D::param_prefix()
    );

    Ok(sqlx::query_as::<_, DatasetInfo>(&query)
        .bind(id)
        .fetch_optional(self.pool())
        .await?)
}

async fn get_dataset_by_table_name(&self, table_name: &str) -> Result<Option<DatasetInfo>> {
    let query = format!(
        "SELECT id, label, schema_name, table_name, parquet_url, arrow_schema_json, source_type, source_config, created_at, updated_at
         FROM datasets WHERE schema_name = 'default' AND table_name = {p}1",
        p = D::param_prefix()
    );

    Ok(sqlx::query_as::<_, DatasetInfo>(&query)
        .bind(table_name)
        .fetch_optional(self.pool())
        .await?)
}

async fn list_datasets(&self) -> Result<Vec<DatasetInfo>> {
    Ok(sqlx::query_as::<_, DatasetInfo>(
        "SELECT id, label, schema_name, table_name, parquet_url, arrow_schema_json, source_type, source_config, created_at, updated_at
         FROM datasets ORDER BY created_at DESC"
    )
    .fetch_all(self.pool())
    .await?)
}

async fn update_dataset(&self, id: &str, label: &str, table_name: &str) -> Result<bool> {
    let query = format!(
        "UPDATE datasets SET label = {p}1, table_name = {p}2, updated_at = {p}3 WHERE id = {p}4",
        p = D::param_prefix()
    );

    let result = sqlx::query(&query)
        .bind(label)
        .bind(table_name)
        .bind(Utc::now())
        .bind(id)
        .execute(self.pool())
        .await?;

    Ok(result.rows_affected() > 0)
}

async fn delete_dataset(&self, id: &str) -> Result<Option<DatasetInfo>> {
    // First get the dataset info for cleanup
    let dataset = self.get_dataset(id).await?;

    if dataset.is_some() {
        let query = format!(
            "DELETE FROM datasets WHERE id = {p}1",
            p = D::param_prefix()
        );
        sqlx::query(&query)
            .bind(id)
            .execute(self.pool())
            .await?;
    }

    Ok(dataset)
}
```

**Step 7: Run build to verify**

Run: `cargo build`
Expected: Build succeeds

**Step 8: Commit**

```bash
git add src/catalog/backend.rs
git commit -m "feat(catalog): implement upload and dataset backend methods"
```

---

## Task 5: Add HTTP Models for Uploads and Datasets

**Files:**
- Modify: `src/http/models.rs`

**Step 1: Add upload response models**

```rust
/// Response body for POST /v1/files
#[derive(Debug, Serialize)]
pub struct CreateUploadResponse {
    pub id: String,
    pub status: String,
    pub size_bytes: i64,
}

/// Single upload metadata for API responses
#[derive(Debug, Serialize)]
pub struct UploadMetadata {
    pub id: String,
    pub status: String,
    pub size_bytes: i64,
    pub content_type: Option<String>,
    pub created_at: DateTime<Utc>,
    pub consumed_at: Option<DateTime<Utc>>,
}

/// Response body for GET /v1/files
#[derive(Debug, Serialize)]
pub struct ListUploadsResponse {
    pub uploads: Vec<UploadMetadata>,
}
```

**Step 2: Add dataset request models**

```rust
/// Source configuration for dataset creation from upload
#[derive(Debug, Deserialize)]
pub struct UploadSource {
    pub upload_id: String,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub options: Option<serde_json::Value>,
}

/// Source configuration for inline dataset creation
#[derive(Debug, Deserialize)]
pub struct InlineSource {
    pub format: String,
    pub content: String,
}

/// Source for dataset creation (union type)
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum DatasetSource {
    Upload { upload_id: String, #[serde(default)] format: Option<String>, #[serde(default)] options: Option<serde_json::Value> },
    Inline { inline: InlineSource },
}

/// Schema override for a column
#[derive(Debug, Deserialize)]
pub struct ColumnOverride {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
}

/// Schema configuration for dataset creation
#[derive(Debug, Deserialize)]
pub struct SchemaConfig {
    #[serde(default)]
    pub columns: Vec<ColumnOverride>,
}

/// Request body for POST /v1/datasets
#[derive(Debug, Deserialize)]
pub struct CreateDatasetRequest {
    pub label: String,
    #[serde(default)]
    pub table_name: Option<String>,
    pub source: DatasetSource,
    #[serde(default)]
    pub schema: Option<SchemaConfig>,
}

/// Request body for PUT /v1/datasets/{id}
#[derive(Debug, Deserialize)]
pub struct UpdateDatasetRequest {
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub table_name: Option<String>,
}
```

**Step 3: Add dataset response models**

```rust
/// Response body for POST /v1/datasets
#[derive(Debug, Serialize)]
pub struct CreateDatasetResponse {
    pub id: String,
    pub label: String,
    pub table_name: String,
    pub status: String,
}

/// Single dataset metadata for list API
#[derive(Debug, Serialize)]
pub struct DatasetMetadata {
    pub id: String,
    pub label: String,
    pub table_name: String,
    pub source_type: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Response body for GET /v1/datasets
#[derive(Debug, Serialize)]
pub struct ListDatasetsResponse {
    pub datasets: Vec<DatasetMetadata>,
}

/// Response body for GET /v1/datasets/{id}
#[derive(Debug, Serialize)]
pub struct GetDatasetResponse {
    pub id: String,
    pub label: String,
    pub table_name: String,
    pub parquet_url: String,
    pub schema: serde_json::Value,
    pub source_type: String,
    pub source_config: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
```

**Step 4: Commit**

```bash
git add src/http/models.rs
git commit -m "feat(http): add upload and dataset request/response models"
```

---

## Task 6: Add table_name Validation Utility

**Files:**
- Create: `src/datasets/mod.rs`
- Create: `src/datasets/validation.rs`
- Modify: `src/lib.rs`

**Step 1: Create datasets module**

Create `src/datasets/mod.rs`:

```rust
//! Dataset management and utilities.

pub mod validation;

pub use validation::*;
```

**Step 2: Create validation module**

Create `src/datasets/validation.rs`:

```rust
//! Validation utilities for dataset table names.

use std::collections::HashSet;
use once_cell::sync::Lazy;

/// Maximum length for a table name.
pub const MAX_TABLE_NAME_LENGTH: usize = 128;

/// SQL reserved words that cannot be used as table names.
static RESERVED_WORDS: Lazy<HashSet<&'static str>> = Lazy::new(|| {
    [
        "select", "from", "where", "insert", "update", "delete", "create", "drop",
        "alter", "table", "index", "view", "and", "or", "not", "null", "true",
        "false", "in", "is", "like", "between", "join", "on", "as", "order",
        "by", "group", "having", "limit", "offset", "union", "all", "distinct",
        "case", "when", "then", "else", "end", "exists", "any", "some",
    ]
    .into_iter()
    .collect()
});

/// Error type for table name validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableNameError {
    Empty,
    TooLong(usize),
    InvalidFirstChar(char),
    InvalidChar(char),
    ReservedWord(String),
}

impl std::fmt::Display for TableNameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(f, "table name cannot be empty"),
            Self::TooLong(len) => write!(f, "table name exceeds maximum length of {} (got {})", MAX_TABLE_NAME_LENGTH, len),
            Self::InvalidFirstChar(c) => write!(f, "table name must start with a letter or underscore, got '{}'", c),
            Self::InvalidChar(c) => write!(f, "table name contains invalid character '{}'", c),
            Self::ReservedWord(word) => write!(f, "'{}' is a SQL reserved word and cannot be used as a table name", word),
        }
    }
}

impl std::error::Error for TableNameError {}

/// Validate a table name.
pub fn validate_table_name(name: &str) -> Result<(), TableNameError> {
    if name.is_empty() {
        return Err(TableNameError::Empty);
    }

    if name.len() > MAX_TABLE_NAME_LENGTH {
        return Err(TableNameError::TooLong(name.len()));
    }

    let mut chars = name.chars();

    // First character must be letter or underscore
    if let Some(first) = chars.next() {
        if !first.is_ascii_alphabetic() && first != '_' {
            return Err(TableNameError::InvalidFirstChar(first));
        }
    }

    // Remaining characters must be alphanumeric or underscore
    for c in chars {
        if !c.is_ascii_alphanumeric() && c != '_' {
            return Err(TableNameError::InvalidChar(c));
        }
    }

    // Check for reserved words
    let lower = name.to_lowercase();
    if RESERVED_WORDS.contains(lower.as_str()) {
        return Err(TableNameError::ReservedWord(name.to_string()));
    }

    Ok(())
}

/// Generate a table name from a label.
pub fn table_name_from_label(label: &str) -> String {
    let mut result = String::with_capacity(label.len());

    for c in label.chars() {
        if c.is_ascii_alphanumeric() {
            result.push(c.to_ascii_lowercase());
        } else if c.is_whitespace() || c == '-' {
            result.push('_');
        }
        // Skip other characters
    }

    // Ensure starts with letter or underscore
    if result.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(true) {
        result = format!("_{}", result);
    }

    // Truncate if needed
    if result.len() > MAX_TABLE_NAME_LENGTH {
        result.truncate(MAX_TABLE_NAME_LENGTH);
    }

    // If empty after processing, use a default
    if result.is_empty() {
        result = "_unnamed".to_string();
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_table_names() {
        assert!(validate_table_name("users").is_ok());
        assert!(validate_table_name("_private").is_ok());
        assert!(validate_table_name("table_123").is_ok());
        assert!(validate_table_name("CamelCase").is_ok());
    }

    #[test]
    fn test_invalid_first_char() {
        assert!(matches!(validate_table_name("123abc"), Err(TableNameError::InvalidFirstChar('1'))));
        assert!(matches!(validate_table_name("-test"), Err(TableNameError::InvalidFirstChar('-'))));
    }

    #[test]
    fn test_invalid_chars() {
        assert!(matches!(validate_table_name("table-name"), Err(TableNameError::InvalidChar('-'))));
        assert!(matches!(validate_table_name("table name"), Err(TableNameError::InvalidChar(' '))));
    }

    #[test]
    fn test_reserved_words() {
        assert!(matches!(validate_table_name("select"), Err(TableNameError::ReservedWord(_))));
        assert!(matches!(validate_table_name("FROM"), Err(TableNameError::ReservedWord(_))));
    }

    #[test]
    fn test_table_name_from_label() {
        assert_eq!(table_name_from_label("Q1 Sales Report"), "q1_sales_report");
        assert_eq!(table_name_from_label("123 Numbers"), "_123_numbers");
        assert_eq!(table_name_from_label("Hello-World"), "hello_world");
        assert_eq!(table_name_from_label(""), "_unnamed");
    }
}
```

**Step 3: Add module to lib.rs**

Add to `src/lib.rs`:

```rust
pub mod datasets;
```

**Step 4: Run tests**

Run: `cargo test datasets::validation::tests`
Expected: All tests pass

**Step 5: Commit**

```bash
git add src/datasets/ src/lib.rs
git commit -m "feat(datasets): add table_name validation and generation utilities"
```

---

## Task 7: Extend StorageManager for Upload Paths

**Files:**
- Modify: `src/storage/mod.rs`
- Modify: `src/storage/filesystem.rs`
- Modify: `src/storage/s3.rs`

**Step 1: Add upload path methods to trait**

Add to `StorageManager` trait in `src/storage/mod.rs`:

```rust
/// Get the URL for storing an upload.
fn upload_url(&self, upload_id: &str) -> String;

/// Get the URL for storing a dataset.
fn dataset_url(&self, dataset_id: &str, version: &str) -> String;

/// Prepare an upload write (returns local path for writing).
fn prepare_upload_write(&self, upload_id: &str) -> std::path::PathBuf;

/// Finalize an upload write (upload to remote if needed).
async fn finalize_upload_write(&self, upload_id: &str) -> Result<String>;

/// Prepare a dataset write (similar to cache write but for datasets).
fn prepare_dataset_write(&self, dataset_id: &str) -> DatasetWriteHandle;

/// Finalize dataset write and return URL.
async fn finalize_dataset_write(&self, handle: &DatasetWriteHandle) -> Result<String>;
```

**Step 2: Add DatasetWriteHandle struct**

```rust
/// Handle for a pending dataset write operation.
#[derive(Debug, Clone)]
pub struct DatasetWriteHandle {
    /// Local path where parquet file should be written
    pub local_path: std::path::PathBuf,
    /// Unique version identifier for this write
    pub version: String,
    /// Dataset ID
    pub dataset_id: String,
}
```

**Step 3: Implement in FilesystemStorage**

Add to `src/storage/filesystem.rs`:

```rust
fn upload_url(&self, upload_id: &str) -> String {
    format!("file://{}/uploads/{}/raw", self.base_path.display(), upload_id)
}

fn dataset_url(&self, dataset_id: &str, version: &str) -> String {
    format!("file://{}/datasets/{}/{}/data.parquet", self.base_path.display(), dataset_id, version)
}

fn prepare_upload_write(&self, upload_id: &str) -> std::path::PathBuf {
    let path = self.base_path.join("uploads").join(upload_id);
    std::fs::create_dir_all(&path).ok();
    path.join("raw")
}

async fn finalize_upload_write(&self, upload_id: &str) -> Result<String> {
    Ok(self.upload_url(upload_id))
}

fn prepare_dataset_write(&self, dataset_id: &str) -> DatasetWriteHandle {
    let version = nanoid::nanoid!(10, &crate::storage::filesystem::VERSION_ALPHABET);
    let path = self.base_path.join("datasets").join(dataset_id).join(&version);
    std::fs::create_dir_all(&path).ok();

    DatasetWriteHandle {
        local_path: path.join("data.parquet"),
        version,
        dataset_id: dataset_id.to_string(),
    }
}

async fn finalize_dataset_write(&self, handle: &DatasetWriteHandle) -> Result<String> {
    Ok(self.dataset_url(&handle.dataset_id, &handle.version))
}
```

**Step 4: Implement in S3Storage**

Add to `src/storage/s3.rs` (similar pattern to filesystem but with S3 upload on finalize).

**Step 5: Run build**

Run: `cargo build`
Expected: Build succeeds

**Step 6: Commit**

```bash
git add src/storage/
git commit -m "feat(storage): add upload and dataset path methods"
```

---

## Task 8: Add Upload HTTP Handlers

**Files:**
- Modify: `src/http/handlers.rs`
- Modify: `src/http/app_server.rs`

**Step 1: Add upload file handler**

Add to `src/http/handlers.rs`:

```rust
use axum::body::Bytes;
use axum::http::HeaderMap;

/// Maximum upload size (2GB)
const MAX_UPLOAD_SIZE: u64 = 2 * 1024 * 1024 * 1024;

/// Handler for POST /v1/files
pub async fn upload_file_handler(
    State(engine): State<Arc<RuntimeEngine>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, Json<CreateUploadResponse>), ApiError> {
    let size_bytes = body.len() as i64;

    // Validate size
    if size_bytes as u64 > MAX_UPLOAD_SIZE {
        return Err(ApiError::bad_request(format!(
            "Upload exceeds maximum size of {} bytes",
            MAX_UPLOAD_SIZE
        )));
    }

    // Extract content type and encoding hints
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let content_encoding = headers
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // Generate upload ID and write file
    let upload_id = crate::generate_upload_id();
    let storage_url = engine.store_upload(&upload_id, &body).await?;

    // Create catalog entry
    let upload = UploadInfo {
        id: upload_id.clone(),
        status: "pending".to_string(),
        storage_url,
        content_type,
        content_encoding,
        size_bytes,
        created_at: Utc::now(),
        consumed_at: None,
    };

    engine.catalog().create_upload(&upload).await?;

    Ok((
        StatusCode::CREATED,
        Json(CreateUploadResponse {
            id: upload_id,
            status: "pending".to_string(),
            size_bytes,
        }),
    ))
}

/// Handler for GET /v1/files
pub async fn list_uploads_handler(
    State(engine): State<Arc<RuntimeEngine>>,
) -> Result<Json<ListUploadsResponse>, ApiError> {
    let uploads = engine.catalog().list_uploads(Some("pending")).await?;

    let uploads = uploads
        .into_iter()
        .map(|u| UploadMetadata {
            id: u.id,
            status: u.status,
            size_bytes: u.size_bytes,
            content_type: u.content_type,
            created_at: u.created_at,
            consumed_at: u.consumed_at,
        })
        .collect();

    Ok(Json(ListUploadsResponse { uploads }))
}
```

**Step 2: Add routes to app_server.rs**

Add route constants:

```rust
pub const PATH_FILES: &str = "/v1/files";
```

Add routes:

```rust
.route(PATH_FILES, post(upload_file_handler).get(list_uploads_handler))
```

**Step 3: Add store_upload method to RuntimeEngine**

This will be implemented in Task 9.

**Step 4: Commit**

```bash
git add src/http/handlers.rs src/http/app_server.rs
git commit -m "feat(http): add upload file endpoints"
```

---

## Task 9: Add Engine Methods for Uploads and Datasets

**Files:**
- Modify: `src/engine.rs`

**Step 1: Add store_upload method**

```rust
/// Store upload bytes to storage.
pub async fn store_upload(&self, upload_id: &str, data: &[u8]) -> Result<String> {
    let local_path = self.storage.prepare_upload_write(upload_id);
    tokio::fs::write(&local_path, data).await?;
    self.storage.finalize_upload_write(upload_id).await
}
```

**Step 2: Add create_dataset method**

```rust
/// Create a dataset from an upload.
pub async fn create_dataset(
    &self,
    label: &str,
    table_name: Option<&str>,
    upload_id: &str,
    format: Option<&str>,
    _options: Option<&serde_json::Value>,
    _schema_overrides: Option<&[crate::http::models::ColumnOverride]>,
) -> Result<DatasetInfo> {
    use crate::datasets::validation::{validate_table_name, table_name_from_label};

    // Validate or generate table_name
    let table_name = match table_name {
        Some(name) => {
            validate_table_name(name)?;
            name.to_string()
        }
        None => {
            let generated = table_name_from_label(label);
            validate_table_name(&generated)?;
            generated
        }
    };

    // Get upload info
    let upload = self.catalog.get_upload(upload_id).await?
        .ok_or_else(|| anyhow::anyhow!("Upload '{}' not found", upload_id))?;

    if upload.status != "pending" {
        anyhow::bail!("Upload '{}' has already been consumed", upload_id);
    }

    // Resolve format
    let format = format
        .map(|s| s.to_string())
        .or_else(|| upload.content_type.as_ref().and_then(|ct| {
            if ct.contains("csv") { Some("csv".to_string()) }
            else if ct.contains("json") { Some("json".to_string()) }
            else if ct.contains("parquet") { Some("parquet".to_string()) }
            else { None }
        }))
        .ok_or_else(|| anyhow::anyhow!("Format not specified and could not be inferred from content type"))?;

    // Generate dataset ID and prepare write
    let dataset_id = crate::generate_dataset_id();
    let handle = self.storage.prepare_dataset_write(&dataset_id);

    // Convert to parquet (implementation depends on format)
    let schema = self.convert_to_parquet(&upload.storage_url, &format, &handle.local_path).await?;

    // Finalize storage
    let parquet_url = self.storage.finalize_dataset_write(&handle).await?;

    // Serialize schema
    let arrow_schema_json = crate::datafetch::serialize_arrow_schema(&schema)?;

    // Create source config
    let source_config = serde_json::json!({
        "upload_id": upload_id,
        "original_format": format,
    });

    let now = Utc::now();
    let dataset = DatasetInfo {
        id: dataset_id,
        label: label.to_string(),
        schema_name: "default".to_string(),
        table_name,
        parquet_url,
        arrow_schema_json,
        source_type: "upload".to_string(),
        source_config: source_config.to_string(),
        created_at: now,
        updated_at: now,
    };

    // Save to catalog
    self.catalog.create_dataset(&dataset).await?;

    // Mark upload as consumed
    self.catalog.consume_upload(upload_id).await?;

    // Register with DataFusion
    self.register_dataset(&dataset).await?;

    Ok(dataset)
}
```

**Step 3: Add convert_to_parquet helper**

```rust
/// Convert uploaded data to parquet format.
async fn convert_to_parquet(
    &self,
    source_url: &str,
    format: &str,
    output_path: &std::path::Path,
) -> Result<Arc<Schema>> {
    use datafusion::prelude::*;

    let ctx = SessionContext::new();
    self.storage.register_with_datafusion(&ctx)?;

    let df = match format {
        "csv" => {
            ctx.read_csv(source_url, CsvReadOptions::default()).await?
        }
        "json" => {
            ctx.read_json(source_url, NdJsonReadOptions::default()).await?
        }
        "parquet" => {
            ctx.read_parquet(source_url, ParquetReadOptions::default()).await?
        }
        _ => anyhow::bail!("Unsupported format: {}", format),
    };

    let schema = df.schema().inner().clone();

    // Write to parquet with our settings
    df.write_parquet(
        output_path.to_str().unwrap(),
        DataFrameWriteOptions::new(),
        None,
    ).await?;

    Ok(schema)
}
```

**Step 4: Add register_dataset helper**

```rust
/// Register a dataset with DataFusion.
async fn register_dataset(&self, dataset: &DatasetInfo) -> Result<()> {
    // This will be fully implemented when we add DatasetsProvider
    // For now, just register as a table
    let ctx = &self.df_ctx;
    ctx.register_parquet(
        &format!("datasets.{}", dataset.table_name),
        &dataset.parquet_url,
        ParquetReadOptions::default(),
    ).await?;
    Ok(())
}
```

**Step 5: Commit**

```bash
git add src/engine.rs
git commit -m "feat(engine): add upload storage and dataset creation methods"
```

---

## Task 10: Add Dataset HTTP Handlers

**Files:**
- Modify: `src/http/handlers.rs`
- Modify: `src/http/app_server.rs`

**Step 1: Add create dataset handler**

```rust
/// Maximum inline content size (1MB)
const MAX_INLINE_SIZE: usize = 1024 * 1024;

/// Handler for POST /v1/datasets
pub async fn create_dataset_handler(
    State(engine): State<Arc<RuntimeEngine>>,
    Json(request): Json<CreateDatasetRequest>,
) -> Result<(StatusCode, Json<CreateDatasetResponse>), ApiError> {
    let dataset = match request.source {
        DatasetSource::Upload { upload_id, format, options } => {
            let schema_overrides = request.schema.as_ref().map(|s| s.columns.as_slice());
            engine.create_dataset(
                &request.label,
                request.table_name.as_deref(),
                &upload_id,
                format.as_deref(),
                options.as_ref(),
                schema_overrides,
            ).await?
        }
        DatasetSource::Inline { inline } => {
            // Validate size
            if inline.content.len() > MAX_INLINE_SIZE {
                return Err(ApiError::bad_request(format!(
                    "Inline content exceeds maximum size of {} bytes",
                    MAX_INLINE_SIZE
                )));
            }

            engine.create_dataset_from_inline(
                &request.label,
                request.table_name.as_deref(),
                &inline.format,
                &inline.content,
                request.schema.as_ref().map(|s| s.columns.as_slice()),
            ).await?
        }
    };

    Ok((
        StatusCode::CREATED,
        Json(CreateDatasetResponse {
            id: dataset.id,
            label: dataset.label,
            table_name: dataset.table_name,
            status: "ready".to_string(),
        }),
    ))
}
```

**Step 2: Add list/get/update/delete handlers**

```rust
/// Handler for GET /v1/datasets
pub async fn list_datasets_handler(
    State(engine): State<Arc<RuntimeEngine>>,
) -> Result<Json<ListDatasetsResponse>, ApiError> {
    let datasets = engine.catalog().list_datasets().await?;

    let datasets = datasets
        .into_iter()
        .map(|d| DatasetMetadata {
            id: d.id,
            label: d.label,
            table_name: d.table_name,
            source_type: d.source_type,
            created_at: d.created_at,
            updated_at: d.updated_at,
        })
        .collect();

    Ok(Json(ListDatasetsResponse { datasets }))
}

/// Handler for GET /v1/datasets/{id}
pub async fn get_dataset_handler(
    State(engine): State<Arc<RuntimeEngine>>,
    Path(id): Path<String>,
) -> Result<Json<GetDatasetResponse>, ApiError> {
    let dataset = engine.catalog().get_dataset(&id).await?
        .ok_or_else(|| ApiError::not_found(format!("Dataset '{}' not found", id)))?;

    let schema: serde_json::Value = serde_json::from_str(&dataset.arrow_schema_json)
        .unwrap_or(serde_json::Value::Null);
    let source_config: serde_json::Value = serde_json::from_str(&dataset.source_config)
        .unwrap_or(serde_json::Value::Null);

    Ok(Json(GetDatasetResponse {
        id: dataset.id,
        label: dataset.label,
        table_name: dataset.table_name,
        parquet_url: dataset.parquet_url,
        schema,
        source_type: dataset.source_type,
        source_config,
        created_at: dataset.created_at,
        updated_at: dataset.updated_at,
    }))
}

/// Handler for PUT /v1/datasets/{id}
pub async fn update_dataset_handler(
    State(engine): State<Arc<RuntimeEngine>>,
    Path(id): Path<String>,
    Json(request): Json<UpdateDatasetRequest>,
) -> Result<Json<GetDatasetResponse>, ApiError> {
    let current = engine.catalog().get_dataset(&id).await?
        .ok_or_else(|| ApiError::not_found(format!("Dataset '{}' not found", id)))?;

    let new_label = request.label.unwrap_or(current.label.clone());
    let new_table_name = request.table_name.unwrap_or(current.table_name.clone());

    // Validate new table_name if changed
    if new_table_name != current.table_name {
        crate::datasets::validation::validate_table_name(&new_table_name)
            .map_err(|e| ApiError::bad_request(e.to_string()))?;
    }

    engine.catalog().update_dataset(&id, &new_label, &new_table_name).await?;

    // Re-fetch to get updated timestamps
    get_dataset_handler(State(engine), Path(id)).await
}

/// Handler for DELETE /v1/datasets/{id}
pub async fn delete_dataset_handler(
    State(engine): State<Arc<RuntimeEngine>>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let dataset = engine.catalog().delete_dataset(&id).await?
        .ok_or_else(|| ApiError::not_found(format!("Dataset '{}' not found", id)))?;

    // Schedule parquet file for deletion
    engine.schedule_deletion(&dataset.parquet_url).await?;

    Ok(StatusCode::NO_CONTENT)
}
```

**Step 3: Add routes**

Add to `src/http/app_server.rs`:

```rust
pub const PATH_DATASETS: &str = "/v1/datasets";
pub const PATH_DATASET: &str = "/v1/datasets/{id}";

// In router:
.route(
    PATH_DATASETS,
    post(create_dataset_handler).get(list_datasets_handler),
)
.route(
    PATH_DATASET,
    get(get_dataset_handler)
        .put(update_dataset_handler)
        .delete(delete_dataset_handler),
)
```

**Step 4: Commit**

```bash
git add src/http/handlers.rs src/http/app_server.rs
git commit -m "feat(http): add dataset CRUD endpoints"
```

---

## Task 11: Add DatasetsProvider for DataFusion

**Files:**
- Create: `src/datafusion/datasets_provider.rs`
- Modify: `src/datafusion/mod.rs`

**Step 1: Create DatasetsProvider**

Create `src/datafusion/datasets_provider.rs`:

```rust
//! DataFusion catalog provider for user-curated datasets.

use crate::catalog::CatalogManager;
use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::common::DataFusionError;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use std::any::Any;
use std::sync::Arc;

use super::block_on;

/// Schema provider for the default dataset schema.
#[derive(Debug)]
pub struct DatasetsSchemaProvider {
    catalog: Arc<dyn CatalogManager>,
}

impl DatasetsSchemaProvider {
    pub fn new(catalog: Arc<dyn CatalogManager>) -> Self {
        Self { catalog }
    }
}

#[async_trait]
impl SchemaProvider for DatasetsSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        match block_on(self.catalog.list_datasets()) {
            Ok(datasets) => datasets.into_iter().map(|d| d.table_name).collect(),
            Err(_) => Vec::new(),
        }
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let dataset = match self.catalog.get_dataset_by_table_name(name).await {
            Ok(Some(d)) => d,
            Ok(None) => return Ok(None),
            Err(e) => return Err(DataFusionError::External(Box::new(e))),
        };

        // Create listing table from parquet URL
        let table_url = ListingTableUrl::parse(&dataset.parquet_url)?;
        let config = ListingTableConfig::new(table_url)
            .with_listing_options(
                datafusion::datasource::listing::ListingOptions::new(Arc::new(ParquetFormat::default()))
            )
            .infer_schema(&datafusion::execution::context::SessionState::new_with_config(
                datafusion::prelude::SessionConfig::new(),
            ))
            .await?;

        let table = ListingTable::try_new(config)?;
        Ok(Some(Arc::new(table)))
    }

    fn table_exist(&self, name: &str) -> bool {
        match block_on(self.catalog.get_dataset_by_table_name(name)) {
            Ok(Some(_)) => true,
            _ => false,
        }
    }
}

/// Catalog provider for the "datasets" catalog.
#[derive(Debug)]
pub struct DatasetsProvider {
    catalog: Arc<dyn CatalogManager>,
    default_schema: Arc<DatasetsSchemaProvider>,
}

impl DatasetsProvider {
    pub fn new(catalog: Arc<dyn CatalogManager>) -> Self {
        let default_schema = Arc::new(DatasetsSchemaProvider::new(catalog.clone()));
        Self { catalog, default_schema }
    }
}

#[async_trait]
impl CatalogProvider for DatasetsProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec!["default".to_string()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if name == "default" {
            Some(self.default_schema.clone())
        } else {
            None
        }
    }
}
```

**Step 2: Export from mod.rs**

Add to `src/datafusion/mod.rs`:

```rust
pub mod datasets_provider;
pub use datasets_provider::DatasetsProvider;
```

**Step 3: Register provider in engine startup**

Modify `src/engine.rs` to register the datasets catalog at startup:

```rust
// In RuntimeEngine::new or build:
let datasets_provider = DatasetsProvider::new(catalog.clone());
df_ctx.register_catalog("datasets", Arc::new(datasets_provider));
```

**Step 4: Commit**

```bash
git add src/datafusion/datasets_provider.rs src/datafusion/mod.rs src/engine.rs
git commit -m "feat(datafusion): add DatasetsProvider for dataset queries"
```

---

## Task 12: Add Integration Tests

**Files:**
- Create: `tests/dataset_tests.rs`

**Step 1: Create test file**

```rust
//! Integration tests for dataset upload and creation.

use runtimedb::RuntimeEngine;
use tempfile::TempDir;

async fn create_test_engine() -> (RuntimeEngine, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let engine = RuntimeEngine::defaults(temp_dir.path()).await.unwrap();
    (engine, temp_dir)
}

#[tokio::test]
async fn test_upload_and_create_dataset() {
    let (engine, _temp) = create_test_engine().await;

    // Upload CSV data
    let csv_data = b"id,name,value\n1,foo,100\n2,bar,200";
    let upload_id = runtimedb::generate_upload_id();
    let storage_url = engine.store_upload(&upload_id, csv_data).await.unwrap();

    // Create upload record
    let upload = runtimedb::catalog::UploadInfo {
        id: upload_id.clone(),
        status: "pending".to_string(),
        storage_url,
        content_type: Some("text/csv".to_string()),
        content_encoding: None,
        size_bytes: csv_data.len() as i64,
        created_at: chrono::Utc::now(),
        consumed_at: None,
    };
    engine.catalog().create_upload(&upload).await.unwrap();

    // Create dataset
    let dataset = engine
        .create_dataset("Test Dataset", Some("test_data"), &upload_id, Some("csv"), None, None)
        .await
        .unwrap();

    assert_eq!(dataset.label, "Test Dataset");
    assert_eq!(dataset.table_name, "test_data");
    assert_eq!(dataset.source_type, "upload");

    // Query the dataset
    let result = engine.execute_query("SELECT * FROM datasets.test_data").await.unwrap();
    assert_eq!(result.results.len(), 1);
    assert_eq!(result.results[0].num_rows(), 2);
}

#[tokio::test]
async fn test_inline_dataset_creation() {
    let (engine, _temp) = create_test_engine().await;

    let csv_content = "code,label\nUS,United States\nCA,Canada";

    let dataset = engine
        .create_dataset_from_inline("Country Codes", None, "csv", csv_content, None)
        .await
        .unwrap();

    assert_eq!(dataset.label, "Country Codes");
    assert_eq!(dataset.table_name, "country_codes"); // auto-generated

    // Query
    let result = engine.execute_query("SELECT * FROM datasets.country_codes").await.unwrap();
    assert_eq!(result.results[0].num_rows(), 2);
}

#[tokio::test]
async fn test_table_name_validation() {
    let (engine, _temp) = create_test_engine().await;

    // Valid table names
    assert!(runtimedb::datasets::validation::validate_table_name("valid_name").is_ok());
    assert!(runtimedb::datasets::validation::validate_table_name("_private").is_ok());

    // Invalid: starts with number
    assert!(runtimedb::datasets::validation::validate_table_name("123abc").is_err());

    // Invalid: contains dash
    assert!(runtimedb::datasets::validation::validate_table_name("my-table").is_err());

    // Invalid: reserved word
    assert!(runtimedb::datasets::validation::validate_table_name("select").is_err());
}

#[tokio::test]
async fn test_upload_consumed_only_once() {
    let (engine, _temp) = create_test_engine().await;

    // Create upload
    let csv_data = b"a,b\n1,2";
    let upload_id = runtimedb::generate_upload_id();
    let storage_url = engine.store_upload(&upload_id, csv_data).await.unwrap();

    let upload = runtimedb::catalog::UploadInfo {
        id: upload_id.clone(),
        status: "pending".to_string(),
        storage_url,
        content_type: Some("text/csv".to_string()),
        content_encoding: None,
        size_bytes: csv_data.len() as i64,
        created_at: chrono::Utc::now(),
        consumed_at: None,
    };
    engine.catalog().create_upload(&upload).await.unwrap();

    // First creation should succeed
    engine
        .create_dataset("First", Some("first"), &upload_id, Some("csv"), None, None)
        .await
        .unwrap();

    // Second creation should fail
    let result = engine
        .create_dataset("Second", Some("second"), &upload_id, Some("csv"), None, None)
        .await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("already been consumed"));
}
```

**Step 2: Run tests**

Run: `cargo test dataset_tests`
Expected: All tests pass

**Step 3: Commit**

```bash
git add tests/dataset_tests.rs
git commit -m "test: add dataset upload integration tests"
```

---

## Task 13: Add HTTP API Tests

**Files:**
- Modify: `tests/http_server_tests.rs`

**Step 1: Add upload endpoint tests**

```rust
#[tokio::test]
async fn test_upload_file_endpoint() {
    let (app, _temp) = create_test_app().await;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/files")
                .header("content-type", "text/csv")
                .body(Body::from("id,name\n1,test"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);

    let body: serde_json::Value = parse_body(response).await;
    assert!(body["id"].as_str().unwrap().starts_with("upld"));
    assert_eq!(body["status"], "pending");
}

#[tokio::test]
async fn test_create_dataset_from_upload() {
    let (app, _temp) = create_test_app().await;

    // First upload
    let upload_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/files")
                .header("content-type", "text/csv")
                .body(Body::from("a,b\n1,2\n3,4"))
                .unwrap(),
        )
        .await
        .unwrap();

    let upload_body: serde_json::Value = parse_body(upload_response).await;
    let upload_id = upload_body["id"].as_str().unwrap();

    // Create dataset
    let create_response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/datasets")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::json!({
                    "label": "Test Dataset",
                    "source": {
                        "upload_id": upload_id,
                        "format": "csv"
                    }
                }).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(create_response.status(), StatusCode::CREATED);

    let body: serde_json::Value = parse_body(create_response).await;
    assert!(body["id"].as_str().unwrap().starts_with("data"));
    assert_eq!(body["label"], "Test Dataset");
    assert_eq!(body["status"], "ready");
}
```

**Step 2: Run tests**

Run: `cargo test http_server_tests`
Expected: All tests pass

**Step 3: Commit**

```bash
git add tests/http_server_tests.rs
git commit -m "test: add HTTP endpoint tests for uploads and datasets"
```

---

## Summary

This implementation plan covers:

1. **Resource IDs** - Add `upld_` and `data_` prefixes
2. **Database Migrations** - Add `uploads` and `datasets` tables
3. **Catalog Types** - Add `UploadInfo` and `DatasetInfo` structs
4. **Catalog Backend** - Implement CRUD operations
5. **HTTP Models** - Request/response types for API
6. **Validation** - table_name validation and generation
7. **Storage** - Upload and dataset path handling
8. **Upload Handlers** - POST/GET /v1/files endpoints
9. **Engine Methods** - Upload storage and dataset creation
10. **Dataset Handlers** - Full CRUD for /v1/datasets
11. **DatasetsProvider** - DataFusion integration for queries
12. **Integration Tests** - End-to-end dataset tests
13. **HTTP Tests** - API endpoint tests

Each task is atomic and can be committed separately. The implementation follows existing patterns in the codebase (catalog trait, storage manager, HTTP handlers).
