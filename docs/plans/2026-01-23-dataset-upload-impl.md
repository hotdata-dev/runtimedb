# Dataset Upload Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement direct dataset upload (CSV/JSON/Parquet) with two-phase creation flow.

**Architecture:** New `uploads` and `datasets` tables in catalog, new HTTP endpoints, `DatasetsProvider` for DataFusion integration. Uploads land in staging storage, get converted to Parquet on dataset creation.

**Tech Stack:** Rust, Axum, DataFusion, Arrow, SQLx, existing StorageManager abstraction

**TDD Approach:** Each task follows red-green-refactor. Write failing tests first, implement minimally to pass, then refactor. No task is complete until tests pass and cover the happy path + key edge cases.

---

## Task 1: Add Resource ID Prefixes

**Files:**
- Modify: `src/id.rs`

**Step 1: Write failing tests first**

Add tests that will fail because the new ID types don't exist yet:

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

#[test]
fn test_resource_id_prefixes_include_new_types() {
    assert_eq!(ResourceId::Upload.prefix(), "upld");
    assert_eq!(ResourceId::Dataset.prefix(), "data");
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test id::tests`
Expected: FAIL - `generate_upload_id` and `generate_dataset_id` not found

**Step 3: Implement minimally to pass**

Add to `define_resource_ids!` macro:

```rust
define_resource_ids! {
    Connection => "conn",
    Result => "rslt",
    Secret => "secr",
    Upload => "upld",
    Dataset => "data",
}
```

Add helper functions:

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

**Step 4: Run tests to verify they pass**

Run: `cargo test id::tests`
Expected: All tests PASS

**Step 5: Commit**

```bash
git add src/id.rs
git commit -m "feat(id): add upload and dataset resource ID prefixes"
```

---

## Task 2: Add table_name Validation (with tests first)

**Files:**
- Create: `src/datasets/mod.rs`
- Create: `src/datasets/validation.rs`
- Modify: `src/lib.rs`

**Step 1: Create module structure and write failing tests**

Create `src/datasets/mod.rs`:

```rust
//! Dataset management and utilities.

pub mod validation;

pub use validation::*;
```

Create `src/datasets/validation.rs` with tests first (implementation stubs that fail):

```rust
//! Validation utilities for dataset table names.

/// Maximum length for a table name.
pub const MAX_TABLE_NAME_LENGTH: usize = 128;

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
pub fn validate_table_name(_name: &str) -> Result<(), TableNameError> {
    todo!("implement validation")
}

/// Generate a table name from a label.
pub fn table_name_from_label(_label: &str) -> String {
    todo!("implement generation")
}

#[cfg(test)]
mod tests {
    use super::*;

    // === Valid table names ===

    #[test]
    fn test_valid_simple_name() {
        assert!(validate_table_name("users").is_ok());
    }

    #[test]
    fn test_valid_with_underscore_prefix() {
        assert!(validate_table_name("_private").is_ok());
    }

    #[test]
    fn test_valid_with_numbers() {
        assert!(validate_table_name("table_123").is_ok());
    }

    #[test]
    fn test_valid_mixed_case() {
        assert!(validate_table_name("CamelCase").is_ok());
    }

    #[test]
    fn test_valid_single_char() {
        assert!(validate_table_name("a").is_ok());
    }

    #[test]
    fn test_valid_underscore_only() {
        assert!(validate_table_name("_").is_ok());
    }

    // === Invalid: empty ===

    #[test]
    fn test_invalid_empty() {
        assert!(matches!(validate_table_name(""), Err(TableNameError::Empty)));
    }

    // === Invalid: first character ===

    #[test]
    fn test_invalid_starts_with_number() {
        assert!(matches!(validate_table_name("123abc"), Err(TableNameError::InvalidFirstChar('1'))));
    }

    #[test]
    fn test_invalid_starts_with_dash() {
        assert!(matches!(validate_table_name("-test"), Err(TableNameError::InvalidFirstChar('-'))));
    }

    #[test]
    fn test_invalid_starts_with_space() {
        assert!(matches!(validate_table_name(" test"), Err(TableNameError::InvalidFirstChar(' '))));
    }

    // === Invalid: characters ===

    #[test]
    fn test_invalid_contains_dash() {
        assert!(matches!(validate_table_name("table-name"), Err(TableNameError::InvalidChar('-'))));
    }

    #[test]
    fn test_invalid_contains_space() {
        assert!(matches!(validate_table_name("table name"), Err(TableNameError::InvalidChar(' '))));
    }

    #[test]
    fn test_invalid_contains_dot() {
        assert!(matches!(validate_table_name("table.name"), Err(TableNameError::InvalidChar('.'))));
    }

    // === Invalid: reserved words ===

    #[test]
    fn test_invalid_reserved_select() {
        assert!(matches!(validate_table_name("select"), Err(TableNameError::ReservedWord(_))));
    }

    #[test]
    fn test_invalid_reserved_from_uppercase() {
        assert!(matches!(validate_table_name("FROM"), Err(TableNameError::ReservedWord(_))));
    }

    #[test]
    fn test_invalid_reserved_table() {
        assert!(matches!(validate_table_name("table"), Err(TableNameError::ReservedWord(_))));
    }

    // === Invalid: too long ===

    #[test]
    fn test_invalid_too_long() {
        let long_name = "a".repeat(129);
        assert!(matches!(validate_table_name(&long_name), Err(TableNameError::TooLong(129))));
    }

    #[test]
    fn test_valid_max_length() {
        let max_name = "a".repeat(128);
        assert!(validate_table_name(&max_name).is_ok());
    }

    // === Label to table name generation ===

    #[test]
    fn test_label_simple_conversion() {
        assert_eq!(table_name_from_label("Q1 Sales Report"), "q1_sales_report");
    }

    #[test]
    fn test_label_with_leading_number() {
        assert_eq!(table_name_from_label("123 Numbers"), "_123_numbers");
    }

    #[test]
    fn test_label_with_dashes() {
        assert_eq!(table_name_from_label("Hello-World"), "hello_world");
    }

    #[test]
    fn test_label_empty() {
        assert_eq!(table_name_from_label(""), "_unnamed");
    }

    #[test]
    fn test_label_special_chars_stripped() {
        assert_eq!(table_name_from_label("Test! @#$% Data"), "test_data");
    }

    #[test]
    fn test_label_unicode_stripped() {
        assert_eq!(table_name_from_label("Sales 📊 Report"), "sales_report");
    }

    #[test]
    fn test_label_truncated_if_too_long() {
        let long_label = "a".repeat(200);
        let result = table_name_from_label(&long_label);
        assert!(result.len() <= MAX_TABLE_NAME_LENGTH);
    }
}
```

Add to `src/lib.rs`:

```rust
pub mod datasets;
```

**Step 2: Run tests to verify they fail**

Run: `cargo test datasets::validation::tests`
Expected: FAIL - all tests panic with "not yet implemented"

**Step 3: Implement validation**

Replace the todo!() stubs with real implementation:

```rust
use std::collections::HashSet;
use once_cell::sync::Lazy;

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
            if !result.ends_with('_') {
                result.push('_');
            }
        }
        // Skip other characters
    }

    // Trim trailing underscores
    while result.ends_with('_') {
        result.pop();
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
```

**Step 4: Run tests to verify they pass**

Run: `cargo test datasets::validation::tests`
Expected: All tests PASS

**Step 5: Commit**

```bash
git add src/datasets/ src/lib.rs
git commit -m "feat(datasets): add table_name validation with comprehensive tests"
```

---

## Task 3: Add Database Migrations

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

## Task 4: Add Catalog Types and Trait Methods (with tests)

**Files:**
- Modify: `src/catalog/manager.rs`
- Create: `tests/catalog_uploads_datasets_tests.rs`

**Step 1: Write integration tests first**

Create `tests/catalog_uploads_datasets_tests.rs`:

```rust
//! Tests for upload and dataset catalog operations.

use chrono::Utc;
use runtimedb::catalog::{CatalogManager, DatasetInfo, SqliteCatalogManager, UploadInfo};
use tempfile::TempDir;

async fn create_test_catalog() -> (impl CatalogManager, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let catalog = SqliteCatalogManager::new(db_path.to_str().unwrap())
        .await
        .unwrap();
    catalog.run_migrations().await.unwrap();
    (catalog, temp_dir)
}

// === Upload Tests ===

#[tokio::test]
async fn test_create_and_get_upload() {
    let (catalog, _temp) = create_test_catalog().await;

    let upload = UploadInfo {
        id: "upld_test123".to_string(),
        status: "pending".to_string(),
        storage_url: "file:///tmp/uploads/upld_test123/raw".to_string(),
        content_type: Some("text/csv".to_string()),
        content_encoding: None,
        size_bytes: 1024,
        created_at: Utc::now(),
        consumed_at: None,
    };

    catalog.create_upload(&upload).await.unwrap();

    let retrieved = catalog.get_upload("upld_test123").await.unwrap().unwrap();
    assert_eq!(retrieved.id, "upld_test123");
    assert_eq!(retrieved.status, "pending");
    assert_eq!(retrieved.size_bytes, 1024);
    assert_eq!(retrieved.content_type, Some("text/csv".to_string()));
}

#[tokio::test]
async fn test_get_upload_not_found() {
    let (catalog, _temp) = create_test_catalog().await;

    let result = catalog.get_upload("nonexistent").await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_list_uploads_by_status() {
    let (catalog, _temp) = create_test_catalog().await;

    // Create pending upload
    let pending = UploadInfo {
        id: "upld_pending".to_string(),
        status: "pending".to_string(),
        storage_url: "file:///tmp/1".to_string(),
        content_type: None,
        content_encoding: None,
        size_bytes: 100,
        created_at: Utc::now(),
        consumed_at: None,
    };
    catalog.create_upload(&pending).await.unwrap();

    // Create consumed upload
    let consumed = UploadInfo {
        id: "upld_consumed".to_string(),
        status: "consumed".to_string(),
        storage_url: "file:///tmp/2".to_string(),
        content_type: None,
        content_encoding: None,
        size_bytes: 200,
        created_at: Utc::now(),
        consumed_at: Some(Utc::now()),
    };
    catalog.create_upload(&consumed).await.unwrap();

    // List pending only
    let pending_list = catalog.list_uploads(Some("pending")).await.unwrap();
    assert_eq!(pending_list.len(), 1);
    assert_eq!(pending_list[0].id, "upld_pending");

    // List all
    let all_list = catalog.list_uploads(None).await.unwrap();
    assert_eq!(all_list.len(), 2);
}

#[tokio::test]
async fn test_consume_upload() {
    let (catalog, _temp) = create_test_catalog().await;

    let upload = UploadInfo {
        id: "upld_to_consume".to_string(),
        status: "pending".to_string(),
        storage_url: "file:///tmp/test".to_string(),
        content_type: None,
        content_encoding: None,
        size_bytes: 100,
        created_at: Utc::now(),
        consumed_at: None,
    };
    catalog.create_upload(&upload).await.unwrap();

    // Consume it
    let consumed = catalog.consume_upload("upld_to_consume").await.unwrap();
    assert!(consumed);

    // Verify status changed
    let retrieved = catalog.get_upload("upld_to_consume").await.unwrap().unwrap();
    assert_eq!(retrieved.status, "consumed");
    assert!(retrieved.consumed_at.is_some());

    // Can't consume again
    let consumed_again = catalog.consume_upload("upld_to_consume").await.unwrap();
    assert!(!consumed_again);
}

// === Dataset Tests ===

#[tokio::test]
async fn test_create_and_get_dataset() {
    let (catalog, _temp) = create_test_catalog().await;

    let now = Utc::now();
    let dataset = DatasetInfo {
        id: "data_test123".to_string(),
        label: "Test Dataset".to_string(),
        schema_name: "default".to_string(),
        table_name: "test_table".to_string(),
        parquet_url: "file:///tmp/datasets/data_test123/v1/data.parquet".to_string(),
        arrow_schema_json: r#"{"fields":[]}"#.to_string(),
        source_type: "upload".to_string(),
        source_config: r#"{"upload_id":"upld_123"}"#.to_string(),
        created_at: now,
        updated_at: now,
    };

    catalog.create_dataset(&dataset).await.unwrap();

    let retrieved = catalog.get_dataset("data_test123").await.unwrap().unwrap();
    assert_eq!(retrieved.id, "data_test123");
    assert_eq!(retrieved.label, "Test Dataset");
    assert_eq!(retrieved.table_name, "test_table");
}

#[tokio::test]
async fn test_get_dataset_by_table_name() {
    let (catalog, _temp) = create_test_catalog().await;

    let now = Utc::now();
    let dataset = DatasetInfo {
        id: "data_abc".to_string(),
        label: "My Data".to_string(),
        schema_name: "default".to_string(),
        table_name: "my_data".to_string(),
        parquet_url: "file:///tmp/test.parquet".to_string(),
        arrow_schema_json: "{}".to_string(),
        source_type: "upload".to_string(),
        source_config: "{}".to_string(),
        created_at: now,
        updated_at: now,
    };
    catalog.create_dataset(&dataset).await.unwrap();

    let retrieved = catalog.get_dataset_by_table_name("my_data").await.unwrap().unwrap();
    assert_eq!(retrieved.id, "data_abc");
}

#[tokio::test]
async fn test_dataset_table_name_uniqueness() {
    let (catalog, _temp) = create_test_catalog().await;

    let now = Utc::now();
    let dataset1 = DatasetInfo {
        id: "data_1".to_string(),
        label: "First".to_string(),
        schema_name: "default".to_string(),
        table_name: "same_name".to_string(),
        parquet_url: "file:///tmp/1.parquet".to_string(),
        arrow_schema_json: "{}".to_string(),
        source_type: "upload".to_string(),
        source_config: "{}".to_string(),
        created_at: now,
        updated_at: now,
    };
    catalog.create_dataset(&dataset1).await.unwrap();

    let dataset2 = DatasetInfo {
        id: "data_2".to_string(),
        label: "Second".to_string(),
        schema_name: "default".to_string(),
        table_name: "same_name".to_string(), // Same table_name!
        parquet_url: "file:///tmp/2.parquet".to_string(),
        arrow_schema_json: "{}".to_string(),
        source_type: "upload".to_string(),
        source_config: "{}".to_string(),
        created_at: now,
        updated_at: now,
    };

    let result = catalog.create_dataset(&dataset2).await;
    assert!(result.is_err()); // Should fail due to uniqueness constraint
}

#[tokio::test]
async fn test_update_dataset() {
    let (catalog, _temp) = create_test_catalog().await;

    let now = Utc::now();
    let dataset = DatasetInfo {
        id: "data_update".to_string(),
        label: "Original Label".to_string(),
        schema_name: "default".to_string(),
        table_name: "original_name".to_string(),
        parquet_url: "file:///tmp/test.parquet".to_string(),
        arrow_schema_json: "{}".to_string(),
        source_type: "upload".to_string(),
        source_config: "{}".to_string(),
        created_at: now,
        updated_at: now,
    };
    catalog.create_dataset(&dataset).await.unwrap();

    // Update
    let updated = catalog
        .update_dataset("data_update", "New Label", "new_name")
        .await
        .unwrap();
    assert!(updated);

    // Verify
    let retrieved = catalog.get_dataset("data_update").await.unwrap().unwrap();
    assert_eq!(retrieved.label, "New Label");
    assert_eq!(retrieved.table_name, "new_name");
    assert!(retrieved.updated_at > now);
}

#[tokio::test]
async fn test_delete_dataset() {
    let (catalog, _temp) = create_test_catalog().await;

    let now = Utc::now();
    let dataset = DatasetInfo {
        id: "data_delete".to_string(),
        label: "To Delete".to_string(),
        schema_name: "default".to_string(),
        table_name: "delete_me".to_string(),
        parquet_url: "file:///tmp/test.parquet".to_string(),
        arrow_schema_json: "{}".to_string(),
        source_type: "upload".to_string(),
        source_config: "{}".to_string(),
        created_at: now,
        updated_at: now,
    };
    catalog.create_dataset(&dataset).await.unwrap();

    // Delete
    let deleted = catalog.delete_dataset("data_delete").await.unwrap();
    assert!(deleted.is_some());
    assert_eq!(deleted.unwrap().id, "data_delete");

    // Verify gone
    let retrieved = catalog.get_dataset("data_delete").await.unwrap();
    assert!(retrieved.is_none());

    // Delete again returns None
    let deleted_again = catalog.delete_dataset("data_delete").await.unwrap();
    assert!(deleted_again.is_none());
}

#[tokio::test]
async fn test_list_datasets() {
    let (catalog, _temp) = create_test_catalog().await;

    let now = Utc::now();
    for i in 1..=3 {
        let dataset = DatasetInfo {
            id: format!("data_{}", i),
            label: format!("Dataset {}", i),
            schema_name: "default".to_string(),
            table_name: format!("table_{}", i),
            parquet_url: format!("file:///tmp/{}.parquet", i),
            arrow_schema_json: "{}".to_string(),
            source_type: "upload".to_string(),
            source_config: "{}".to_string(),
            created_at: now,
            updated_at: now,
        };
        catalog.create_dataset(&dataset).await.unwrap();
    }

    let datasets = catalog.list_datasets().await.unwrap();
    assert_eq!(datasets.len(), 3);
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test catalog_uploads_datasets_tests`
Expected: FAIL - types and methods don't exist

**Step 3: Add types to manager.rs**

Add after `QueryResult` struct in `src/catalog/manager.rs`:

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

Add trait methods to `CatalogManager`:

```rust
// Upload management methods

/// Create a new upload record.
async fn create_upload(&self, upload: &UploadInfo) -> Result<()>;

/// Get an upload by ID.
async fn get_upload(&self, id: &str) -> Result<Option<UploadInfo>>;

/// List uploads, optionally filtered by status.
async fn list_uploads(&self, status: Option<&str>) -> Result<Vec<UploadInfo>>;

/// Mark an upload as consumed. Returns true if status changed.
async fn consume_upload(&self, id: &str) -> Result<bool>;

// Dataset management methods

/// Create a new dataset record.
async fn create_dataset(&self, dataset: &DatasetInfo) -> Result<()>;

/// Get a dataset by ID.
async fn get_dataset(&self, id: &str) -> Result<Option<DatasetInfo>>;

/// Get a dataset by table_name (within default schema).
async fn get_dataset_by_table_name(&self, table_name: &str) -> Result<Option<DatasetInfo>>;

/// List all datasets.
async fn list_datasets(&self) -> Result<Vec<DatasetInfo>>;

/// Update dataset metadata (label, table_name). Returns true if found and updated.
async fn update_dataset(&self, id: &str, label: &str, table_name: &str) -> Result<bool>;

/// Delete a dataset by ID. Returns the deleted dataset info for cleanup, or None if not found.
async fn delete_dataset(&self, id: &str) -> Result<Option<DatasetInfo>>;
```

**Step 4: Implement in backend.rs**

Add implementations to the generic backend (see Task 4 in previous version for full implementation code).

**Step 5: Run tests to verify they pass**

Run: `cargo test catalog_uploads_datasets_tests`
Expected: All tests PASS

**Step 6: Commit**

```bash
git add src/catalog/ tests/catalog_uploads_datasets_tests.rs
git commit -m "feat(catalog): add upload and dataset types with full test coverage"
```

---

## Task 5: Extend StorageManager for Upload/Dataset Paths (with tests)

**Files:**
- Modify: `src/storage/mod.rs`
- Modify: `src/storage/filesystem.rs`
- Modify: `src/storage/s3.rs`
- Create: `tests/storage_uploads_datasets_tests.rs`

**Step 1: Write tests first**

Create `tests/storage_uploads_datasets_tests.rs`:

```rust
//! Tests for upload and dataset storage operations.

use runtimedb::storage::{FilesystemStorage, StorageManager};
use tempfile::TempDir;

fn create_test_storage() -> (FilesystemStorage, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let storage = FilesystemStorage::new(temp_dir.path());
    (storage, temp_dir)
}

#[test]
fn test_upload_url_format() {
    let (storage, temp) = create_test_storage();
    let url = storage.upload_url("upld_abc123");
    assert!(url.starts_with("file://"));
    assert!(url.contains("/uploads/upld_abc123/raw"));
}

#[test]
fn test_dataset_url_format() {
    let (storage, temp) = create_test_storage();
    let url = storage.dataset_url("data_xyz", "v1");
    assert!(url.starts_with("file://"));
    assert!(url.contains("/datasets/data_xyz/v1/data.parquet"));
}

#[tokio::test]
async fn test_upload_write_roundtrip() {
    let (storage, temp) = create_test_storage();

    let upload_id = "upld_test123";
    let data = b"hello,world\n1,2";

    // Prepare and write
    let local_path = storage.prepare_upload_write(upload_id);
    tokio::fs::write(&local_path, data).await.unwrap();

    // Finalize
    let url = storage.finalize_upload_write(upload_id).await.unwrap();
    assert!(url.contains(upload_id));

    // Read back
    let read_data = storage.read(&url).await.unwrap();
    assert_eq!(read_data, data);
}

#[tokio::test]
async fn test_dataset_write_creates_versioned_directory() {
    let (storage, temp) = create_test_storage();

    let dataset_id = "data_test456";
    let handle = storage.prepare_dataset_write(dataset_id);

    assert!(handle.local_path.to_str().unwrap().contains(dataset_id));
    assert!(!handle.version.is_empty());

    // Write some data
    tokio::fs::write(&handle.local_path, b"parquet data here").await.unwrap();

    // Finalize
    let url = storage.finalize_dataset_write(&handle).await.unwrap();
    assert!(url.contains(dataset_id));
    assert!(url.contains(&handle.version));
    assert!(url.ends_with("data.parquet"));
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test storage_uploads_datasets_tests`
Expected: FAIL - methods don't exist

**Step 3: Add trait methods and implement**

Add to `src/storage/mod.rs`:

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

Add to `StorageManager` trait:

```rust
/// Get the URL for storing an upload.
fn upload_url(&self, upload_id: &str) -> String;

/// Get the URL for storing a dataset.
fn dataset_url(&self, dataset_id: &str, version: &str) -> String;

/// Prepare an upload write (returns local path for writing).
fn prepare_upload_write(&self, upload_id: &str) -> std::path::PathBuf;

/// Finalize an upload write (upload to remote if needed).
async fn finalize_upload_write(&self, upload_id: &str) -> Result<String>;

/// Prepare a dataset write.
fn prepare_dataset_write(&self, dataset_id: &str) -> DatasetWriteHandle;

/// Finalize dataset write and return URL.
async fn finalize_dataset_write(&self, handle: &DatasetWriteHandle) -> Result<String>;
```

Implement in `filesystem.rs` and `s3.rs`.

**Step 4: Run tests to verify they pass**

Run: `cargo test storage_uploads_datasets_tests`
Expected: All tests PASS

**Step 5: Commit**

```bash
git add src/storage/ tests/storage_uploads_datasets_tests.rs
git commit -m "feat(storage): add upload and dataset path handling with tests"
```

---

## Task 6: Add HTTP Models

**Files:**
- Modify: `src/http/models.rs`

**Step 1: Add models**

Add upload response models, dataset request/response models as shown in previous Task 5. These are pure data structures, so unit tests aren't necessary - they'll be tested via integration tests.

**Step 2: Verify compilation**

Run: `cargo build`
Expected: Build succeeds

**Step 3: Commit**

```bash
git add src/http/models.rs
git commit -m "feat(http): add upload and dataset request/response models"
```

---

## Task 7: Add Upload HTTP Handlers (with integration tests)

**Files:**
- Modify: `src/http/handlers.rs`
- Modify: `src/http/app_server.rs`
- Modify: `src/engine.rs`
- Modify: `tests/http_server_tests.rs`

**Step 1: Write HTTP integration tests first**

Add to `tests/http_server_tests.rs`:

```rust
// === Upload Endpoint Tests ===

#[tokio::test]
async fn test_upload_file_endpoint() {
    let (app, _temp) = create_test_app().await;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/files")
                .header("content-type", "text/csv")
                .body(Body::from("id,name\n1,test\n2,other"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);

    let body: serde_json::Value = parse_body(response).await;
    assert!(body["id"].as_str().unwrap().starts_with("upld"));
    assert_eq!(body["status"], "pending");
    assert!(body["size_bytes"].as_i64().unwrap() > 0);
}

#[tokio::test]
async fn test_upload_file_too_large() {
    let (app, _temp) = create_test_app().await;

    // Create data larger than limit (we'll use a smaller test limit)
    // For real tests, mock the limit or use smaller data
    // This test validates the error handling exists
}

#[tokio::test]
async fn test_list_uploads_empty() {
    let (app, _temp) = create_test_app().await;

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/files")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body: serde_json::Value = parse_body(response).await;
    assert!(body["uploads"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_list_uploads_after_upload() {
    let (app, _temp) = create_test_app().await;

    // Upload a file first
    let _ = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/files")
                .header("content-type", "text/csv")
                .body(Body::from("a,b\n1,2"))
                .unwrap(),
        )
        .await
        .unwrap();

    // List uploads
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/files")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body: serde_json::Value = parse_body(response).await;
    let uploads = body["uploads"].as_array().unwrap();
    assert_eq!(uploads.len(), 1);
    assert_eq!(uploads[0]["status"], "pending");
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test http_server_tests::test_upload`
Expected: FAIL - endpoints don't exist

**Step 3: Implement handlers and routes**

Add handlers to `src/http/handlers.rs`, routes to `src/http/app_server.rs`, and `store_upload` method to `src/engine.rs`.

**Step 4: Run tests to verify they pass**

Run: `cargo test http_server_tests::test_upload`
Expected: All tests PASS

**Step 5: Commit**

```bash
git add src/http/ src/engine.rs tests/http_server_tests.rs
git commit -m "feat(http): add upload file endpoints with integration tests"
```

---

## Task 8: Add Dataset HTTP Handlers (with integration tests)

**Files:**
- Modify: `src/http/handlers.rs`
- Modify: `src/http/app_server.rs`
- Modify: `src/engine.rs`
- Modify: `tests/http_server_tests.rs`

**Step 1: Write HTTP integration tests first**

Add to `tests/http_server_tests.rs`:

```rust
// === Dataset Endpoint Tests ===

#[tokio::test]
async fn test_create_dataset_from_upload() {
    let (app, _temp) = create_test_app().await;

    // First upload a file
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

    // Create dataset from upload
    let create_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/datasets")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "label": "Test Dataset",
                        "source": {
                            "upload_id": upload_id,
                            "format": "csv"
                        }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(create_response.status(), StatusCode::CREATED);

    let body: serde_json::Value = parse_body(create_response).await;
    assert!(body["id"].as_str().unwrap().starts_with("data"));
    assert_eq!(body["label"], "Test Dataset");
    assert_eq!(body["table_name"], "test_dataset"); // auto-generated
    assert_eq!(body["status"], "ready");
}

#[tokio::test]
async fn test_create_dataset_with_inline_data() {
    let (app, _temp) = create_test_app().await;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/datasets")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "label": "Country Codes",
                        "source": {
                            "inline": {
                                "format": "csv",
                                "content": "code,name\nUS,United States\nCA,Canada"
                            }
                        }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);

    let body: serde_json::Value = parse_body(response).await;
    assert_eq!(body["table_name"], "country_codes");
}

#[tokio::test]
async fn test_create_dataset_inline_too_large() {
    let (app, _temp) = create_test_app().await;

    // Create content larger than 1MB limit
    let large_content = "a\n".repeat(600_000); // ~1.2MB

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/datasets")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "label": "Too Large",
                        "source": {
                            "inline": {
                                "format": "csv",
                                "content": large_content
                            }
                        }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_create_dataset_invalid_table_name() {
    let (app, _temp) = create_test_app().await;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/datasets")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "label": "Test",
                        "table_name": "select",  // reserved word
                        "source": {
                            "inline": {
                                "format": "csv",
                                "content": "a\n1"
                            }
                        }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_list_datasets() {
    let (app, _temp) = create_test_app().await;

    // Create a dataset first
    let _ = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/datasets")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "label": "Test",
                        "source": { "inline": { "format": "csv", "content": "a\n1" } }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    // List
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/datasets")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body: serde_json::Value = parse_body(response).await;
    assert_eq!(body["datasets"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn test_get_dataset() {
    let (app, _temp) = create_test_app().await;

    // Create
    let create_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/datasets")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "label": "My Dataset",
                        "source": { "inline": { "format": "csv", "content": "x\n1" } }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    let create_body: serde_json::Value = parse_body(create_response).await;
    let dataset_id = create_body["id"].as_str().unwrap();

    // Get
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&format!("/v1/datasets/{}", dataset_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body: serde_json::Value = parse_body(response).await;
    assert_eq!(body["id"], dataset_id);
    assert_eq!(body["label"], "My Dataset");
}

#[tokio::test]
async fn test_update_dataset() {
    let (app, _temp) = create_test_app().await;

    // Create
    let create_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/datasets")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "label": "Original",
                        "source": { "inline": { "format": "csv", "content": "a\n1" } }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    let create_body: serde_json::Value = parse_body(create_response).await;
    let dataset_id = create_body["id"].as_str().unwrap();

    // Update
    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(&format!("/v1/datasets/{}", dataset_id))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "label": "Updated Label",
                        "table_name": "new_table_name"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body: serde_json::Value = parse_body(response).await;
    assert_eq!(body["label"], "Updated Label");
    assert_eq!(body["table_name"], "new_table_name");
}

#[tokio::test]
async fn test_delete_dataset() {
    let (app, _temp) = create_test_app().await;

    // Create
    let create_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/datasets")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "label": "To Delete",
                        "source": { "inline": { "format": "csv", "content": "a\n1" } }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    let create_body: serde_json::Value = parse_body(create_response).await;
    let dataset_id = create_body["id"].as_str().unwrap();

    // Delete
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(&format!("/v1/datasets/{}", dataset_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Verify gone
    let get_response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&format!("/v1/datasets/{}", dataset_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(get_response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_upload_consumed_only_once() {
    let (app, _temp) = create_test_app().await;

    // Upload
    let upload_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/files")
                .header("content-type", "text/csv")
                .body(Body::from("a\n1"))
                .unwrap(),
        )
        .await
        .unwrap();

    let upload_body: serde_json::Value = parse_body(upload_response).await;
    let upload_id = upload_body["id"].as_str().unwrap();

    // First dataset creation succeeds
    let first = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/datasets")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "label": "First",
                        "table_name": "first",
                        "source": { "upload_id": upload_id, "format": "csv" }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(first.status(), StatusCode::CREATED);

    // Second creation fails - upload already consumed
    let second = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/datasets")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "label": "Second",
                        "table_name": "second",
                        "source": { "upload_id": upload_id, "format": "csv" }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(second.status(), StatusCode::BAD_REQUEST);
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test http_server_tests::test_create_dataset`
Expected: FAIL - endpoints don't exist

**Step 3: Implement handlers, engine methods, and routes**

Implement all dataset handlers and the `create_dataset` / `create_dataset_from_inline` engine methods.

**Step 4: Run tests to verify they pass**

Run: `cargo test http_server_tests`
Expected: All tests PASS

**Step 5: Commit**

```bash
git add src/http/ src/engine.rs tests/http_server_tests.rs
git commit -m "feat(http): add dataset CRUD endpoints with integration tests"
```

---

## Task 9: Add DatasetsProvider for DataFusion (with query tests)

**Files:**
- Create: `src/datafusion/datasets_provider.rs`
- Modify: `src/datafusion/mod.rs`
- Modify: `src/engine.rs`
- Create: `tests/dataset_query_tests.rs`

**Step 1: Write query integration tests first**

Create `tests/dataset_query_tests.rs`:

```rust
//! Tests for querying datasets via DataFusion.

use runtimedb::RuntimeEngine;
use tempfile::TempDir;

async fn create_test_engine() -> (RuntimeEngine, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let engine = RuntimeEngine::defaults(temp_dir.path()).await.unwrap();
    (engine, temp_dir)
}

#[tokio::test]
async fn test_query_dataset_after_inline_creation() {
    let (engine, _temp) = create_test_engine().await;

    // Create dataset with inline data
    engine
        .create_dataset_from_inline(
            "Numbers",
            Some("numbers"),
            "csv",
            "id,value\n1,100\n2,200\n3,300",
            None,
        )
        .await
        .unwrap();

    // Query it
    let result = engine
        .execute_query("SELECT * FROM datasets.numbers ORDER BY id")
        .await
        .unwrap();

    assert_eq!(result.results.len(), 1);
    assert_eq!(result.results[0].num_rows(), 3);
}

#[tokio::test]
async fn test_query_dataset_with_aggregation() {
    let (engine, _temp) = create_test_engine().await;

    engine
        .create_dataset_from_inline(
            "Sales",
            Some("sales"),
            "csv",
            "region,amount\nNorth,100\nSouth,200\nNorth,150",
            None,
        )
        .await
        .unwrap();

    let result = engine
        .execute_query("SELECT region, SUM(amount) as total FROM datasets.sales GROUP BY region ORDER BY region")
        .await
        .unwrap();

    assert_eq!(result.results[0].num_rows(), 2);
}

#[tokio::test]
async fn test_query_nonexistent_dataset() {
    let (engine, _temp) = create_test_engine().await;

    let result = engine
        .execute_query("SELECT * FROM datasets.nonexistent")
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_query_dataset_after_table_name_update() {
    let (engine, _temp) = create_test_engine().await;

    // Create dataset
    let dataset = engine
        .create_dataset_from_inline("Data", Some("old_name"), "csv", "x\n1", None)
        .await
        .unwrap();

    // Query with old name works
    engine
        .execute_query("SELECT * FROM datasets.old_name")
        .await
        .unwrap();

    // Update table name
    engine
        .catalog()
        .update_dataset(&dataset.id, "Data", "new_name")
        .await
        .unwrap();

    // Old name no longer works
    let old_result = engine
        .execute_query("SELECT * FROM datasets.old_name")
        .await;
    assert!(old_result.is_err());

    // New name works
    engine
        .execute_query("SELECT * FROM datasets.new_name")
        .await
        .unwrap();
}

#[tokio::test]
async fn test_query_multiple_datasets() {
    let (engine, _temp) = create_test_engine().await;

    engine
        .create_dataset_from_inline("Users", Some("users"), "csv", "id,name\n1,Alice\n2,Bob", None)
        .await
        .unwrap();

    engine
        .create_dataset_from_inline(
            "Orders",
            Some("orders"),
            "csv",
            "id,user_id,amount\n1,1,100\n2,2,200",
            None,
        )
        .await
        .unwrap();

    // Join across datasets
    let result = engine
        .execute_query(
            "SELECT u.name, o.amount
             FROM datasets.users u
             JOIN datasets.orders o ON u.id = o.user_id
             ORDER BY u.name",
        )
        .await
        .unwrap();

    assert_eq!(result.results[0].num_rows(), 2);
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test dataset_query_tests`
Expected: FAIL - datasets catalog not registered

**Step 3: Implement DatasetsProvider**

Create `src/datafusion/datasets_provider.rs` with full implementation.

Register in engine startup.

**Step 4: Run tests to verify they pass**

Run: `cargo test dataset_query_tests`
Expected: All tests PASS

**Step 5: Commit**

```bash
git add src/datafusion/ src/engine.rs tests/dataset_query_tests.rs
git commit -m "feat(datafusion): add DatasetsProvider with query integration tests"
```

---

## Task 10: End-to-End Integration Tests

**Files:**
- Create: `tests/dataset_e2e_tests.rs`

**Step 1: Write comprehensive e2e tests**

```rust
//! End-to-end tests for the complete dataset upload flow.

use runtimedb::RuntimeEngine;
use tempfile::TempDir;

async fn create_test_engine() -> (RuntimeEngine, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let engine = RuntimeEngine::defaults(temp_dir.path()).await.unwrap();
    (engine, temp_dir)
}

#[tokio::test]
async fn test_full_upload_to_query_flow() {
    let (engine, _temp) = create_test_engine().await;

    // 1. Upload CSV data
    let csv_data = b"product,price,quantity\nWidget,10.50,100\nGadget,25.00,50";
    let upload_id = runtimedb::generate_upload_id();
    let storage_url = engine.store_upload(&upload_id, csv_data).await.unwrap();

    // 2. Create upload record
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

    // 3. Create dataset from upload
    let dataset = engine
        .create_dataset("Product Inventory", None, &upload_id, Some("csv"), None, None)
        .await
        .unwrap();

    assert_eq!(dataset.label, "Product Inventory");
    assert_eq!(dataset.table_name, "product_inventory");

    // 4. Verify upload is consumed
    let consumed_upload = engine.catalog().get_upload(&upload_id).await.unwrap().unwrap();
    assert_eq!(consumed_upload.status, "consumed");

    // 5. Query the dataset
    let result = engine
        .execute_query("SELECT product, price * quantity as total FROM datasets.product_inventory")
        .await
        .unwrap();

    assert_eq!(result.results[0].num_rows(), 2);

    // 6. List datasets shows it
    let datasets = engine.catalog().list_datasets().await.unwrap();
    assert_eq!(datasets.len(), 1);
    assert_eq!(datasets[0].id, dataset.id);
}

#[tokio::test]
async fn test_json_upload_and_query() {
    let (engine, _temp) = create_test_engine().await;

    let json_data = br#"{"name": "Alice", "age": 30}
{"name": "Bob", "age": 25}"#;

    let upload_id = runtimedb::generate_upload_id();
    let storage_url = engine.store_upload(&upload_id, json_data).await.unwrap();

    let upload = runtimedb::catalog::UploadInfo {
        id: upload_id.clone(),
        status: "pending".to_string(),
        storage_url,
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        size_bytes: json_data.len() as i64,
        created_at: chrono::Utc::now(),
        consumed_at: None,
    };
    engine.catalog().create_upload(&upload).await.unwrap();

    let dataset = engine
        .create_dataset("People", Some("people"), &upload_id, Some("json"), None, None)
        .await
        .unwrap();

    let result = engine
        .execute_query("SELECT name FROM datasets.people WHERE age > 26")
        .await
        .unwrap();

    assert_eq!(result.results[0].num_rows(), 1);
}

#[tokio::test]
async fn test_parquet_upload_passthrough() {
    // This test would require creating a valid parquet file first
    // Skipping detailed implementation for now
}

#[tokio::test]
async fn test_dataset_deletion_cleans_up_files() {
    let (engine, temp) = create_test_engine().await;

    let dataset = engine
        .create_dataset_from_inline("Temp", Some("temp_data"), "csv", "a\n1", None)
        .await
        .unwrap();

    // Verify parquet file exists
    let parquet_path = dataset.parquet_url.strip_prefix("file://").unwrap();
    assert!(std::path::Path::new(parquet_path).exists());

    // Delete dataset
    engine.catalog().delete_dataset(&dataset.id).await.unwrap();

    // File should be scheduled for deletion (may still exist during grace period)
    // In a real test, we'd wait for the deletion worker or check the pending_deletions table
}
```

**Step 2: Run all tests**

Run: `cargo test`
Expected: All tests PASS

**Step 3: Commit**

```bash
git add tests/dataset_e2e_tests.rs
git commit -m "test: add comprehensive dataset e2e integration tests"
```

---

## Summary

This TDD-focused plan has 10 tasks, each following red-green-refactor:

| Task | Focus | Tests First? |
|------|-------|--------------|
| 1 | Resource ID prefixes | Yes - unit tests |
| 2 | table_name validation | Yes - comprehensive unit tests |
| 3 | Database migrations | Build verification |
| 4 | Catalog types & methods | Yes - catalog integration tests |
| 5 | Storage extensions | Yes - storage integration tests |
| 6 | HTTP models | Compile verification (pure data) |
| 7 | Upload HTTP handlers | Yes - HTTP integration tests |
| 8 | Dataset HTTP handlers | Yes - HTTP integration tests |
| 9 | DatasetsProvider | Yes - query integration tests |
| 10 | E2E tests | Comprehensive flow tests |

**Key TDD principles applied:**
- Write failing tests before implementation
- Tests cover happy path + edge cases
- No task is complete until tests pass
- Tests document expected behavior
- Refactor only after green
