# Connection Secret Refactor

## Problem

When deleting a connection, we want to delete the associated secret if no other connections use it. The current model stores credential references inside `config_json` at varying paths depending on source type (e.g., `$.credential.name` for Postgres, `$.catalog_type.credential.name` for Iceberg). This makes it difficult to query for secret usage at the database level.

## Solution

Store secrets with resource IDs (prefix `secr`) and reference them by ID from the `connections` table. The `config_json` field stores the auth mechanism type, but the actual secret reference lives in a dedicated column.

## Design Principles

1. **All secrets are opaque strings** - No structured secret types (like AWS credentials). Complex auth requiring multiple values should use separate secrets or JSON-encoded strings handled by the caller.
2. **Auth type is always explicit** - When using `secret_name` in the API, an `auth` field is required. Only inline `password`/`token` fields can infer auth type.
3. **Resource IDs for internal references** - Secrets have user-facing names (unique, for human use) and resource IDs (for internal references). Connections store the secret's ID, not its name.
4. **Transactional secret cleanup** - Delete operations use database transactions to prevent race conditions.

## Schema Change

```sql
-- Secrets table with resource ID
CREATE TABLE secrets (
    id TEXT PRIMARY KEY,           -- Resource ID: secr_...
    name TEXT UNIQUE NOT NULL,     -- User-facing name
    provider TEXT NOT NULL,
    provider_ref TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

ALTER TABLE connections ADD COLUMN secret_id TEXT REFERENCES secrets(id);
CREATE INDEX idx_connections_secret_id ON connections(secret_id);
```

## Data Model

### `connections` table

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Internal ID |
| external_id | TEXT | User-facing ID (conn_...) |
| name | TEXT | Connection name |
| source_type | TEXT | postgres, mysql, snowflake, etc. |
| config_json | TEXT | Source config with auth type, but no secret ref |
| secret_id | TEXT | Resource ID of the secret (nullable, FK to secrets.id) |

### `config_json` structure

Contains source-specific configuration including an `auth` field that describes how the secret is used:

**Postgres:**
```json
{
  "type": "postgres",
  "host": "localhost",
  "port": 5432,
  "user": "postgres",
  "database": "mydb",
  "auth": "password"
}
```

**Snowflake (password):**
```json
{
  "type": "snowflake",
  "account": "xyz123",
  "user": "bob",
  "warehouse": "COMPUTE_WH",
  "database": "PROD",
  "auth": "password"
}
```

**Snowflake (future - token/SSO):**
```json
{
  "type": "snowflake",
  "account": "xyz123",
  "warehouse": "COMPUTE_WH",
  "database": "PROD",
  "auth": "oauth"
}
```

**Motherduck:**
```json
{
  "type": "motherduck",
  "database": "my_db",
  "auth": "token"
}
```

**DuckDB (no auth):**
```json
{
  "type": "duckdb",
  "path": "/path/to/db.duckdb"
}
```

**Iceberg REST:**
```json
{
  "type": "iceberg",
  "catalog_type": {
    "type": "rest",
    "uri": "https://catalog.example.com",
    "auth": "bearer_token"
  },
  "warehouse": "s3://bucket/path"
}
```

**Iceberg Glue (uses environment/IAM credentials, no secret needed):**
```json
{
  "type": "iceberg",
  "catalog_type": {
    "type": "glue",
    "region": "us-east-1"
  },
  "warehouse": "s3://bucket/path"
}
```

> **Note on AWS credentials:** Glue auth relies on IAM roles or environment variables (AWS_ACCESS_KEY_ID, etc.), not stored secrets. If we later need explicit AWS credential support, we'll add it as a JSON-encoded secret string.

## API Changes

### Create Connection Request

Users can provide credentials in three ways:

**1. Inline password (auto-creates secret):**
```json
{
  "name": "mydb",
  "source_type": "postgres",
  "host": "localhost",
  "port": 5432,
  "user": "postgres",
  "database": "mydb",
  "password": "secret123"
}
```

**2. Inline token (auto-creates secret):**
```json
{
  "name": "my-motherduck",
  "source_type": "motherduck",
  "database": "analytics",
  "token": "md_abc123..."
}
```

**3. Reference existing secret (requires explicit auth):**
```json
{
  "name": "mydb",
  "source_type": "postgres",
  "host": "localhost",
  "port": 5432,
  "user": "postgres",
  "database": "mydb",
  "secret_name": "my-existing-secret",
  "auth": "password"
}
```

### Auth type inference

| Field provided | Inferred `auth` value | Notes |
|----------------|----------------------|-------|
| `password` | `"password"` | Unambiguous |
| `token` | `"token"` for Motherduck, `"bearer_token"` for Iceberg REST | Source-specific |
| `secret_name` only | **Error** | Must provide explicit `auth` field |
| `secret_name` + `auth` | Uses provided `auth` | Explicit, no inference |

## Source Enum Changes

Remove `Credential` from `Source` variants. Add `auth` field to each variant that supports authentication.

### Before

```rust
pub enum Credential {
    None,
    SecretRef { name: String },
}

pub enum Source {
    Postgres {
        host: String,
        port: u16,
        user: String,
        database: String,
        credential: Credential,
    },
    // ...
}
```

### After

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AuthType {
    Password,
    Token,
    BearerToken,
}

pub enum Source {
    Postgres {
        host: String,
        port: u16,
        user: String,
        database: String,
        auth: AuthType,
    },
    Motherduck {
        database: String,
        auth: AuthType,
    },
    Duckdb {
        path: String,
        // no auth field - doesn't support authentication
    },
    Iceberg {
        catalog_type: IcebergCatalogType,
        warehouse: String,
        namespace: Option<String>,
        // auth lives inside catalog_type for Iceberg
    },
    // ...
}

pub enum IcebergCatalogType {
    Rest {
        uri: String,
        auth: AuthType,  // bearer_token
    },
    Glue {
        region: String,
        // no auth - uses IAM/environment
    },
}
```

## Credential Resolution

Secrets are always opaque strings. The `auth` field tells the code how to interpret and apply the secret value.

```rust
impl Source {
    /// Returns the auth type for this source, if any
    pub fn auth_type(&self) -> Option<&AuthType>;
}
```

Resolution in `Engine`:
1. Fetch connection (includes `secret_id` and `config_json`)
2. Parse `Source` from `config_json` to get `auth_type()`
3. If `secret_id` is Some, resolve via `SecretManager::get_by_id()` → returns `Vec<u8>`
4. Convert to string and use according to auth type (e.g., set as password, add as Bearer header)

## Secret Cleanup on Connection Delete

Secret cleanup is **best-effort**. When a connection is deleted, we attempt to delete its associated secret if no other connections reference it. This path has low concurrency, so we keep the implementation simple without complex locking.

### Delete Flow

```sql
-- Step 1: Delete connection and related tables (by id)
DELETE FROM tables WHERE connection_id = ?;
DELETE FROM connections WHERE id = ?;

-- Step 2: If secret_id was set, count remaining references
SELECT COUNT(*) FROM connections WHERE secret_id = ?;
```

```
-- Step 3: AFTER DB operations, if count == 0, delete the secret
SecretManager.delete(secret_id)
```

### Failure Modes

| Failure Point | Result | Recovery |
|---------------|--------|----------|
| DB delete fails | Connection and secret both intact | Retry |
| Secret delete fails | Connection deleted, orphaned secret remains | Harmless; manual cleanup |
| Race: concurrent insert with same secret | Secret not deleted (count > 0) | Correct behavior |
| Race: concurrent delete of last two connections | Both see count=0, both try to delete secret | SecretManager.delete is idempotent |

### Pseudocode

```rust
/// Remove a connection by its external ID (user-facing ID from API path).
async fn remove_connection(&self, external_id: &str) -> Result<()> {
    // Resolve external_id to internal connection info
    let conn = self.catalog.get_connection_by_external_id(external_id).await?
        .ok_or_else(|| anyhow!("Connection not found"))?;

    let secret_id = conn.secret_id.clone();

    // Delete connection and related tables
    self.catalog.delete_connection(conn.id).await?;

    // Delete physical cache files (idempotent)
    self.delete_connection_files(conn.id).await?;

    // Best-effort secret cleanup
    if let Some(id) = secret_id {
        let ref_count = self.catalog.count_connections_using_secret(&id).await?;
        if ref_count == 0 {
            if let Err(e) = self.secret_manager.delete(&id).await {
                tracing::warn!("Failed to delete orphaned secret '{}': {}", id, e);
            }
        }
    }

    Ok(())
}
```

### Notes

- **No locking:** This path has low concurrency. The worst case is a harmless orphaned secret.
- **`delete_connection` handles table cleanup:** The catalog method deletes both the connection row and related table rows.

### Requirements

- **`SecretManager.delete` MUST be idempotent:** If two concurrent deletes both see count=0, both will attempt to delete. The method must return success (not error) when the secret does not exist.

## Files to Modify

1. `migrations/sqlite/v1.sql` - Add `secrets` table with `id`, add `secret_id` column to connections
2. `migrations/postgres/v1.sql` - Same as SQLite
3. `src/id.rs` - Add `Secret` resource ID type with prefix "secr"
4. `src/source.rs` - Remove `Credential`, add `AuthType`, update `Source` variants
5. `src/secrets/mod.rs` - Add `id` to `SecretMetadata`, update methods to return/use IDs
6. `src/catalog/backend.rs` - Update queries to include `secret_id`
7. `src/catalog/manager.rs` - Update `ConnectionInfo` struct to use `secret_id`
8. `src/engine.rs` - Update connection creation, add secret cleanup on delete
9. `src/http/handlers.rs` - Resolve `secret_name` (user-facing) to `secret_id` (internal)
10. Tests throughout
