# Secret Manager Design

## Overview

RuntimeDB stores database credentials as managed secrets rather than embedding plaintext in connection configs. 
A `SecretManager` orchestrates validation, catalog metadata, and encryption, while pluggable `SecretBackend` 
implementations handle the actual byte storage. 

The backend implemented initially, encrypts values into the catalog database with AES-256-GCM-SIV. 
Future backends (Vault, KMS, etc.) only need to implement the backend trait, allowing us to reuse the same manager logic and HTTP API.

Connections reference secrets by name (e.g., `{ "credential": { "type": "secret_ref", "name": "prod-pg" } }`). 
Secret values never get persisted outside the dedicated storage layer and are resolved just-in-time when creating external connections.

## Architecture

```
┌────────────────────────────────────────────────────────┐
│                    SecretManager                       │
│  - Validates names                                     │
│  - Manages catalog metadata + lifecycle status         │
│  - Coordinates optimistic locking + retries            │
│  - Delegates byte storage to SecretBackend             │
└──────────────┬─────────────────────────────────────────┘
               │
               ▼
┌────────────────────────────────────────────────────────┐
│                 SecretBackend (trait)                  │
│  get/put/delete raw bytes using SecretRecord context   │
│  returns provider_ref + status info                    │
└──────────────┬─────────────────────────────────────────┘
               │
               ▼
┌────────────────────────────────────────────────────────┐
│          EncryptedCatalogBackend (v1 implementation)   │
│  - AES-256-GCM-SIV (nonce-misuse resistant)            │
│  - Key sourced from RUNTIMEDB_SECRET_KEY (base64)        │
│  - Stores ciphertext in encrypted_secret_values table  │
└────────────────────────────────────────────────────────┘
```

### Data Flow

1. Client calls `POST /secrets` with `name` + plaintext value (UTF-8 string for v1).
2. `SecretManager::create` validates the name, claims the metadata row (`status=creating`), and writes bytes through the backend.
3. Metadata transitions to `active` with timestamps and provider info.
4. Connection configs refer to the secret by name.
5. When a fetcher needs credentials it resolves `Credential::SecretRef` via `SecretManager::get` and only uses the plaintext in memory.

## Secret Name Constraints

- Allowed characters: `[a-zA-Z0-9_-]`
- Length: 1–128 characters
- Comparison: case-insensitive (names normalized to lowercase)
- Validation: centralized in `validate_and_normalize_name` before any catalog or backend call

Validation regex: `^[a-zA-Z0-9_-]{1,128}$`

## Database Schema

```sql
CREATE TABLE secrets (
    name TEXT PRIMARY KEY,          -- normalized lowercase
    provider TEXT NOT NULL,         -- e.g. "encrypted"
    provider_ref TEXT,
    status TEXT NOT NULL,           -- 'creating' | 'active' | 'pending_delete'
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE encrypted_secret_values (
    name TEXT PRIMARY KEY,
    encrypted_value BLOB NOT NULL
);
```

Metadata rows track lifecycle and provider info. The encrypted values table is deliberately separate so other backends can ignore it completely.

## Lifecycle & Concurrency

```
creating --(backend write succeeds)--> active --(delete requested)--> pending_delete --(cleanup)--> deleted
```

- **Creating**: Metadata row inserted to “claim” the name. Only one request can create a row because of the PK constraint.
- **Active**: Secret is usable and visible to read/list operations.
- **PendingDelete**: Delete in progress. Reads ignore this state, but delete calls continue to operate on it so retries work.

### Create

`SecretManager::create` uses optimistic locking:
1. Insert metadata row with `status=creating` (fails fast if name already active/creating).
2. Write via backend. On failure we leave the metadata row as `creating`; the user must delete to retry.
3. Promote to `active` with an `OptimisticLock` keyed by `created_at`. If another process deleted the row before promotion we fail with a database error, leaving at most an orphaned backend value that the next create overwrites.

### Update

`update` loads active metadata, writes via backend, and updates timestamps/provider_ref with last-write-wins semantics. Since the secret already exists we don’t need an optimistic lock; the final update simply reflects the latest successful request.

### Delete

Delete is a three-phase commit:
1. Set `status=pending_delete` (idempotent) so reads stop seeing the secret immediately.
2. Delete through the backend.
3. Remove metadata. If backend deletion fails we keep the row as `pending_delete` so the client can retry. If metadata deletion fails we log a warning; the value is already gone and a future create cleans up the stale row.

## SecretBackend Trait

```rust
#[async_trait]
pub trait SecretBackend: Debug + Send + Sync {
    async fn get(&self, record: &SecretRecord) -> Result<Option<BackendRead>, BackendError>;
    async fn put(&self, record: &SecretRecord, value: &[u8]) -> Result<BackendWrite, BackendError>;
    async fn delete(&self, record: &SecretRecord) -> Result<bool, BackendError>;
}

pub struct SecretRecord {
    pub name: String,
    pub provider_ref: Option<String>,
}
```

The manager passes the normalized name and any provider-specific reference (e.g., Vault path, KMS ARN). Backends return `BackendWrite` so the manager can persist refreshed `provider_ref` values and track whether the operation created or updated data.

## Encrypted Catalog Backend

The default backend encrypts plaintext values with AES-256-GCM-SIV using the normalized name as associated data (AAD). The encrypted blob layout:

```
[ 'R','V','S','1' ][ scheme ][ key_version ][ 12-byte nonce ][ ciphertext || tag ]
```

- Scheme `0x01` = AES-256-GCM-SIV
- Key version `0x01` for the current master key
- Nonce is randomly generated per secret write

All keys are provided via `RUNTIMEDB_SECRET_KEY` (base64-encoded 32-byte key). The builder fails immediately if the env var is missing or invalid. Additional key versions can be supported later without schema changes.

## Source & Credential Representation

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Credential {
    #[default]
    None,
    SecretRef { name: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Source {
    Postgres {
        host: String,
        port: u16,
        user: String,
        database: String,
        #[serde(default)]
        credential: Credential,
    },
    Snowflake {
        account: String,
        user: String,
        warehouse: String,
        database: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        role: Option<String>,
        #[serde(default)]
        credential: Credential,
    },
    Motherduck {
        database: String,
        #[serde(default)]
        credential: Credential,
    },
    Duckdb { path: String },
}
```

`Credential::resolve(&SecretManager)` either errors (`Credential::None`) or fetches the referenced secret, returning a UTF-8 `String`. Data fetchers call this right before building external connection strings so that invalid or missing secrets don’t block connection registration—discovery simply fails with a descriptive error until the secret exists.

## HTTP API

| Method | Path               | Description                                  |
|--------|--------------------|----------------------------------------------|
| POST   | `/secrets`         | Create a new secret                          |
| PUT    | `/secrets/{name}`  | Update an existing secret’s value            |
| GET    | `/secrets`         | List metadata for all active secrets         |
| GET    | `/secrets/{name}`  | Fetch metadata for a specific secret         |
| DELETE | `/secrets/{name}`  | Delete a secret (three-phase commit)         |

### Request/Response Models

```rust
pub struct CreateSecretRequest {
    pub name: String,
    pub value: String, // UTF-8 for v1
}

pub struct CreateSecretResponse {
    pub name: String,
    pub created_at: DateTime<Utc>,
}

pub struct UpdateSecretRequest {
    pub value: String,
}

pub struct UpdateSecretResponse {
    pub name: String,
    pub updated_at: DateTime<Utc>,
}

pub struct SecretMetadataResponse {
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
```

The API accepts UTF-8 strings today; internally the manager stores raw bytes so binary secrets can be added later without schema changes.

### Error Handling

| Scenario | Status | Message |
|----------|--------|---------|
| Secret manager disabled | 503 | `Secret manager not configured` |
| Invalid name | 400 | Includes validation hint |
| Secret not found | 404 | `Secret '{name}' not found` |
| Secret already exists | 409 | `Secret '{name}' already exists` |
| Creation in progress | 409 | `Secret '{name}' is being created by another process...` |
| Backend/storage failure | 500 | `Backend error: ...` |

Errors are surfaced via `SecretError -> ApiError`, so any new failure modes automatically produce consistent HTTP responses.

## Configuration

- `RUNTIMEDB_SECRET_KEY`: base64-encoded 32-byte key required to enable the encrypted backend.
- `RuntimeDBEngine::builder().secret_key(...)` enforces the presence of this key at startup; the server refuses to boot without it because every HTTP secret operation depends on the manager.

## Code Organization

- `src/secrets/mod.rs`: `SecretManager`, lifecycle management, validation, optimistic locking.
- `src/secrets/backend.rs`: `SecretBackend` trait plus helper structs (`SecretRecord`, `BackendWrite`, etc.).
- `src/secrets/encrypted_catalog_backend.rs`: AES-256-GCM-SIV backend implementation.
- `src/catalog/*_manager.rs`: catalog metadata operations, status transitions, and encrypted value storage helpers.
- `src/http/{handlers,models}.rs`: Secret CRUD endpoints and DTOs.
- `src/source.rs` & `src/datafetch`: credential references and runtime secret resolution.

## Test Coverage

- Unit tests for encryption/decryption round-trips, base64 key parsing, and decryption failure scenarios.
- Secret manager tests for create/update/delete/list flows, optimistic locking, name normalization, and invalid names.
- HTTP integration tests for all secret endpoints (including update and error cases).
- Engine HTTP tests that cover connection creation without upfront credentials and discover retries.

This final design matches the implementation on `feat/secret-manager`: the manager owns lifecycle + metadata, backends only store ciphertext, and the API exposes clear semantics for create/update/delete with robust concurrency guarantees.
