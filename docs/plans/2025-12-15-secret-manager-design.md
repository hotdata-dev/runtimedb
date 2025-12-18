# Secret Manager Design

## Overview

A pluggable secret manager that stores encrypted credentials in the catalog database. Connections reference secrets by name rather than containing plaintext credentials.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     SecretManager                        │
│  (trait: get, put, delete, list)                        │
└─────────────────┬───────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────┐
│              EncryptedSecretManager                      │
│  - Uses AES-256-GCM-SIV (misuse resistant)             │
│  - Key from RIVETDB_SECRET_KEY env var                  │
│  - Stores in encrypted_secret_values table              │
└─────────────────────────────────────────────────────────┘
```

### Data Flow

1. User creates secret via `POST /secrets` with name + plaintext value
2. SecretManager encrypts and stores in database
3. User creates connection with `secret_ref: "my-secret-name"`
4. At connection time, DataFetcher resolves secret to plaintext
5. Plaintext used only in memory for the database connection

## Secret Name Constraints

- Allowed characters: `[a-zA-Z0-9_-]`
- Length: 1-128 characters
- Case-insensitive: stored/compared as lowercase
- Validation location: SecretManager trait methods

Validation regex: `^[a-zA-Z0-9_-]{1,128}$`

## Database Schema

```sql
CREATE TABLE secrets (
    name TEXT PRIMARY KEY,           -- normalized lowercase
    provider TEXT NOT NULL,          -- e.g., "encrypted"
    provider_ref TEXT,               -- unused for encrypted, future-proofing
    status TEXT NOT NULL,            -- 'creating', 'active', 'pending_delete'
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE encrypted_secret_values (
    name TEXT PRIMARY KEY REFERENCES secrets(name) ON DELETE CASCADE,
    encrypted_value BLOB NOT NULL    -- version || key_version || nonce || ciphertext
);
```

### Secret Status Lifecycle

Secrets have a status field that tracks their lifecycle state:

```
┌──────────┐     backend      ┌──────────┐
│ Creating │ ───succeeds────► │  Active  │
└──────────┘                  └──────────┘
                                   │
                              delete called
                                   │
                                   ▼
                            ┌──────────────┐     backend      ┌─────────┐
                            │PendingDelete │ ───succeeds────► │ Deleted │
                            └──────────────┘                  └─────────┘
```

- **Creating**: Metadata inserted, backend write in progress. Used for optimistic locking during concurrent creates.
- **Active**: Secret is available for use. Only active secrets are visible to `GET` and `list` operations.
- **PendingDelete**: Marked for deletion, backend cleanup in progress. Invisible to reads but retryable for delete.

## Concurrency and Optimistic Locking

Secret creation uses optimistic locking to safely handle concurrent requests for the same name.

### The Problem

Without coordination, concurrent creates can corrupt state:
1. Request A writes value to backend
2. Request B writes value to backend (overwrites A)
3. Request A inserts metadata (wins)
4. Request B's metadata insert fails, triggers rollback
5. Rollback deletes the backend value—but that's now A's value
6. Result: metadata exists but backend value is gone

### The Solution

Use the database as a coordination point with three-phase create:

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  1. Claim Name  │────►│ 2. Write Backend│────►│  3. Activate    │
│ INSERT Creating │     │   store value   │     │ UPDATE → Active │
└─────────────────┘     └─────────────────┘     └─────────────────┘
        │                       │                       │
        ▼                       ▼                       ▼
   UNIQUE constraint        on failure:           optimistic lock:
   ensures only one         leave Creating        only if created_at
   request proceeds         (user deletes         matches our claim
                            to retry)
```

**Phase 1 - Claim**: Insert metadata with `status=Creating`. The UNIQUE constraint on `name` ensures only one request wins. Losers get `AlreadyExists` or `CreationInProgress` depending on the winner's status.

**Phase 2 - Backend Write**: Store the encrypted value. If this fails, the `Creating` record remains. The user sees `CreationInProgress` on retry and must `DELETE` to clear the stuck state.

**Phase 3 - Activate**: Update status to `Active` using an optimistic lock on `created_at`. This ensures that if another process deleted our `Creating` record and inserted a new one, we detect the change and fail safely (the orphaned backend value is harmless—subsequent creates overwrite it).

### OptimisticLock

```rust
/// Used to conditionally update a secret only if it hasn't been modified.
pub struct OptimisticLock {
    pub created_at: DateTime<Utc>,
}
```

The catalog's `update_secret_metadata` method accepts an optional lock:
- `None`: Unconditional update (used for normal updates)
- `Some(lock)`: Only update if `created_at` matches (used to finalize creation)

### Update Concurrency

Updates use last-write-wins semantics. This is acceptable because:
- Both requests are updating an existing, valid secret
- No rollback can corrupt state
- The final value is from a legitimate update request

### Delete Concurrency

Deletes use three-phase commit:
1. Set `status=PendingDelete` (secret becomes invisible)
2. Delete from backend
3. Delete metadata row

If step 2 fails, the secret remains in `PendingDelete`. Subsequent `DELETE` calls can retry (the manager looks up by any status). If step 3 fails, the secret is effectively deleted (value gone, create will clean up stale metadata).

### Encrypted Value Format

```
['R','V','S','1'][1 byte scheme][1 byte key_version][12 byte nonce][ciphertext...]
```

- Magic prefix `RVS1` (4 bytes) - identifies format, catches corruption early
- Encryption scheme `0x01` = AES-256-GCM-SIV
- Key version `0x01` = first master key
- Total header: 18 bytes before ciphertext
- Allows future scheme changes and key rotation without schema changes

### Encryption Details

- **Algorithm:** AES-256-GCM-SIV (nonce misuse resistant)
- **AAD (Associated Authenticated Data):** Secret name (normalized lowercase)
  - Binds ciphertext to the secret name
  - Prevents copying encrypted blobs between secrets

**Normalization Invariant:** All secret names MUST be normalized (lowercase) before:
- AAD construction
- Encryption
- Decryption
- Database lookup

This prevents "works on insert, fails on read" bugs from case mismatches.

## SecretManager Trait

```rust
#[async_trait]
pub trait SecretManager: Send + Sync {
    async fn get(&self, name: &str) -> Result<Vec<u8>>;
    async fn put(&self, name: &str, value: &[u8]) -> Result<()>;
    async fn delete(&self, name: &str) -> Result<()>;
    async fn list(&self) -> Result<Vec<SecretMetadata>>;

    // Convenience helper for string secrets
    async fn get_string(&self, name: &str) -> Result<String> {
        let bytes = self.get(name).await?;
        String::from_utf8(bytes).map_err(|_| /* invalid utf8 error */)
    }
}

pub enum SecretStatus {
    Creating,      // Backend write in progress
    Active,        // Available for use
    PendingDelete, // Deletion in progress
}

pub struct SecretMetadata {
    pub name: String,
    pub provider: String,
    pub provider_ref: Option<String>,
    pub status: SecretStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
```

## Source Refactoring

```rust
pub enum Source {
    Postgres {
        host: String,
        port: u16,
        user: String,
        database: String,
        credential_type: CredentialType,
        secret_ref: Option<String>,
    },
    Snowflake {
        account: String,
        user: String,
        warehouse: String,
        database: String,
        role: Option<String>,
        credential_type: CredentialType,
        secret_ref: Option<String>,
    },
    Motherduck {
        database: String,
        credential_type: CredentialType,
        secret_ref: Option<String>,
    },
    Duckdb { path: String },
}

pub enum CredentialType {
    Password,
    Token,
}
```

### Secret Resolution

```rust
impl Source {
    pub async fn resolve_credentials(
        &self,
        secret_manager: &dyn SecretManager
    ) -> Result<ResolvedSource>;
}
```

`ResolvedSource` is a parallel struct with plaintext fields, used only transiently.

## API Endpoints

```
POST   /secrets              - Create/update a secret
GET    /secrets              - List secrets (metadata only)
GET    /secrets/{name}       - Get secret metadata
DELETE /secrets/{name}       - Delete a secret
```

### Request/Response Models

```rust
// POST /secrets
pub struct CreateSecretRequest {
    pub name: String,
    pub value: String,  // UTF-8 string for v1, binary support future
}

pub struct CreateSecretResponse {
    pub name: String,  // normalized
    pub created_at: DateTime<Utc>,
}

// GET /secrets
pub struct ListSecretsResponse {
    pub secrets: Vec<SecretMetadata>,
}

// GET /secrets/{name}
pub struct GetSecretResponse {
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
```

Note: API accepts strings for v1. Trait uses bytes internally for future binary support.

### Error Cases

| Scenario | HTTP Status | Error |
|----------|-------------|-------|
| `RIVETDB_SECRET_KEY` not set | 503 | "Secret manager not configured" |
| Secret not found | 404 | "Secret 'x' not found" |
| Secret already exists | 409 | "Secret 'x' already exists" |
| Creation in progress | 409 | "Secret 'x' is being created by another process; delete it first if you want to retry" |
| Invalid secret name format | 400 | "Invalid secret name: must be..." |
| Decryption failure | 500 | "Failed to decrypt secret" |

## Configuration

Key provided via environment variable:

```bash
export RIVETDB_SECRET_KEY="base64-encoded-32-byte-key"
```

Requirements:
- Exactly 32 bytes (256 bits) for AES-256
- Base64 encoded
- If not set, secret endpoints return 503

Startup behavior:
- Engine logs whether secret manager is available
- No hard failure if key missing

### Future Key Rotation (not implemented in v1)

- Add `RIVETDB_SECRET_KEY_2`, `RIVETDB_SECRET_KEY_3`, etc.
- Decrypt with matching key version from blob
- Encrypt with latest key version
- Key version stored in encrypted blob format

## Files to Create

- `src/secrets/mod.rs` - SecretManager trait, SecretMetadata, CredentialType
- `src/secrets/encrypted.rs` - EncryptedSecretManager implementation
- `src/secrets/validation.rs` - Name validation logic

## Files to Modify

- `src/source.rs` - Add credential_type, secret_ref; add resolve_credentials()
- `src/catalog/migrations.rs` - Add secrets tables
- `src/http/handlers.rs` - New secret endpoints
- `src/http/models.rs` - Secret request/response types
- `src/http/app_server.rs` - Register secret routes
- `src/engine.rs` - Hold SecretManager, wire into connection flow
- `src/datafetch/` - Use resolved credentials

## Dependencies

- `aes-gcm-siv` - AES-256-GCM-SIV encryption
- `base64` - Key decoding

## Test Coverage

- Unit tests for encryption/decryption round-trip
- Unit tests for name validation
- Unit tests for AAD binding (can't decrypt with wrong name)
- Unit tests for stuck "creating" status detection and recovery
- Integration tests for secret CRUD API
- Integration tests for connection with secret reference