# Restore Credential Abstraction

## Problem

The current refactor threads `secret_id` through many layers (engine → orchestrator → fetcher → native implementations) and passes the resolved secret value to fetchers. This:

1. **Limits extensibility** - Can't easily support multi-credential auth (e.g., Snowflake keypair + passphrase)
2. **Breaks encapsulation** - Source no longer knows how to connect to itself
3. **Exposes secrets** - Resolved values passed through more code paths
4. **Creates churn** - Many files changed just to thread the ID

## Solution

Restore the `Credential` abstraction in `Source`, but use secret IDs instead of names for the internal reference. Keep the `secret_id` column on connections for DB queryability.

## Design

### Credential Enum (in source.rs)

```rust
/// Credential storage - either no credential or a reference to a stored secret.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Credential {
    #[default]
    None,
    SecretRef {
        /// Resource ID of the secret (secr_...)
        id: String,
    },
}

impl Credential {
    /// Resolve the credential to a plaintext string.
    pub async fn resolve(&self, secrets: &SecretManager) -> anyhow::Result<String> {
        match self {
            Credential::None => Err(anyhow::anyhow!("no credential configured")),
            Credential::SecretRef { id } => {
                secrets.get_string_by_id(id).await
                    .map_err(|e| anyhow::anyhow!("failed to resolve secret '{}': {}", id, e))
            }
        }
    }

    /// Get the secret ID if this is a SecretRef.
    pub fn secret_id(&self) -> Option<&str> {
        match self {
            Credential::None => None,
            Credential::SecretRef { id } => Some(id),
        }
    }
}
```

### Source Variants

```rust
pub enum Source {
    Postgres {
        host: String,
        port: u16,
        user: String,
        database: String,
        #[serde(default)]
        credential: Credential,
    },
    // ... other variants with credential field
}

impl Source {
    /// Get the secret ID from this source's credential, if any.
    pub fn secret_id(&self) -> Option<&str> {
        match self {
            Source::Postgres { credential, .. } => credential.secret_id(),
            Source::Snowflake { credential, .. } => credential.secret_id(),
            Source::Motherduck { credential, .. } => credential.secret_id(),
            Source::Duckdb { .. } => None,
            Source::Iceberg { catalog_type, .. } => match catalog_type {
                IcebergCatalogType::Rest { credential, .. } => credential.secret_id(),
                IcebergCatalogType::Glue { .. } => None,
            },
            Source::Mysql { credential, .. } => credential.secret_id(),
        }
    }
}
```

### DataFetcher Trait (restored)

```rust
#[async_trait]
pub trait DataFetcher: Send + Sync + std::fmt::Debug {
    async fn discover_tables(
        &self,
        source: &Source,
        secrets: &SecretManager,  // NOT resolved value
    ) -> Result<Vec<TableMetadata>, DataFetchError>;

    async fn fetch_table(
        &self,
        source: &Source,
        secrets: &SecretManager,  // NOT resolved value
        catalog: Option<&str>,
        schema: &str,
        table: &str,
        writer: &mut StreamingParquetWriter,
    ) -> Result<(), DataFetchError>;
}
```

### Connection Storage

The `connections` table keeps `secret_id` for queryability:

```sql
CREATE TABLE connections (
    id INTEGER PRIMARY KEY,
    external_id TEXT UNIQUE NOT NULL,
    name TEXT UNIQUE NOT NULL,
    source_type TEXT NOT NULL,
    config_json TEXT NOT NULL,  -- Contains Credential with id
    secret_id TEXT REFERENCES secrets(id),  -- Denormalized for queries
    created_at TIMESTAMP
);
```

When storing a connection:
1. Parse Source from config
2. Extract `source.secret_id()`
3. Store both `config_json` (with credential) and `secret_id` (for queries)

### HTTP Handler Flow

```rust
// 1. Create secret (if inline password/token)
let secret_id = if let Some(password) = request.password {
    Some(secret_manager.create(&auto_name, password.as_bytes()).await?)
} else if let Some(ref name) = request.secret_name {
    Some(secret_manager.get_metadata(name).await?.id)
} else {
    None
};

// 2. Build Source with Credential containing the ID
let credential = match &secret_id {
    Some(id) => Credential::SecretRef { id: id.clone() },
    None => Credential::None,
};
// Inject credential into config before deserializing to Source

// 3. Register connection (extracts secret_id from Source for DB column)
engine.register_connection(&request.name, source).await?;
```

### Engine Changes

```rust
pub async fn register_connection(&self, name: &str, source: Source) -> Result<i32> {
    let source_type = source.source_type();
    let secret_id = source.secret_id().map(|s| s.to_string());
    let config_json = serde_json::to_string(&source)?;

    // Store with secret_id for queryability
    self.catalog.add_connection(name, source_type, &config_json, secret_id.as_deref()).await
}
```

## Benefits

1. **Encapsulation** - Source knows its credential, fetchers resolve internally
2. **Extensibility** - Can add `KeypairWithPassphrase { key_id, passphrase_id }` to Credential
3. **Less threading** - No secret_id parameter through orchestrator/fetcher layers
4. **DB queryability** - `secret_id` column still exists for cleanup queries
5. **Cleaner fetchers** - Just call `credential.resolve(secrets).await`

## Files to Modify

1. `src/source.rs` - Restore Credential enum with `id` field
2. `src/datafetch/fetcher.rs` - Restore SecretManager parameter
3. `src/datafetch/native/*.rs` - Restore credential.resolve() pattern
4. `src/datafetch/orchestrator.rs` - Remove secret_id parameter
5. `src/engine.rs` - Extract secret_id from Source for DB storage
6. `src/http/handlers.rs` - Build Credential with resolved ID
7. `src/datafusion/*.rs` - Simplify, use Source's credential
8. Tests
