# Redis Read-Through Metadata Cache

## Overview

Add an optional Redis caching layer for all `CatalogManager` metadata calls. Implemented as a `CachingCatalogManager` wrapper that sits in front of the existing SQLite/Postgres catalog backends. When Redis is not configured, the system behaves exactly as it does today.

## Architecture

### Component: `CachingCatalogManager`

A new struct implementing `CatalogManager` that wraps any inner `CatalogManager` implementation.

```
HTTP request / DataFusion query
  → CachingCatalogManager (check Redis)
    → hit:  return cached value
    → miss: call inner CatalogManager, cache result, return
```

**Holds:**
- `inner: Arc<dyn CatalogManager>` — the real SQLite/Postgres catalog
- `redis: redis::aio::ConnectionManager` — async Redis connection pool
- `config: CacheConfig`

**Wiring at startup:**

```rust
let catalog: Arc<dyn CatalogManager> = if let Some(redis_url) = &config.redis_url {
    let inner = build_catalog_manager(&config)?;
    Arc::new(CachingCatalogManager::new(inner, redis_url, cache_config).await?)
} else {
    build_catalog_manager(&config)?
};
```

## Cache Key Schema

All keys use the `rdb:` prefix (configurable).

```
{prefix}conn:list                              → Vec<ConnectionInfo>
{prefix}conn:{id}                              → ConnectionInfo
{prefix}conn:name:{name}                       → ConnectionInfo
{prefix}tbl:list:all                           → Vec<TableInfo>
{prefix}tbl:list:conn:{connection_id}          → Vec<TableInfo>
{prefix}tbl:{connection_id}:{schema}:{name}    → TableInfo
{prefix}tbl:names:{connection_id}              → Vec<String>
{prefix}lock:warmup                            → node_id (distributed lock)
```

### Key Segment Encoding

User-provided values (connection IDs, names, schema names, table names) may contain `:` or other characters that conflict with the key structure. All dynamic segments are URL-encoded before insertion:

```rust
fn encode_key_segment(s: &str) -> String {
    urlencoding::encode(s).into_owned()
}

fn decode_key_segment(s: &str) -> Result<String> {
    urlencoding::decode(s)
        .map(|s| s.into_owned())
        .map_err(|e| anyhow!("invalid key segment: {e}"))
}
```

Example: a table named `my:table` in schema `public` becomes key `{prefix}tbl:{conn_id}:public:my%3Atable`.

### Serialization

Each value is a JSON-serialized string stored via `SETEX` with the hard TTL:

```rust
struct CachedEntry<T: Serialize + DeserializeOwned> {
    data: T,
    cached_at: u64, // unix timestamp millis
}
```

## Read Path

Simple cache-aside:

```rust
async fn cached_read<T>(&self, key: &str, fetch: F) -> Result<T>
where
    T: Serialize + DeserializeOwned,
    F: FnOnce() -> Future<Output = Result<T>>,
{
    // Try to get from Redis
    match self.redis.get::<_, Option<String>>(key).await {
        Ok(Some(json)) => {
            // Deserialize cached entry
            match serde_json::from_str::<CachedEntry<T>>(&json) {
                Ok(entry) => return Ok(entry.data),
                Err(e) => {
                    // Corrupted cache entry — log and treat as miss
                    warn!("cache: failed to deserialize {key}: {e}");
                }
            }
        }
        Ok(None) => {
            // Cache miss — fall through to fetch
        }
        Err(e) => {
            // Redis unreachable — log and fall through to fetch
            warn!("cache: redis get failed for {key}: {e}");
        }
    }

    // Fetch from inner catalog
    let data = fetch().await?;

    // Cache the result (best-effort)
    if let Err(e) = self.cache_set(key, &data).await {
        warn!("cache: failed to set {key}: {e}");
    }

    Ok(data)
}

async fn cache_set<T: Serialize>(&self, key: &str, data: &T) -> Result<()> {
    let entry = CachedEntry {
        data,
        cached_at: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
    };
    let json = serde_json::to_string(&entry)?;
    self.redis
        .set_ex(key, json, self.config.hard_ttl_secs)
        .await
        .map_err(|e| anyhow!("redis set failed: {e}"))
}
```

**Key behaviors:**
- Redis unreachable → log warning, fall through to inner catalog, request succeeds
- Corrupted cache entry → log warning, treat as miss, re-fetch and re-cache
- Cache set failure → log warning, return fetched data anyway

## Write Path: Write-Through + Targeted Deletion

Every mutating method delegates to `inner`, then updates the cache. The rule: **write-through for data that still exists, DEL for data that was removed.**

All write-path keys use `{prefix}` from config and `encode_key_segment()` for dynamic values.

### `add_connection()`
- Re-fetch `inner.list_connections()` → write to `{prefix}conn:list`
- Write the new connection to `{prefix}conn:{encode(id)}`, `{prefix}conn:name:{encode(name)}`

### `delete_connection(id)`
- **Before calling inner**: fetch `inner.get_connection(id)` to get the connection's `name` (needed for cache key deletion)
- Call `inner.delete_connection(id)`
- Re-fetch `inner.list_connections()` → write to `{prefix}conn:list`
- `DEL` connection keys: `{prefix}conn:{encode(id)}`, `{prefix}conn:name:{encode(name)}` (using the name fetched above)
- Re-fetch `inner.list_tables(None)` → write to `{prefix}tbl:list:all`
- `DEL` connection-scoped keys: `{prefix}tbl:list:conn:{encode(id)}`, `{prefix}tbl:names:{encode(id)}`
- `SCAN` + `DEL` pattern `{prefix}tbl:{encode(id)}:*`

### `add_table()`
- Re-fetch `inner.get_table()` → write to `{prefix}tbl:{encode(conn_id)}:{encode(schema)}:{encode(name)}`
- Re-fetch `inner.list_tables(Some(conn_id))` → write to `{prefix}tbl:list:conn:{encode(conn_id)}`
- Re-fetch `inner.list_tables(None)` → write to `{prefix}tbl:list:all`
- Re-fetch `inner.list_dataset_table_names(conn_id)` → write to `{prefix}tbl:names:{encode(conn_id)}`

### `update_table_sync()`
- Re-fetch `inner.get_table()` → write to `{prefix}tbl:{encode(conn_id)}:{encode(schema)}:{encode(name)}`
- Re-fetch `inner.list_tables(Some(conn_id))` → write to `{prefix}tbl:list:conn:{encode(conn_id)}`
- Re-fetch `inner.list_tables(None)` → write to `{prefix}tbl:list:all`

### `clear_table_cache_metadata()` / `clear_connection_cache_metadata()`
Same write-through pattern for the affected scope.

### Failure Handling
If Redis is unreachable during write-through, log a warning and continue. The catalog DB is the source of truth. The hard TTL guarantees eventual consistency.

## Background Warmup Loop

An optional `tokio::spawn` task that proactively refreshes all cached metadata on a timer, keeping the cache warm even without user requests.

```rust
async fn cache_warmup_loop(
    inner: Arc<dyn CatalogManager>,
    redis: ConnectionManager,
    config: CacheConfig,
    node_id: String,
) {
    let prefix = &config.key_prefix;
    let interval = Duration::from_secs(config.warmup_interval_secs);

    loop {
        tokio::time::sleep(interval).await;

        // Distributed lock — only one node runs warmup
        let lock_key = format!("{prefix}lock:warmup");
        let acquired = redis
            .set_nx_ex(&lock_key, &node_id, config.warmup_lock_ttl_secs)
            .await
            .unwrap_or(false);
        if !acquired {
            continue;
        }

        // Channel to signal lock loss from heartbeat to warmup loop
        let (lock_lost_tx, mut lock_lost_rx) = tokio::sync::watch::channel(false);

        // Spawn heartbeat task to extend lock while warmup runs.
        // Uses a Lua script to atomically check value before extending (fencing).
        let heartbeat_redis = redis.clone();
        let heartbeat_key = lock_key.clone();
        let heartbeat_ttl = config.warmup_lock_ttl_secs;
        let heartbeat_node_id = node_id.clone();
        let heartbeat_handle = tokio::spawn(async move {
            // Lua script: only extend TTL if we still own the lock
            let extend_script = r#"
                if redis.call("GET", KEYS[1]) == ARGV[1] then
                    return redis.call("EXPIRE", KEYS[1], ARGV[2])
                else
                    return 0
                end
            "#;
            loop {
                tokio::time::sleep(Duration::from_secs(heartbeat_ttl / 2)).await;
                let result: Result<i32, _> = heartbeat_redis
                    .eval(extend_script, &[&heartbeat_key], &[&heartbeat_node_id, &heartbeat_ttl.to_string()])
                    .await;
                match result {
                    Ok(1) => continue,  // Successfully extended
                    Ok(_) | Err(_) => {
                        // Lock stolen, expired, or Redis error — signal loss and stop
                        warn!("cache warmup: lock lost or heartbeat failed, aborting warmup");
                        let _ = lock_lost_tx.send(true);
                        break;
                    }
                }
            }
        });

        // Helper macro to check lock status between phases
        macro_rules! check_lock {
            () => {
                if *lock_lost_rx.borrow() {
                    warn!("cache warmup: aborting due to lock loss");
                    heartbeat_handle.abort();
                    continue;
                }
            };
        }

        // Phase 1: Refresh connections
        let conns = match inner.list_connections().await {
            Ok(conns) => {
                check_lock!();
                if let Err(e) = cache_set(&format!("{prefix}conn:list"), &conns).await {
                    warn!("cache warmup: failed to set conn:list: {e}");
                }
                for conn in &conns {
                    check_lock!();
                    let id_key = format!("{prefix}conn:{}", encode_key_segment(&conn.id));
                    let name_key = format!("{prefix}conn:name:{}", encode_key_segment(&conn.name));
                    if let Err(e) = cache_set(&id_key, conn).await {
                        warn!("cache warmup: failed to set {id_key}: {e}");
                    }
                    if let Err(e) = cache_set(&name_key, conn).await {
                        warn!("cache warmup: failed to set {name_key}: {e}");
                    }
                }
                conns
            }
            Err(e) => {
                warn!("cache warmup: failed to list connections: {e}");
                heartbeat_handle.abort();
                continue;
            }
        };

        // Phase 2: Refresh tables — global list
        check_lock!();
        if let Ok(all_tables) = inner.list_tables(None).await {
            if let Err(e) = cache_set(&format!("{prefix}tbl:list:all"), &all_tables).await {
                warn!("cache warmup: failed to set tbl:list:all: {e}");
            }
        }

        // Phase 3: Refresh tables per connection
        for conn in &conns {
            check_lock!();
            let conn_id = encode_key_segment(&conn.id);
            if let Ok(tables) = inner.list_tables(Some(&conn.id)).await {
                let list_key = format!("{prefix}tbl:list:conn:{conn_id}");
                if let Err(e) = cache_set(&list_key, &tables).await {
                    warn!("cache warmup: failed to set {list_key}: {e}");
                }
                for table in &tables {
                    check_lock!();
                    let schema = encode_key_segment(&table.schema_name);
                    let name = encode_key_segment(&table.table_name);
                    let key = format!("{prefix}tbl:{conn_id}:{schema}:{name}");
                    if let Err(e) = cache_set(&key, table).await {
                        warn!("cache warmup: failed to set {key}: {e}");
                    }
                }
            }
            if let Ok(names) = inner.list_dataset_table_names(&conn.id).await {
                let names_key = format!("{prefix}tbl:names:{conn_id}");
                if let Err(e) = cache_set(&names_key, &names).await {
                    warn!("cache warmup: failed to set {names_key}: {e}");
                }
            }
        }

        heartbeat_handle.abort();
        // Lock released by TTL expiry
    }
}
```

### Distributed Lock

- Acquired with `SET NX EX` (atomic set-if-not-exists with TTL), storing `node_id` as the value
- Lock TTL (default 5 minutes) is extended via heartbeat while warmup runs
- **Heartbeat with fencing**: a background task runs a Lua script every `TTL/2` seconds that atomically checks if the lock value still matches `node_id` before extending. If another node has acquired the lock (value changed), the heartbeat signals lock loss and stops.
- **Lock loss cancellation**: the warmup loop checks a `watch` channel between phases (after connections, after global list, per-connection loop). If the heartbeat signals lock loss, the warmup aborts immediately — no further writes occur, preventing split-brain warmups.
- If the lock holder crashes, no more heartbeats are sent, and the TTL expires — another node picks up the next cycle
- Heartbeat is aborted when warmup completes (success or failure)

## Configuration

```rust
pub struct CacheConfig {
    /// Redis connection URL. None = caching disabled entirely.
    pub redis_url: Option<String>,

    /// Hard TTL — Redis key expiry. Default: 1800 (30 minutes).
    pub hard_ttl_secs: u64,

    /// Background warmup interval. Default: 0 (disabled).
    /// Set to e.g. 1500 (25 min) to keep cache warm proactively.
    pub warmup_interval_secs: u64,

    /// Distributed lock TTL for warmup dedup. Default: 300 (5 minutes).
    pub warmup_lock_ttl_secs: u64,

    /// Key prefix. Default: "rdb:"
    pub key_prefix: String,
}
```

Environment variables:

```
RUNTIMEDB_REDIS_URL=redis://localhost:6379
RUNTIMEDB_CACHE_HARD_TTL_SECS=1800
RUNTIMEDB_CACHE_WARMUP_INTERVAL_SECS=1500
RUNTIMEDB_CACHE_WARMUP_LOCK_TTL_SECS=300
RUNTIMEDB_CACHE_KEY_PREFIX=rdb:
```

### Validation

- `warmup_interval_secs` must be less than `hard_ttl_secs` (if both are set). Enforced at startup.
- If `redis_url` is not set, all other cache config is ignored.

## Testing Strategy

### Unit Tests

Wrap `MockCatalog` (already exists) with `CachingCatalogManager` using a real Redis instance (via `testcontainers` or similar):

- **Cold miss**: no cached value, inner called, result cached, returned
- **Cache hit**: value in Redis, inner not called, cached value returned
- **Write-through**: mutation calls inner, cache updated, subsequent read returns new data without calling inner
- **Hard TTL expiry**: key expires, next read is cold miss
- **Redis down**: operations succeed by falling through to inner, warnings logged
- **Warmup loop**: runs, populates all keys, subsequent reads are cache hits
- **Distributed lock**: only one of N concurrent loops acquires the lock
- **Warmup aborts on lock loss**: critical test for preventing split-brain (see below)

### Lock-Loss Abort Test

This test validates that the warmup loop correctly aborts when the lock is stolen mid-run:

**Setup:**
- Configure a short lock TTL (e.g., 2 seconds) to make the test fast
- Use a mock or shim for the Redis `EVAL` call that can be controlled to return `0` (lock lost) on demand
- Alternatively, use a real Redis and overwrite the lock key with a different `node_id` mid-warmup

**Test sequence:**
1. Start warmup with `MockCatalog` containing multiple connections and tables
2. Allow warmup to complete Phase 1 (connections) — verify `{prefix}conn:list` is written
3. Trigger lock loss: either inject `EVAL → 0` on next heartbeat tick, or `SET {prefix}lock:warmup "other_node"`
4. Wait for heartbeat to detect loss and signal via channel
5. Assert:
   - Warmup loop exits early (does not complete all phases)
   - `{prefix}tbl:list:all` is **not** written (or only partially written before abort)
   - Per-connection table keys (`{prefix}tbl:{conn_id}:*`) are **not** written after lock loss
   - Log contains "aborting due to lock loss" message
   - Heartbeat task has stopped

**Implementation notes:**
- For deterministic testing, add a test hook or trait that allows injecting a fake Redis response for the Lua extend script
- Example: `trait LockExtender { async fn extend(&self) -> Result<bool>; }` with a `MockLockExtender` that returns `Ok(false)` after N calls
- This avoids flaky timing-based tests

### Integration

Existing tests are unaffected — they run against unwrapped catalog managers. The `CachingCatalogManager` is additive and opt-in.

## Implementation Scope

### New Files
- `src/catalog/caching_manager.rs` — `CachingCatalogManager` struct + `CatalogManager` impl

### Modified Files
- `src/config/mod.rs` — add `CacheConfig` fields
- `src/engine.rs` — wrap catalog manager when Redis is configured
- `Cargo.toml` — add `redis` crate (with `tokio-comp` feature) behind an optional feature flag

### New Dependencies
- `redis` crate with async tokio support
- `testcontainers` (dev dependency) for Redis integration tests
