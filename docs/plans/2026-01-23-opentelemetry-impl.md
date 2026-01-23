# OpenTelemetry Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add OpenTelemetry distributed tracing to RuntimeDB with OTLP export.

**Architecture:** Telemetry module provides `init_telemetry()` called at startup. Engine-level spans with `#[tracing::instrument]` propagate to all interfaces. HTTP middleware extracts trace context from headers.

**Tech Stack:** opentelemetry 0.31, tracing-opentelemetry 0.32, tower-http 0.6.8

---

## Task 1: Add Dependencies

**Files:**
- Modify: `Cargo.toml`

**Step 1: Add OpenTelemetry dependencies**

Add these dependencies to `Cargo.toml` after line 34 (`tracing-subscriber`):

```toml
opentelemetry = "0.31"
opentelemetry_sdk = { version = "0.31", features = ["rt-tokio"] }
opentelemetry-otlp = { version = "0.31", features = ["tonic"] }
tracing-opentelemetry = "0.32"
tower-http = { version = "0.6.8", features = ["trace"] }
```

**Step 2: Run cargo check**

Run: `cargo check`
Expected: Compiles with new dependencies downloaded

**Step 3: Commit**

```bash
git add Cargo.toml Cargo.lock
git commit -m "feat(telemetry): add OpenTelemetry dependencies"
```

---

## Task 2: Create Telemetry Module

**Files:**
- Create: `src/telemetry.rs`
- Modify: `src/lib.rs`

**Step 1: Create telemetry.rs with init and shutdown functions**

Create `src/telemetry.rs`:

```rust
//! OpenTelemetry telemetry initialization.
//!
//! Provides `init_telemetry()` for tracing setup and `shutdown_telemetry()` for cleanup.
//! When `OTEL_EXPORTER_OTLP_ENDPOINT` is set, traces are exported via OTLP.
//! Otherwise, only console logging is enabled.

use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use std::sync::OnceLock;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

static TRACER_PROVIDER: OnceLock<SdkTracerProvider> = OnceLock::new();

/// Initialize telemetry with optional OTLP export.
///
/// Checks `OTEL_EXPORTER_OTLP_ENDPOINT` to determine if OTLP export is enabled.
/// Sets up tracing-subscriber with both console output and OpenTelemetry layer.
///
/// # Environment Variables
/// - `OTEL_EXPORTER_OTLP_ENDPOINT`: OTLP endpoint URL (enables export when set)
/// - `OTEL_SERVICE_NAME`: Service name in traces (default: "runtimedb")
/// - `OTEL_TRACES_SAMPLER`: Sampler type (default: "traceidratio")
/// - `OTEL_TRACES_SAMPLER_ARG`: Sampling ratio 0.0-1.0 (default: "1.0")
/// - `OTEL_RUNTIMEDB_INCLUDE_SQL`: Include SQL in spans (default: "true")
pub fn init_telemetry() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Set global propagator for W3C Trace Context
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    let env_filter = EnvFilter::from_default_env()
        .add_directive(tracing::Level::INFO.into());

    let fmt_layer = tracing_subscriber::fmt::layer();

    if let Ok(endpoint) = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
        // OTLP export enabled
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(&endpoint)
            .build()?;

        let tracer_provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .build();

        // Store provider for shutdown
        let _ = TRACER_PROVIDER.set(tracer_provider.clone());

        let tracer = tracer_provider.tracer("runtimedb");
        let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .with(otel_layer)
            .init();

        tracing::info!(endpoint = %endpoint, "OpenTelemetry OTLP export enabled");
    } else {
        // Console-only logging
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .init();
    }

    Ok(())
}

/// Shutdown telemetry and flush pending spans.
///
/// Should be called during graceful shutdown to ensure all spans are exported.
pub fn shutdown_telemetry() {
    if let Some(provider) = TRACER_PROVIDER.get() {
        if let Err(e) = provider.shutdown() {
            eprintln!("Error shutting down tracer provider: {:?}", e);
        }
    }
}

/// Check if SQL should be included in trace attributes.
///
/// Controlled by `OTEL_RUNTIMEDB_INCLUDE_SQL` env var (default: true).
pub fn include_sql_in_traces() -> bool {
    std::env::var("OTEL_RUNTIMEDB_INCLUDE_SQL")
        .map(|v| v != "false" && v != "0")
        .unwrap_or(true)
}
```

**Step 2: Add telemetry module to lib.rs**

Add after line 9 (`pub mod storage;`) in `src/lib.rs`:

```rust
pub mod telemetry;
```

**Step 3: Run cargo check**

Run: `cargo check`
Expected: Compiles successfully

**Step 4: Commit**

```bash
git add src/telemetry.rs src/lib.rs
git commit -m "feat(telemetry): add telemetry module with init/shutdown"
```

---

## Task 3: Integrate Telemetry in Server

**Files:**
- Modify: `src/bin/server.rs`

**Step 1: Replace tracing_subscriber init with telemetry module**

Replace lines 18-24 in `src/bin/server.rs`:

```rust
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();
```

With:

```rust
    // Initialize telemetry (tracing + optional OpenTelemetry)
    runtimedb::telemetry::init_telemetry()
        .expect("Failed to initialize telemetry");
```

**Step 2: Add shutdown_telemetry call before Ok(())**

Before line 64 (`Ok(())`) add:

```rust
    // Flush pending telemetry spans
    runtimedb::telemetry::shutdown_telemetry();
```

**Step 3: Run cargo check**

Run: `cargo check`
Expected: Compiles successfully

**Step 4: Commit**

```bash
git add src/bin/server.rs
git commit -m "feat(telemetry): integrate telemetry init/shutdown in server"
```

---

## Task 4: Add HTTP Tracing Middleware

**Files:**
- Modify: `src/http/app_server.rs`

**Step 1: Add tower-http TraceLayer to router**

Add imports at top of `src/http/app_server.rs`:

```rust
use tower_http::trace::TraceLayer;
```

**Step 2: Add TraceLayer to router chain**

Modify the router creation in `AppServer::new()`. After `.with_state(engine.clone())` on line 65, add `.layer(TraceLayer::new_for_http())`:

Replace:
```rust
                .with_state(engine.clone()),
```

With:
```rust
                .with_state(engine.clone())
                .layer(TraceLayer::new_for_http()),
```

**Step 3: Run cargo check**

Run: `cargo check`
Expected: Compiles successfully

**Step 4: Commit**

```bash
git add src/http/app_server.rs
git commit -m "feat(telemetry): add HTTP tracing middleware"
```

---

## Task 5: Instrument Engine - execute_query

**Files:**
- Modify: `src/engine.rs`

**Step 1: Replace log imports with tracing**

Replace line 22 in `src/engine.rs`:
```rust
use log::{info, warn};
```

With:
```rust
use tracing::{info, warn};
```

**Step 2: Add instrument attribute to execute_query**

Add before `pub async fn execute_query` (line 355):

```rust
    #[tracing::instrument(
        name = "execute_query",
        skip(self, sql),
        fields(
            runtimedb.sql = tracing::field::Empty,
            runtimedb.rows_returned = tracing::field::Empty,
        )
    )]
```

**Step 3: Record SQL conditionally and rows at end**

Inside `execute_query`, after `let start = Instant::now();` (line 358), add:

```rust
        if crate::telemetry::include_sql_in_traces() {
            tracing::Span::current().record("runtimedb.sql", sql);
        }
```

Before the `Ok(QueryResponse { ... })` return (around line 371), add:

```rust
        let row_count: usize = results.iter().map(|b| b.num_rows()).sum();
        tracing::Span::current().record("runtimedb.rows_returned", row_count);
```

**Step 4: Run cargo check**

Run: `cargo check`
Expected: Compiles successfully

**Step 5: Commit**

```bash
git add src/engine.rs
git commit -m "feat(telemetry): instrument execute_query with tracing"
```

---

## Task 6: Instrument Engine - refresh_table_data

**Files:**
- Modify: `src/engine.rs`

**Step 1: Add instrument attribute to refresh_table_data**

Add before `pub async fn refresh_table_data` (line 748):

```rust
    #[tracing::instrument(
        name = "refresh_table",
        skip(self),
        fields(
            runtimedb.connection_id = %connection_id,
            runtimedb.schema = %schema_name,
            runtimedb.table = %table_name,
            runtimedb.rows_synced = tracing::field::Empty,
            runtimedb.warnings_count = tracing::field::Empty,
        )
    )]
```

**Step 2: Record fields before return**

Before the `Ok(TableRefreshResult { ... })` return (around line 787), add:

```rust
        tracing::Span::current().record("runtimedb.rows_synced", rows_synced);
        tracing::Span::current().record("runtimedb.warnings_count", warnings.len());
```

**Step 3: Run cargo check**

Run: `cargo check`
Expected: Compiles successfully

**Step 4: Commit**

```bash
git add src/engine.rs
git commit -m "feat(telemetry): instrument refresh_table_data with tracing"
```

---

## Task 7: Instrument Engine - refresh_connection_data

**Files:**
- Modify: `src/engine.rs`

**Step 1: Add instrument attribute to refresh_connection_data**

Add before `pub async fn refresh_connection_data` (line 801):

```rust
    #[tracing::instrument(
        name = "refresh_connection",
        skip(self),
        fields(
            runtimedb.connection_id = %connection_id,
            runtimedb.parallelism = self.parallel_refresh_count,
            runtimedb.tables_refreshed = tracing::field::Empty,
            runtimedb.tables_failed = tracing::field::Empty,
            runtimedb.rows_synced = tracing::field::Empty,
        )
    )]
```

**Step 2: Record fields before return**

Before the final `Ok(result)` return (around line 894), add:

```rust
        tracing::Span::current().record("runtimedb.tables_refreshed", result.tables_refreshed);
        tracing::Span::current().record("runtimedb.tables_failed", result.tables_failed);
        tracing::Span::current().record("runtimedb.rows_synced", result.total_rows);
```

**Step 3: Run cargo check**

Run: `cargo check`
Expected: Compiles successfully

**Step 4: Commit**

```bash
git add src/engine.rs
git commit -m "feat(telemetry): instrument refresh_connection_data with tracing"
```

---

## Task 8: Instrument Orchestrator

**Files:**
- Modify: `src/datafetch/orchestrator.rs`

**Step 1: Add tracing import**

Add at top of file after other imports:

```rust
use tracing;
```

**Step 2: Add instrument to cache_table**

Add before `pub async fn cache_table` (line 39):

```rust
    #[tracing::instrument(
        name = "cache_table",
        skip(self, source),
        fields(
            runtimedb.connection_id = connection_id,
            runtimedb.schema = %schema_name,
            runtimedb.table = %table_name,
            runtimedb.rows_written = tracing::field::Empty,
            runtimedb.cache_url = tracing::field::Empty,
        )
    )]
```

**Step 3: Record fields in cache_table before return**

Before the `Ok((parquet_url, row_count))` return (line 100), add:

```rust
        tracing::Span::current().record("runtimedb.rows_written", row_count);
        tracing::Span::current().record("runtimedb.cache_url", &parquet_url);
```

**Step 4: Add instrument to refresh_table**

Add before `pub async fn refresh_table` (line 120):

```rust
    #[tracing::instrument(
        name = "orchestrator_refresh_table",
        skip(self, source),
        fields(
            runtimedb.connection_id = connection_id,
            runtimedb.schema = %schema_name,
            runtimedb.table = %table_name,
            runtimedb.rows_synced = tracing::field::Empty,
            runtimedb.cache_url = tracing::field::Empty,
        )
    )]
```

**Step 5: Record fields in refresh_table before return**

Before the `Ok((new_url, old_path, row_count))` return (line 193), add:

```rust
        tracing::Span::current().record("runtimedb.rows_synced", row_count);
        tracing::Span::current().record("runtimedb.cache_url", &new_url);
```

**Step 6: Run cargo check**

Run: `cargo check`
Expected: Compiles successfully

**Step 7: Commit**

```bash
git add src/datafetch/orchestrator.rs
git commit -m "feat(telemetry): instrument FetchOrchestrator with tracing"
```

---

## Task 9: Instrument Native Fetcher Dispatch

**Files:**
- Modify: `src/datafetch/native/mod.rs`

**Step 1: Add instrument to discover_tables**

Add before `async fn discover_tables` in NativeFetcher impl (line 160):

```rust
    #[tracing::instrument(
        name = "discover_tables",
        skip(self, source, secrets),
        fields(
            runtimedb.backend = %source.source_type(),
            runtimedb.tables_found = tracing::field::Empty,
        )
    )]
```

**Step 2: Wrap discover_tables to record result**

Replace the body of `discover_tables` (lines 164-173):

```rust
        let tables = match source {
            Source::Duckdb { .. } | Source::Motherduck { .. } => {
                duckdb::discover_tables(source, secrets).await
            }
            Source::Postgres { .. } => postgres::discover_tables(source, secrets).await,
            Source::Iceberg { .. } => iceberg::discover_tables(source, secrets).await,
            Source::Mysql { .. } => mysql::discover_tables(source, secrets).await,
            Source::Snowflake { .. } => snowflake::discover_tables(source, secrets).await,
        }?;
        tracing::Span::current().record("runtimedb.tables_found", tables.len());
        Ok(tables)
```

**Step 3: Add instrument to fetch_table**

Add before `async fn fetch_table` in NativeFetcher impl (line 176):

```rust
    #[tracing::instrument(
        name = "fetch_table",
        skip(self, source, secrets, writer),
        fields(
            runtimedb.backend = %source.source_type(),
            runtimedb.schema = %schema,
            runtimedb.table = %table,
        )
    )]
```

**Step 4: Run cargo check**

Run: `cargo check`
Expected: Compiles successfully

**Step 5: Commit**

```bash
git add src/datafetch/native/mod.rs
git commit -m "feat(telemetry): instrument NativeFetcher with tracing"
```

---

## Task 10: Instrument Storage - Filesystem

**Files:**
- Modify: `src/storage/filesystem.rs`

**Step 1: Add tracing import**

Add at top of file:

```rust
use tracing;
```

**Step 2: Add instrument to prepare_cache_write**

Add before `fn prepare_cache_write` (line 103):

```rust
    #[tracing::instrument(
        name = "prepare_cache_write",
        skip(self),
        fields(
            runtimedb.backend = "filesystem",
            runtimedb.connection_id = connection_id,
            runtimedb.schema = %schema,
            runtimedb.table = %table,
        )
    )]
```

**Step 3: Add instrument to finalize_cache_write**

Add before `async fn finalize_cache_write` (line 132):

```rust
    #[tracing::instrument(
        name = "finalize_cache_write",
        skip(self),
        fields(
            runtimedb.backend = "filesystem",
            runtimedb.cache_url = tracing::field::Empty,
        )
    )]
```

**Step 4: Record cache_url before return in finalize_cache_write**

Before the `Ok(format!(...))` return (line 141), add:

```rust
        let url = format!("file://{}", version_dir.display());
        tracing::Span::current().record("runtimedb.cache_url", &url);
```

And change the return to:

```rust
        Ok(url)
```

**Step 5: Add instrument to delete_prefix**

Add before `async fn delete_prefix` (line 81):

```rust
    #[tracing::instrument(
        name = "delete_cache",
        skip(self),
        fields(
            runtimedb.backend = "filesystem",
            runtimedb.prefix = %prefix,
        )
    )]
```

**Step 6: Run cargo check**

Run: `cargo check`
Expected: Compiles successfully

**Step 7: Commit**

```bash
git add src/storage/filesystem.rs
git commit -m "feat(telemetry): instrument FilesystemStorage with tracing"
```

---

## Task 11: Instrument Storage - S3

**Files:**
- Modify: `src/storage/s3.rs`

**Step 1: Read s3.rs to understand structure**

Read the file first to find the method locations.

**Step 2: Add tracing import**

Add at top of file after other imports.

**Step 3: Add instrument to prepare_cache_write**

Add before `fn prepare_cache_write`:

```rust
    #[tracing::instrument(
        name = "prepare_cache_write",
        skip(self),
        fields(
            runtimedb.backend = "s3",
            runtimedb.connection_id = connection_id,
            runtimedb.schema = %schema,
            runtimedb.table = %table,
            runtimedb.bucket = %self.bucket,
        )
    )]
```

**Step 4: Add instrument to finalize_cache_write**

Add before `async fn finalize_cache_write`:

```rust
    #[tracing::instrument(
        name = "finalize_cache_write",
        skip(self),
        fields(
            runtimedb.backend = "s3",
            runtimedb.bucket = %self.bucket,
            runtimedb.key = tracing::field::Empty,
            runtimedb.cache_url = tracing::field::Empty,
        )
    )]
```

Record key and cache_url before return.

**Step 5: Add instrument to delete_prefix**

Add before `async fn delete_prefix`:

```rust
    #[tracing::instrument(
        name = "delete_cache",
        skip(self),
        fields(
            runtimedb.backend = "s3",
            runtimedb.bucket = %self.bucket,
            runtimedb.prefix = %prefix,
        )
    )]
```

**Step 6: Run cargo check**

Run: `cargo check`
Expected: Compiles successfully

**Step 7: Commit**

```bash
git add src/storage/s3.rs
git commit -m "feat(telemetry): instrument S3Storage with tracing"
```

---

## Task 12: Add Docker Compose for Local Testing

**Files:**
- Create: `docker-compose.otel.yml`

**Step 1: Create docker-compose.otel.yml**

```yaml
services:
  jaeger:
    image: jaegertracing/all-in-one:latest
    ports:
      - "16686:16686"  # Web UI
      - "4317:4317"    # OTLP gRPC
    environment:
      - COLLECTOR_OTLP_ENABLED=true
```

**Step 2: Commit**

```bash
git add docker-compose.otel.yml
git commit -m "chore: add Jaeger docker-compose for local telemetry testing"
```

---

## Task 13: Run Tests

**Files:** None (verification only)

**Step 1: Run all tests**

Run: `cargo test`
Expected: All tests pass

**Step 2: Run cargo clippy**

Run: `cargo clippy -- -D warnings`
Expected: No warnings

---

## Task 14: Manual Verification with Jaeger

**Step 1: Start Jaeger**

Run: `docker compose -f docker-compose.otel.yml up -d`

**Step 2: Start server with OTLP enabled**

Run: `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 cargo run --bin server -- config/dev.toml`

**Step 3: Make a test request**

Run: `curl -X POST http://localhost:3000/query -H 'Content-Type: application/json' -d '{"sql": "SELECT 1"}'`

**Step 4: Verify traces in Jaeger UI**

Open: http://localhost:16686
Expected: See "runtimedb" service with `execute_query` span containing `runtimedb.sql` and `runtimedb.rows_returned` attributes

**Step 5: Stop Jaeger**

Run: `docker compose -f docker-compose.otel.yml down`

---

## Task 15: Final Commit and PR

**Step 1: Verify all changes are committed**

Run: `git status`
Expected: Clean working tree

**Step 2: Create PR**

```bash
gh pr create --title "feat: add OpenTelemetry instrumentation" --body "$(cat <<'EOF'
## Summary
- Adds OpenTelemetry distributed tracing with OTLP export
- Instruments engine, datafetch, and storage layers
- Enables trace context propagation for distributed tracing

## Test plan
- [x] All existing tests pass
- [x] Manual verification with Jaeger shows spans with correct attributes

Closes #76
EOF
)"
```
