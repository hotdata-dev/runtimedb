# OpenTelemetry Instrumentation Design

## Overview

Add OpenTelemetry tracing to RuntimeDB for distributed observability. Instrumentation lives at the engine level so all interfaces (HTTP, future gRPC, CLI) benefit automatically.

## Dependencies

```toml
opentelemetry = "0.31"
opentelemetry_sdk = { version = "0.31", features = ["rt-tokio"] }
opentelemetry-otlp = { version = "0.31", features = ["tonic"] }
tracing-opentelemetry = "0.32"
tower-http = { version = "0.6.8", features = ["trace"] }
```

## Configuration

Environment variables (OpenTelemetry standard naming):

| Variable | Description | Default |
|----------|-------------|---------|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP endpoint URL. Enables export when set. | None (disabled) |
| `OTEL_SERVICE_NAME` | Service name in traces | `runtimedb` |
| `OTEL_TRACES_SAMPLER` | Sampler type | `traceidratio` |
| `OTEL_TRACES_SAMPLER_ARG` | Sampling ratio (0.0-1.0) | `1.0` |

RuntimeDB-specific:

| Variable | Description | Default |
|----------|-------------|---------|
| `OTEL_RUNTIMEDB_INCLUDE_SQL` | Include SQL in `runtimedb.sql` attribute | `true` |

**Note:** SQL may contain sensitive data. Set `OTEL_RUNTIMEDB_INCLUDE_SQL=false` to disable.

## Trace Context Propagation

W3C Trace Context (`traceparent`/`tracestate` headers).

`init_telemetry()` sets the global text map propagator to `TraceContextPropagator`. The HTTP layer uses `axum-tracing-opentelemetry` or custom middleware to extract the incoming `traceparent` header and set it as the parent context on the request span.

## Attribute Reference

Custom attributes use `runtimedb.*` prefix:

| Attribute | Description |
|-----------|-------------|
| `runtimedb.sql` | Query text (when `OTEL_RUNTIMEDB_INCLUDE_SQL=true`) |
| `runtimedb.connection_id` | Connection identifier |
| `runtimedb.schema` | Schema name |
| `runtimedb.table` | Table name |
| `runtimedb.backend` | Backend type (postgres, mysql, duckdb, iceberg, snowflake, filesystem, s3) |
| `runtimedb.rows_synced` | Rows synced during refresh |
| `runtimedb.rows_fetched` | Rows fetched from source |
| `runtimedb.rows_returned` | Rows returned from query |
| `runtimedb.rows_written` | Rows written to cache |
| `runtimedb.parallelism` | Parallel refresh count |
| `runtimedb.cache_url` | Cache file URL/path |
| `runtimedb.prefix` | Cache prefix for deletion |
| `runtimedb.file_size_bytes` | Written file size |
| `runtimedb.tables_found` | Tables discovered |
| `runtimedb.tables_refreshed` | Tables successfully refreshed |
| `runtimedb.tables_failed` | Tables that failed refresh |
| `runtimedb.warnings_count` | Warning count |
| `runtimedb.result_persisted` | Whether query result was persisted |
| `runtimedb.bucket` | S3 bucket name |
| `runtimedb.key` | S3 object key |

Standard OTel attributes used where applicable: `http.method`, `http.route`, `http.status_code`, `otel.status_code`, `error.message`.

## New Files

### `src/telemetry.rs`

```rust
pub fn init_telemetry() -> Result<(), Box<dyn std::error::Error>>
```

- Checks `OTEL_EXPORTER_OTLP_ENDPOINT`
- If set: configures OTLP exporter + OpenTelemetry layer + fmt layer
- If not set: fmt layer only (current behavior)
- Sets global propagator to `TraceContextPropagator` for W3C trace context

```rust
pub fn shutdown_telemetry()
```

- Flushes pending spans, shuts down tracer provider

### `docker-compose.otel.yml`

Jaeger all-in-one for local testing (see Local Testing section).

## Modified Files

### `src/bin/server.rs`

Call `init_telemetry()` at startup, `shutdown_telemetry()` on graceful shutdown.

### `src/engine.rs`

| Method | Span Name | Attributes | Recorded After |
|--------|-----------|------------|----------------|
| `execute_query()` | `execute_query` | `runtimedb.sql` (if enabled) | `runtimedb.rows_returned`, `runtimedb.result_persisted` |
| `refresh_table_data()` | `refresh_table` | `runtimedb.connection_id`, `runtimedb.schema`, `runtimedb.table` | `runtimedb.rows_synced`, `runtimedb.warnings_count` |
| `refresh_connection_data()` | `refresh_connection` | `runtimedb.connection_id`, `runtimedb.parallelism` | `runtimedb.tables_refreshed`, `runtimedb.tables_failed`, `runtimedb.rows_synced` |

Migrate all `log::` macros to `tracing::`.

### `src/datafetch/orchestrator.rs`

| Method | Span Name | Attributes | Recorded After |
|--------|-----------|------------|----------------|
| `cache_table()` | `cache_table` | `runtimedb.connection_id`, `runtimedb.schema`, `runtimedb.table` | `runtimedb.rows_written`, `runtimedb.cache_url` |
| `refresh_table()` | `orchestrator_refresh_table` | `runtimedb.connection_id`, `runtimedb.schema`, `runtimedb.table` | `runtimedb.rows_synced`, `runtimedb.cache_url` |

### `src/datafetch/native/*.rs`

| Method | Span Name | Attributes | Recorded After |
|--------|-----------|------------|----------------|
| `discover_tables()` | `discover_tables` | `runtimedb.backend`, `runtimedb.connection_id` | `runtimedb.tables_found` |
| `fetch_table()` | `fetch_table` | `runtimedb.backend`, `runtimedb.connection_id`, `runtimedb.schema`, `runtimedb.table` | `runtimedb.rows_fetched` |

Files: `mod.rs`, `postgres.rs`, `mysql.rs`, `duckdb.rs`, `iceberg.rs`, `snowflake.rs`

### `src/storage/filesystem.rs` and `src/storage/s3.rs`

| Method | Span Name | Attributes | Recorded After |
|--------|-----------|------------|----------------|
| `prepare_cache_write()` | `prepare_cache_write` | `runtimedb.backend`, `runtimedb.connection_id`, `runtimedb.schema`, `runtimedb.table` | - |
| `finalize_cache_write()` | `finalize_cache_write` | `runtimedb.backend`, `runtimedb.cache_url` | `runtimedb.file_size_bytes` |
| `delete_prefix()` | `delete_cache` | `runtimedb.backend`, `runtimedb.prefix` | - |

S3 backend also records `runtimedb.bucket`, `runtimedb.key`.

### `src/http/app_server.rs`

Add tracing middleware that:
- Creates root span with `http.method`, `http.route`, `http.status_code`
- Extracts `traceparent` header and sets parent context
- Engine spans become children of request span

## Local Testing

**`docker-compose.otel.yml`** in project root:

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

**Usage:**

```bash
docker compose -f docker-compose.otel.yml up -d
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
cargo run --bin server
# Hit endpoints, then view traces at http://localhost:16686
```

## Design Decisions

1. **Traces only, no dedicated metrics** - Most platforms derive metrics from traces
2. **High-level spans only** - No sub-spans for parse/plan/execute; use attributes instead
3. **Engine-level instrumentation** - All interfaces benefit; HTTP is just one consumer
4. **Optional OTLP** - Gracefully degrades to console-only when endpoint not configured
5. **W3C Trace Context** - Standard propagation format
6. **SQL opt-out** - SQL included by default for debugging value; can be disabled for sensitive environments
