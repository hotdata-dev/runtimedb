# Enhanced Query Execution Tracing

## Problem

Users report "slow queries" but current tracing doesn't provide enough visibility to diagnose where time is spent. The `execute_query` span (827ms in example traces) is a black box - we can't see whether slowness comes from SQL parsing, DataFusion execution, cache misses, remote database fetches, or storage transfers.

## Goal

Instrument every phase where time could be spent during query execution so we can answer:
- Is the query hitting cache or fetching from remote?
- How long does the remote database query take?
- How much time is spent on storage transfers (S3 upload/download)?
- Where exactly in the pipeline is the bottleneck?

## Design

### 1. Execute Query Path (engine.rs)

New spans inside `execute_query`:

| Span | Description | Attributes |
|------|-------------|------------|
| `sql_to_dataframe` | SQL parsing + logical planning | - |
| `collect_results` | Physical execution + result collection | `runtimedb.batches_collected` |

### 2. Lazy Table Provider (lazy_table_provider.rs)

New spans for table scan operations:

| Span | Description | Attributes |
|------|-------------|------------|
| `lazy_table_scan` | Per-table scan with cache decision | `runtimedb.connection_id`, `runtimedb.schema`, `runtimedb.table`, `runtimedb.cache_hit` |
| `load_parquet_exec` | Build parquet execution plan | `runtimedb.parquet_url` |

### 3. Catalog Operations (sqlite_manager.rs, postgres_manager.rs)

Comprehensive instrumentation of CatalogManager methods:

**High-priority (query path):**

| Span | Method | Attributes |
|------|--------|------------|
| `catalog_get_table` | `get_table()` | `runtimedb.connection_id`, `runtimedb.schema`, `runtimedb.table` |
| `catalog_update_table_sync` | `update_table_sync()` | `runtimedb.table_id` |
| `catalog_store_result` | `store_result()` | `runtimedb.result_id` |
| `catalog_get_result` | `get_result()` | `runtimedb.result_id` |

**Medium-priority (connection/schema management):**

| Span | Method | Attributes |
|------|--------|------------|
| `catalog_list_connections` | `list_connections()` | `runtimedb.count` |
| `catalog_get_connection` | `get_connection()` | `runtimedb.connection_id` |
| `catalog_add_table` | `add_table()` | `runtimedb.connection_id`, `runtimedb.schema`, `runtimedb.table` |
| `catalog_list_tables` | `list_tables()` | `runtimedb.connection_id`, `runtimedb.count` |

**Lower-priority (dataset/upload ops):**

| Span | Method | Attributes |
|------|--------|------------|
| `catalog_create_dataset` | `create_dataset()` | `runtimedb.dataset_id` |
| `catalog_get_dataset` | `get_dataset()` | `runtimedb.dataset_id` |
| `catalog_list_datasets` | `list_datasets()` | `runtimedb.count` |

### 4. Data Fetcher (native/mod.rs)

Enhance existing `fetch_table` span with additional attributes:

| Existing Span | New Attributes |
|---------------|----------------|
| `fetch_table` | `runtimedb.rows_fetched`, `runtimedb.batches_written` |

This instruments at the abstraction level. Backend-specific instrumentation (postgres, mysql, etc.) can be added later if needed.

### 5. Parquet Writer (parquet_writer.rs)

| Span | Description | Attributes |
|------|-------------|------------|
| `close_parquet_writer` | Finalize parquet file | `runtimedb.total_rows`, `runtimedb.file_size_bytes` |

### 6. Storage Operations (s3.rs, filesystem.rs)

| Span | Description | Attributes |
|------|-------------|------------|
| `s3_upload` | Upload to S3 | `runtimedb.bytes_uploaded`, `runtimedb.destination_url` |
| `s3_download` | Download from S3 | `runtimedb.bytes_downloaded`, `runtimedb.source_url` |

Enhance existing `finalize_cache_write` with `runtimedb.bytes_transferred`.

## Expected Trace Structure

After implementation, a query with a cache miss will show:

```
execute_query (827ms)
├── sql_to_dataframe (15ms)
└── collect_results (812ms)
    └── lazy_table_scan [cache_hit=false] (810ms)
        ├── catalog_get_table (2ms)
        └── fetch_and_cache (807ms)
            ├── fetch_table [backend=postgres, rows=50000, batches=5] (600ms)
            ├── close_parquet_writer [bytes=12MB] (7ms)
            └── finalize_cache_write [bytes=12MB] (200ms)
                └── s3_upload (198ms)
```

A cache hit will show:

```
execute_query (50ms)
├── sql_to_dataframe (15ms)
└── collect_results (35ms)
    └── lazy_table_scan [cache_hit=true] (30ms)
        ├── catalog_get_table (2ms)
        └── load_parquet_exec (28ms)
```

## Implementation Order

1. **Phase 1 - Query path visibility (HIGH):**
   - `sql_to_dataframe` and `collect_results` in engine.rs
   - `lazy_table_scan` and `load_parquet_exec` in lazy_table_provider.rs
   - `catalog_get_table` in catalog managers

2. **Phase 2 - Fetch pipeline (HIGH):**
   - Enhance `fetch_table` with rows/batches attributes
   - `close_parquet_writer` with file size
   - Storage transfer metrics (s3_upload, s3_download, bytes_transferred)

3. **Phase 3 - Catalog completeness (MEDIUM):**
   - Remaining catalog operations (list_connections, add_table, etc.)

## Conventions

- All custom attributes use `runtimedb.*` namespace
- Span names use `snake_case`
- Catalog spans prefixed with `catalog_`
- Use `tracing::field::Empty` for attributes recorded dynamically
- Use `Span::current().record()` for metrics calculated during execution
