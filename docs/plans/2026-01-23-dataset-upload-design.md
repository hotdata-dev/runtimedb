# Dataset Upload Design

Add a `datasets` abstraction for user-curated data not tied to external connections. First implementation supports direct file upload (CSV, JSON, Parquet) with future extensibility for remote URLs, query results, etc.

## Overview

**Query path:** `SELECT * FROM datasets.{table_name}`

**Two-phase creation:**
1. `POST /v1/files` - Upload raw bytes, get ID
2. `POST /v1/datasets` - Create dataset from upload (with schema config)

**Plus inline shortcut:** `POST /v1/datasets` with `inline` source for small payloads.

## Storage Layout

**Pending uploads:**
```
{cache_base}/uploads/{upld_id}/raw
```
- Raw bytes stored as-is
- Same storage backend as datasets (local or S3)
- Tracked in catalog with `pending` status

**Created datasets:**
```
{cache_base}/datasets/{data_id}/{version}/data.parquet
```
- Always Parquet (converted from source format)
- Versioned directories for future refresh/append support
- Uses existing `StreamingParquetWriter` settings (Parquet 2.0, Zstd)

## Catalog Schema

**New `uploads` table:**
```sql
CREATE TABLE uploads (
  id TEXT PRIMARY KEY,           -- upld_abc123
  status TEXT NOT NULL,          -- 'pending' | 'consumed'
  storage_url TEXT NOT NULL,
  content_type TEXT,
  content_encoding TEXT,
  size_bytes BIGINT NOT NULL,
  created_at TIMESTAMP NOT NULL,
  consumed_at TIMESTAMP           -- set when status becomes 'consumed'
);
```

**New `datasets` table:**
```sql
CREATE TABLE datasets (
  id TEXT PRIMARY KEY,           -- data_abc123
  label TEXT NOT NULL,
  schema_name TEXT NOT NULL DEFAULT 'default',
  table_name TEXT NOT NULL,
  parquet_url TEXT NOT NULL,
  arrow_schema_json TEXT NOT NULL,
  source_type TEXT NOT NULL,     -- 'upload' | 'url' | 'query' (future)
  source_config TEXT NOT NULL,   -- JSON blob
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  UNIQUE(schema_name, table_name)
);
```

Note: `schema_name` defaults to `'default'` and is not user-configurable in v1. This prepares for future multi-schema support.

`source_config` for uploads:
```json
{
  "upload_id": "upld_abc123",
  "original_format": "csv",
  "schema_overrides": { ... }
}
```

## Resource IDs

New prefixes in `id.rs`:
- `upld_` for uploads
- `data_` for datasets

## API Endpoints

### Upload File

```
POST /v1/files
Content-Type: text/csv (optional hint)
Content-Encoding: gzip (optional)
Body: <binary data>

Response 201:
{
  "id": "upld_abc123",
  "status": "pending",
  "size_bytes": 1048576
}
```

Size limit: 2GB (configurable).

### List Pending Uploads

```
GET /v1/files

Response 200:
{
  "uploads": [
    {
      "id": "upld_abc123",
      "status": "pending",
      "size_bytes": 1048576,
      "content_type": "text/csv",
      "created_at": "2026-01-23T10:00:00Z",
      "consumed_at": null
    }
  ]
}
```

### Create Dataset from Upload

```
POST /v1/datasets
{
  "label": "Q1 Sales Report",
  "table_name": "q1_sales",        // optional, auto-generated from label if omitted
  "source": {
    "upload_id": "upld_abc123",
    "format": "csv",               // optional if content_type was provided
    "options": {
      "delimiter": ",",
      "header": true,
      "null_values": ["", "NULL"]
    }
  },
  "schema": {                      // optional, inferred if omitted
    "columns": [
      { "name": "revenue", "type": "decimal(10,2)" }
    ]
  }
}

Response 201:
{
  "id": "data_xyz789",
  "label": "Q1 Sales Report",
  "table_name": "q1_sales",
  "status": "ready"
}
```

### Create Dataset with Inline Data

```
POST /v1/datasets
{
  "label": "Country Codes",
  "source": {
    "inline": {
      "format": "csv",
      "content": "code,name\nUS,United States\nCA,Canada"
    }
  }
}

Response 201:
{
  "id": "data_xyz789",
  "label": "Country Codes",
  "table_name": "country_codes",
  "status": "ready"
}
```

**Inline size limit:** 1MB max. Returns 400 if exceeded.

### List Datasets

```
GET /v1/datasets

Response 200:
{
  "datasets": [
    {
      "id": "data_xyz789",
      "label": "Q1 Sales Report",
      "table_name": "q1_sales",
      "source_type": "upload",
      "created_at": "2026-01-23T10:00:00Z",
      "updated_at": "2026-01-23T10:00:00Z"
    }
  ]
}
```

### Get Dataset

```
GET /v1/datasets/{id}

Response 200:
{
  "id": "data_xyz789",
  "label": "Q1 Sales Report",
  "table_name": "q1_sales",
  "parquet_url": "s3://bucket/datasets/data_xyz789/v1/data.parquet",
  "schema": { ... },
  "source_type": "upload",
  "source_config": { ... },
  "created_at": "2026-01-23T10:00:00Z",
  "updated_at": "2026-01-23T10:00:00Z"
}
```

### Update Dataset Metadata

```
PUT /v1/datasets/{id}
{
  "label": "Q1 Sales Report (Final)",
  "table_name": "quarterly_sales"
}

Response 200:
{
  "id": "data_xyz789",
  "label": "Q1 Sales Report (Final)",
  "table_name": "quarterly_sales",
  ...
}
```

### Delete Dataset

```
DELETE /v1/datasets/{id}

Response 204
```

Deletes catalog entry and schedules Parquet files for deletion (using existing grace period mechanism).

## Validation

### table_name validation (when provided):
- Starts with letter or underscore
- Contains only letters, numbers, underscores
- Not a SQL reserved word
- Max 128 characters
- Unique across datasets

### Format resolution:
1. Explicit `format` in request
2. `content_type` hint from upload
3. Error if neither

### Upload consumption:
- Upload ID can only be used once
- After dataset creation, upload status becomes `consumed`
- Attempting to reuse returns 400 error

## Conversion Pipeline

When `POST /v1/datasets` is called:

1. **Resolve format** - Explicit `format` > `content_type` from upload > error
2. **Load raw bytes** - Stream from `storage_url` (handles S3 or local)
3. **Decompress if needed** - Based on `content_encoding`
4. **Infer schema** - Parse first 10,000 rows, build Arrow schema
5. **Apply overrides** - User-specified column types replace inferred ones
6. **Stream convert** - Parse full file, write to Parquet via `StreamingParquetWriter`
7. **Update catalog** - Mark upload as `consumed` (set `consumed_at`), insert dataset record
8. **Register with DataFusion** - Dataset becomes queryable

For Parquet uploads: rewrite with our settings (compression, encoding). Schema overrides return error: "altering parquet schema is currently unsupported".

## DataFusion Integration

**New `DatasetsProvider`** implements `CatalogProvider`:
- Registers as the `datasets` catalog
- Single implicit schema (extensible to multiple later)
- Lists/loads tables from the `datasets` catalog table
- Returns `ListingTable` pointing to dataset's Parquet URL

**Registration:** At engine startup, register `datasets` catalog. Lazy-load individual tables on first query.

**Query path:** `SELECT * FROM datasets.q1_sales`

## Future Extensibility

The `source_type` + `source_config` pattern supports future sources:

**Remote URL:**
```json
{
  "source_type": "url",
  "source_config": {
    "url": "https://example.com/data.csv",
    "format": "csv",
    "refresh_schedule": "0 0 * * *"
  }
}
```

**Query result:**
```json
{
  "source_type": "query",
  "source_config": {
    "sql": "SELECT * FROM conn.schema.table WHERE ...",
    "refresh_schedule": "0 */6 * * *"
  }
}
```

## Out of Scope

- Append to existing dataset
- Resumable uploads
- Pending upload cleanup (tracking only, cleanup deferred)
- Parquet schema modification
- Custom catalogs (only `datasets` for now)
- User-configurable schema_name (hardcoded to `default`)

## Future Considerations

- **Type promotion rules**: When schema inference encounters conflicting types across rows (e.g., integers then floats), define promotion hierarchy
- **Configurable sampling**: Allow users to specify sample size or sampling strategy for schema inference
- **Large file sampling**: For files > N rows, consider sampling from multiple locations rather than just first 10,000 rows

## Acceptance Criteria

- [ ] `POST /v1/files` accepts binary data and returns an ID
- [ ] Uploads tracked in catalog with pending status
- [ ] `GET /v1/files` returns list of pending uploads
- [ ] `POST /v1/datasets` creates dataset from upload ID
- [ ] `POST /v1/datasets` supports inline data for CSV/JSON
- [ ] Upload ID is consumed (cannot be reused) after dataset creation
- [ ] CSV/JSON uploads converted to Parquet on dataset creation
- [ ] Parquet uploads rewritten with standard settings
- [ ] Schema inference works for CSV/JSON
- [ ] Schema overrides work for CSV/JSON
- [ ] Parquet schema modification returns error
- [ ] `table_name` validation enforced
- [ ] `table_name` auto-generated from `label` when omitted
- [ ] `GET /v1/datasets` lists all datasets
- [ ] `GET /v1/datasets/{id}` returns dataset details
- [ ] `PUT /v1/datasets/{id}` updates label and table_name
- [ ] `DELETE /v1/datasets/{id}` removes dataset and files
- [ ] Datasets queryable via `SELECT * FROM datasets.{table_name}`
- [ ] Uploads exceeding size limit (default 2GB) rejected
- [ ] Format resolution: explicit > content_type hint > error
