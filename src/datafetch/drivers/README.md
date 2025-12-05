# ADBC Driver Libraries

This directory contains vendored ADBC driver shared libraries.

## Required Drivers

| Driver | Source | Filename |
|--------|--------|----------|
| DuckDB | [DuckDB Releases](https://github.com/duckdb/duckdb/releases) | libduckdb.{dylib,so,dll} |
| PostgreSQL | [ADBC Releases](https://github.com/apache/arrow-adbc/releases) | libadbc_driver_postgresql.{dylib,so,dll} |
| Snowflake | [ADBC Releases](https://github.com/apache/arrow-adbc/releases) | libadbc_driver_snowflake.{dylib,so,dll} |

## Directory Structure

```
drivers/
├── macos-arm64/     # macOS Apple Silicon
├── macos-x64/       # macOS Intel
├── linux-x64/       # Linux x86_64
└── windows-x64/     # Windows x86_64
```

## Manual Installation

1. Download the appropriate driver for your platform
2. Place in the correct platform directory
3. The build will copy these to the output directory

## Environment Variable Override

You can also set driver paths via environment variables:
- `RIVETDB_DRIVER_DUCKDB`
- `RIVETDB_DRIVER_POSTGRESQL`
- `RIVETDB_DRIVER_SNOWFLAKE`