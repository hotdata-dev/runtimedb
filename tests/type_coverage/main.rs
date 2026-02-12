//! Type coverage integration tests.
//!
//! Tests comprehensive type mapping from source databases to Arrow types.
//!
//! Run with: cargo test --test type_coverage

mod capturing_writer;
mod fixtures;
mod harness;

// Backend-specific tests
mod bigquery_types;
mod duckdb_types;
mod mysql_types;
mod postgres_types;
