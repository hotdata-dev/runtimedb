// Backend modules - public for type mapping function access in tests
// The type mapping functions are the authoritative implementations that tests validate against.
pub mod duckdb;
pub mod iceberg;
pub mod mysql;
mod parquet_writer;
pub mod postgres;
pub mod snowflake;

pub use parquet_writer::StreamingParquetWriter;

use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;

/// Parse DECIMAL/NUMERIC(precision, scale) parameters from a type string.
///
/// This is the shared implementation used by PostgreSQL, MySQL, and DuckDB backends.
/// Each backend provides its own fallback for when parameters can't be parsed or are
/// out of range, since the default behavior differs by database:
/// - PostgreSQL: Unconstrained NUMERIC has arbitrary precision, falls back to Utf8
/// - MySQL: DECIMAL without params defaults to DECIMAL(10,0)
/// - DuckDB: Uses DECIMAL(38,10) as default
///
/// Returns `Some(DataType::Decimal128(p, s))` if valid parameters were parsed,
/// or `None` if the caller should use their fallback.
pub fn parse_decimal_params(type_str: &str) -> Option<DataType> {
    let start = type_str.find('(')?;
    let end = type_str.find(')')?;
    let params = &type_str[start + 1..end];
    let parts: Vec<&str> = params.split(',').map(|s| s.trim()).collect();

    match parts.len() {
        2 => {
            let precision = parts[0].parse::<u8>().ok()?;
            let scale = parts[1].parse::<i8>().ok()?;
            // Arrow Decimal128 supports precision 1-38
            if (1..=38).contains(&precision) && scale >= 0 && scale as u8 <= precision {
                Some(DataType::Decimal128(precision, scale))
            } else {
                None // Out of range, caller should use fallback
            }
        }
        1 => {
            // DECIMAL(p) with no scale defaults to scale 0
            let precision = parts[0].parse::<u8>().ok()?;
            if (1..=38).contains(&precision) {
                Some(DataType::Decimal128(precision, 0))
            } else {
                None // Out of range, caller should use fallback
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_decimal_with_precision_and_scale() {
        assert_eq!(
            parse_decimal_params("DECIMAL(10,2)"),
            Some(DataType::Decimal128(10, 2))
        );
        assert_eq!(
            parse_decimal_params("NUMERIC(20, 5)"),
            Some(DataType::Decimal128(20, 5))
        );
        assert_eq!(
            parse_decimal_params("decimal(38,10)"),
            Some(DataType::Decimal128(38, 10))
        );
    }

    #[test]
    fn test_parse_decimal_precision_only() {
        assert_eq!(
            parse_decimal_params("DECIMAL(10)"),
            Some(DataType::Decimal128(10, 0))
        );
        assert_eq!(
            parse_decimal_params("NUMERIC(38)"),
            Some(DataType::Decimal128(38, 0))
        );
    }

    #[test]
    fn test_parse_decimal_no_params() {
        assert_eq!(parse_decimal_params("DECIMAL"), None);
        assert_eq!(parse_decimal_params("NUMERIC"), None);
    }

    #[test]
    fn test_parse_decimal_out_of_range() {
        // Precision > 38 (Arrow limit)
        assert_eq!(parse_decimal_params("DECIMAL(39,2)"), None);
        assert_eq!(parse_decimal_params("DECIMAL(65,10)"), None);

        // Precision = 0 (invalid)
        assert_eq!(parse_decimal_params("DECIMAL(0,0)"), None);

        // Scale > precision (invalid)
        assert_eq!(parse_decimal_params("DECIMAL(5,10)"), None);

        // Negative scale (invalid for Arrow)
        assert_eq!(parse_decimal_params("DECIMAL(10,-2)"), None);
    }

    #[test]
    fn test_parse_decimal_max_valid_precision() {
        assert_eq!(
            parse_decimal_params("DECIMAL(38,0)"),
            Some(DataType::Decimal128(38, 0))
        );
        assert_eq!(
            parse_decimal_params("DECIMAL(38,38)"),
            Some(DataType::Decimal128(38, 38))
        );
    }

    #[test]
    fn test_parse_decimal_whitespace_handling() {
        assert_eq!(
            parse_decimal_params("DECIMAL( 10 , 2 )"),
            Some(DataType::Decimal128(10, 2))
        );
        assert_eq!(
            parse_decimal_params("DECIMAL(  20  )"),
            Some(DataType::Decimal128(20, 0))
        );
    }

    #[test]
    fn test_parse_decimal_invalid_format() {
        assert_eq!(parse_decimal_params("DECIMAL(abc)"), None);
        assert_eq!(parse_decimal_params("DECIMAL(10,abc)"), None);
        assert_eq!(parse_decimal_params("DECIMAL(10,2,3)"), None);
        assert_eq!(parse_decimal_params("DECIMAL()"), None);
    }
}

use crate::datafetch::batch_writer::BatchWriter;
use crate::datafetch::{DataFetchError, DataFetcher, TableMetadata};
use crate::secrets::SecretManager;
use crate::source::Source;

/// Native Rust driver-based data fetcher
#[derive(Debug, Default)]
pub struct NativeFetcher;

impl NativeFetcher {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl DataFetcher for NativeFetcher {
    async fn discover_tables(
        &self,
        source: &Source,
        secrets: &SecretManager,
    ) -> Result<Vec<TableMetadata>, DataFetchError> {
        match source {
            Source::Duckdb { .. } | Source::Motherduck { .. } => {
                duckdb::discover_tables(source, secrets).await
            }
            Source::Postgres { .. } => postgres::discover_tables(source, secrets).await,
            Source::Iceberg { .. } => iceberg::discover_tables(source, secrets).await,
            Source::Mysql { .. } => mysql::discover_tables(source, secrets).await,
            Source::Snowflake { .. } => snowflake::discover_tables(source, secrets).await,
        }
    }

    async fn fetch_table(
        &self,
        source: &Source,
        secrets: &SecretManager,
        catalog: Option<&str>,
        schema: &str,
        table: &str,
        writer: &mut dyn BatchWriter,
    ) -> Result<(), DataFetchError> {
        match source {
            Source::Duckdb { .. } | Source::Motherduck { .. } => {
                duckdb::fetch_table(source, secrets, catalog, schema, table, writer).await
            }
            Source::Postgres { .. } => {
                postgres::fetch_table(source, secrets, catalog, schema, table, writer).await
            }
            Source::Iceberg { .. } => {
                iceberg::fetch_table(source, secrets, catalog, schema, table, writer).await
            }
            Source::Mysql { .. } => {
                mysql::fetch_table(source, secrets, catalog, schema, table, writer).await
            }
            Source::Snowflake { .. } => {
                snowflake::fetch_table(source, secrets, catalog, schema, table, writer).await
            }
        }
    }
}
