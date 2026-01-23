//! DuckDB type coverage tests.
//!
//! Tests comprehensive type mapping from DuckDB types to Arrow types.
//! DuckDB runs in-process so no container is needed.
//!
//! IMPORTANT: This test validates actual Arrow values produced by fetch_table,
//! not text cast representations. Uses CapturingBatchWriter to capture Arrow
//! RecordBatches and validates both schema types and data values.

use arrow_schema::{DataType, TimeUnit};
use duckdb::Connection;
use std::sync::Arc;
use tempfile::TempDir;

// Production code imports
use runtimedb::catalog::{CatalogManager, SqliteCatalogManager};
use runtimedb::datafetch::{DataFetcher, NativeFetcher};
use runtimedb::secrets::{EncryptedCatalogBackend, SecretManager, ENCRYPTED_PROVIDER_TYPE};
use runtimedb::source::Source;

use crate::capturing_writer::CapturingBatchWriter;
use crate::fixtures::{Constraints, FixtureCategory};
use crate::harness::{
    get_val_column_type, validate_batch_values, ComparisonMode, ExpectedOutput, FailureReason,
    SemanticType, TestReport, TestShape, TestValue, TypeTestCase, TypeTestResult,
};

// ============================================================================
// Test Case Builder
// ============================================================================

/// Build test cases for DuckDB types using fixture data.
fn build_duckdb_test_cases() -> Vec<TypeTestCase> {
    let mut cases = Vec::new();

    // Boolean type
    if let Ok(booleans) = FixtureCategory::load("booleans.toml") {
        let constraints = Constraints::new().with_nullable(true);
        let filtered = booleans.select_compatible(&constraints);
        cases.push(TypeTestCase {
            db_type: "BOOLEAN".to_string(),
            semantic_type: SemanticType::Boolean,
            expected_arrow_type: DataType::Boolean,
            values: filtered.values.into_iter().map(TestValue::from).collect(),
            shape: TestShape::Scalar,
        });
    }

    // Integer types
    if let Ok(integers) = FixtureCategory::load("integers.toml") {
        // TINYINT: -128 to 127
        let tinyint_constraints = Constraints::new()
            .with_min_value("-128")
            .with_max_value("127")
            .with_signed(true)
            .with_nullable(true);
        let filtered = integers.select_compatible(&tinyint_constraints);
        cases.push(TypeTestCase {
            db_type: "TINYINT".to_string(),
            semantic_type: SemanticType::Integer {
                signed: true,
                bits: 8,
            },
            expected_arrow_type: DataType::Int8,
            values: filtered.values.into_iter().map(TestValue::from).collect(),
            shape: TestShape::Scalar,
        });

        // SMALLINT: -32768 to 32767
        let smallint_constraints = Constraints::new()
            .with_min_value("-32768")
            .with_max_value("32767")
            .with_signed(true)
            .with_nullable(true);
        let filtered = integers.select_compatible(&smallint_constraints);
        cases.push(TypeTestCase {
            db_type: "SMALLINT".to_string(),
            semantic_type: SemanticType::Integer {
                signed: true,
                bits: 16,
            },
            expected_arrow_type: DataType::Int16,
            values: filtered.values.into_iter().map(TestValue::from).collect(),
            shape: TestShape::Scalar,
        });

        // INTEGER: -2147483648 to 2147483647
        let int_constraints = Constraints::new()
            .with_min_value("-2147483648")
            .with_max_value("2147483647")
            .with_signed(true)
            .with_nullable(true);
        let filtered = integers.select_compatible(&int_constraints);
        cases.push(TypeTestCase {
            db_type: "INTEGER".to_string(),
            semantic_type: SemanticType::Integer {
                signed: true,
                bits: 32,
            },
            expected_arrow_type: DataType::Int32,
            values: filtered.values.into_iter().map(TestValue::from).collect(),
            shape: TestShape::Scalar,
        });

        // BIGINT: -9223372036854775808 to 9223372036854775807
        let bigint_constraints = Constraints::new()
            .with_min_value("-9223372036854775808")
            .with_max_value("9223372036854775807")
            .with_signed(true)
            .with_nullable(true);
        let filtered = integers.select_compatible(&bigint_constraints);
        cases.push(TypeTestCase {
            db_type: "BIGINT".to_string(),
            semantic_type: SemanticType::Integer {
                signed: true,
                bits: 64,
            },
            expected_arrow_type: DataType::Int64,
            values: filtered.values.into_iter().map(TestValue::from).collect(),
            shape: TestShape::Scalar,
        });

        // UTINYINT: 0 to 255
        let utinyint_constraints = Constraints::new()
            .with_min_value("0")
            .with_max_value("255")
            .with_signed(false)
            .with_nullable(true);
        let filtered = integers.select_compatible(&utinyint_constraints);
        cases.push(TypeTestCase {
            db_type: "UTINYINT".to_string(),
            semantic_type: SemanticType::Integer {
                signed: false,
                bits: 8,
            },
            expected_arrow_type: DataType::UInt8,
            values: filtered.values.into_iter().map(TestValue::from).collect(),
            shape: TestShape::Scalar,
        });

        // USMALLINT: 0 to 65535
        let usmallint_constraints = Constraints::new()
            .with_min_value("0")
            .with_max_value("65535")
            .with_signed(false)
            .with_nullable(true);
        let filtered = integers.select_compatible(&usmallint_constraints);
        cases.push(TypeTestCase {
            db_type: "USMALLINT".to_string(),
            semantic_type: SemanticType::Integer {
                signed: false,
                bits: 16,
            },
            expected_arrow_type: DataType::UInt16,
            values: filtered.values.into_iter().map(TestValue::from).collect(),
            shape: TestShape::Scalar,
        });

        // UINTEGER: 0 to 4294967295
        let uinteger_constraints = Constraints::new()
            .with_min_value("0")
            .with_max_value("4294967295")
            .with_signed(false)
            .with_nullable(true);
        let filtered = integers.select_compatible(&uinteger_constraints);
        cases.push(TypeTestCase {
            db_type: "UINTEGER".to_string(),
            semantic_type: SemanticType::Integer {
                signed: false,
                bits: 32,
            },
            expected_arrow_type: DataType::UInt32,
            values: filtered.values.into_iter().map(TestValue::from).collect(),
            shape: TestShape::Scalar,
        });

        // UBIGINT: 0 to 18446744073709551615
        let ubigint_constraints = Constraints::new()
            .with_min_value("0")
            .with_max_value("18446744073709551615")
            .with_signed(false)
            .with_nullable(true);
        let filtered = integers.select_compatible(&ubigint_constraints);
        cases.push(TypeTestCase {
            db_type: "UBIGINT".to_string(),
            semantic_type: SemanticType::Integer {
                signed: false,
                bits: 64,
            },
            expected_arrow_type: DataType::UInt64,
            values: filtered.values.into_iter().map(TestValue::from).collect(),
            shape: TestShape::Scalar,
        });
    }

    // Float types
    if let Ok(floats) = FixtureCategory::load("floats.toml") {
        // FLOAT (32-bit) - exclude values too large for float4 and special values
        // DuckDB FLOAT doesn't support NaN/Infinity literals directly
        let float_values: Vec<TestValue> = floats
            .values
            .iter()
            .filter(|v| {
                !matches!(
                    v.name.as_str(),
                    "nan" | "pos_infinity" | "neg_infinity" | "large"
                )
            })
            .cloned()
            .map(TestValue::from)
            .collect();
        cases.push(TypeTestCase {
            db_type: "FLOAT".to_string(),
            semantic_type: SemanticType::Float { bits: 32 },
            expected_arrow_type: DataType::Float32,
            values: float_values,
            shape: TestShape::Scalar,
        });

        // DOUBLE (64-bit) - exclude NaN/Infinity (DuckDB doesn't support these literals)
        let double_values: Vec<TestValue> = floats
            .values
            .iter()
            .filter(|v| !matches!(v.name.as_str(), "nan" | "pos_infinity" | "neg_infinity"))
            .cloned()
            .map(TestValue::from)
            .collect();
        cases.push(TypeTestCase {
            db_type: "DOUBLE".to_string(),
            semantic_type: SemanticType::Float { bits: 64 },
            expected_arrow_type: DataType::Float64,
            values: double_values,
            shape: TestShape::Scalar,
        });
    }

    // Decimal type - DuckDB has native Decimal128 support
    if let Ok(decimals) = FixtureCategory::load("decimals.toml") {
        // DECIMAL(38,10) - high precision, DuckDB supports up to 38 digits
        let decimal_constraints = Constraints::new()
            .with_max_precision(38)
            .with_max_scale(10)
            .with_nullable(true)
            .with_signed(true);
        let filtered = decimals.select_compatible(&decimal_constraints);
        cases.push(TypeTestCase {
            db_type: "DECIMAL(38,10)".to_string(),
            semantic_type: SemanticType::Decimal,
            expected_arrow_type: DataType::Decimal128(38, 10),
            values: filtered.values.into_iter().map(TestValue::from).collect(),
            shape: TestShape::Scalar,
        });
    }

    // String types
    if let Ok(strings) = FixtureCategory::load("strings.toml") {
        // VARCHAR - DuckDB uses Utf8
        // Exclude special_chars which uses PostgreSQL-specific escape syntax
        let varchar_constraints = Constraints::new().with_nullable(true);
        let filtered = strings.select_compatible(&varchar_constraints);
        let varchar_values: Vec<TestValue> = filtered
            .values
            .into_iter()
            .filter(|v| v.name != "special_chars")
            .map(TestValue::from)
            .collect();
        cases.push(TypeTestCase {
            db_type: "VARCHAR".to_string(),
            semantic_type: SemanticType::String,
            expected_arrow_type: DataType::Utf8,
            values: varchar_values,
            shape: TestShape::Scalar,
        });
    }

    // Timestamp types
    if let Ok(timestamps) = FixtureCategory::load("timestamps.toml") {
        let ts_constraints = Constraints::new().with_nullable(true);

        // TIMESTAMP (without timezone)
        let filtered = timestamps.select_compatible(&ts_constraints);
        cases.push(TypeTestCase {
            db_type: "TIMESTAMP".to_string(),
            semantic_type: SemanticType::Timestamp { with_tz: false },
            expected_arrow_type: DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            values: filtered.values.into_iter().map(TestValue::from).collect(),
            shape: TestShape::Scalar,
        });
    }

    // JSON type
    if let Ok(json) = FixtureCategory::load("json.toml") {
        let json_constraints = Constraints::new().with_nullable(true);
        let filtered = json.select_compatible(&json_constraints);
        cases.push(TypeTestCase {
            db_type: "JSON".to_string(),
            semantic_type: SemanticType::Json,
            expected_arrow_type: DataType::Utf8,
            values: filtered.values.into_iter().map(TestValue::from).collect(),
            shape: TestShape::Scalar,
        });
    }

    // TIMESTAMPTZ (Timestamp with timezone) - DuckDB stores as UTC
    if let Ok(timestamps) = FixtureCategory::load("timestamps.toml") {
        let ts_constraints = Constraints::new().with_nullable(true);
        let filtered = timestamps.select_compatible(&ts_constraints);
        cases.push(TypeTestCase {
            db_type: "TIMESTAMPTZ".to_string(),
            semantic_type: SemanticType::Timestamp { with_tz: true },
            expected_arrow_type: DataType::Timestamp(
                arrow_schema::TimeUnit::Microsecond,
                Some("UTC".into()),
            ),
            values: filtered.values.into_iter().map(TestValue::from).collect(),
            shape: TestShape::Scalar,
        });
    }

    // DATE type - DuckDB Date32
    if let Ok(timestamps) = FixtureCategory::load("timestamps.toml") {
        // Use timestamp fixtures but extract only the date portion
        let date_values: Vec<TestValue> = timestamps
            .values
            .iter()
            .filter(|v| !v.name.contains("microseconds")) // Skip fractional timestamps
            .map(|v| {
                // Extract date portion from timestamp SQL and expected values
                let date_sql = if v.sql.to_uppercase() == "NULL" {
                    "NULL".to_string()
                } else {
                    // Extract date from 'YYYY-MM-DD HH:MM:SS' -> 'YYYY-MM-DD'
                    let content = v.sql.trim_matches('\'');
                    let date_part = content.split_whitespace().next().unwrap_or(content);
                    format!("'{}'", date_part)
                };
                let expected_date =
                    if v.expected.is_empty() && v.expected_null.as_deref() == Some("NULL") {
                        crate::harness::ExpectedOutput::Null
                    } else {
                        let content = v.expected.split('T').next().unwrap_or(&v.expected);
                        crate::harness::ExpectedOutput::String(content.to_string())
                    };
                TestValue {
                    sql_literal: date_sql,
                    expected: expected_date,
                    comparison: ComparisonMode::Exact,
                    note: Some(v.name.clone()),
                }
            })
            .collect();
        cases.push(TypeTestCase {
            db_type: "DATE".to_string(),
            semantic_type: SemanticType::Date,
            expected_arrow_type: DataType::Date32,
            values: date_values,
            shape: TestShape::Scalar,
        });
    }

    // TIME type - DuckDB Time64 with microsecond precision
    {
        // Manual time values since we don't have a dedicated time fixture
        let time_values = vec![
            TestValue {
                sql_literal: "'00:00:00'".to_string(),
                expected: ExpectedOutput::String("00:00:00".to_string()),
                comparison: ComparisonMode::Exact,
                note: Some("midnight".to_string()),
            },
            TestValue {
                sql_literal: "'12:00:00'".to_string(),
                expected: ExpectedOutput::String("12:00:00".to_string()),
                comparison: ComparisonMode::Exact,
                note: Some("noon".to_string()),
            },
            TestValue {
                sql_literal: "'23:59:59'".to_string(),
                expected: ExpectedOutput::String("23:59:59".to_string()),
                comparison: ComparisonMode::Exact,
                note: Some("end_of_day".to_string()),
            },
            TestValue {
                sql_literal: "'10:30:45.123456'".to_string(),
                expected: ExpectedOutput::String("10:30:45.123456".to_string()),
                comparison: ComparisonMode::Exact,
                note: Some("with_microseconds".to_string()),
            },
            TestValue {
                sql_literal: "NULL".to_string(),
                expected: ExpectedOutput::Null,
                comparison: ComparisonMode::Exact,
                note: Some("null".to_string()),
            },
        ];
        cases.push(TypeTestCase {
            db_type: "TIME".to_string(),
            semantic_type: SemanticType::Time,
            expected_arrow_type: DataType::Time64(TimeUnit::Microsecond),
            values: time_values,
            shape: TestShape::Scalar,
        });
    }

    // INTERVAL type - DuckDB uses MonthDayNano
    {
        // Manual interval values
        let interval_values = vec![
            TestValue {
                sql_literal: "INTERVAL '1' DAY".to_string(),
                expected: ExpectedOutput::String("1 day".to_string()),
                comparison: ComparisonMode::Exact,
                note: Some("one_day".to_string()),
            },
            TestValue {
                sql_literal: "INTERVAL '1' HOUR".to_string(),
                expected: ExpectedOutput::String("01:00:00".to_string()),
                comparison: ComparisonMode::Exact,
                note: Some("one_hour".to_string()),
            },
            TestValue {
                sql_literal: "INTERVAL '1' MONTH".to_string(),
                expected: ExpectedOutput::String("1 month".to_string()),
                comparison: ComparisonMode::Exact,
                note: Some("one_month".to_string()),
            },
            TestValue {
                sql_literal: "NULL".to_string(),
                expected: ExpectedOutput::Null,
                comparison: ComparisonMode::Exact,
                note: Some("null".to_string()),
            },
        ];
        cases.push(TypeTestCase {
            db_type: "INTERVAL".to_string(),
            semantic_type: SemanticType::Interval,
            expected_arrow_type: DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano),
            values: interval_values,
            shape: TestShape::Scalar,
        });
    }

    // BLOB type - Binary data (using binary.toml fixture)
    if let Ok(binary) = FixtureCategory::load("binary.toml") {
        let binary_constraints = Constraints::new().with_nullable(true);
        let filtered = binary.select_compatible(&binary_constraints);
        // Convert fixture values to DuckDB BLOB syntax
        let blob_values: Vec<TestValue> = filtered
            .values
            .into_iter()
            .map(|v| {
                // Convert hex string from fixture to DuckDB BLOB syntax
                let sql_literal = if v.sql.to_uppercase() == "NULL" {
                    "NULL".to_string()
                } else if v.sql.is_empty() {
                    "''::BLOB".to_string()
                } else {
                    // Convert hex bytes to DuckDB blob format: '\xHH\xHH...'::BLOB
                    let hex_bytes: String = v
                        .sql
                        .as_bytes()
                        .chunks(2)
                        .map(|chunk| format!("\\x{}", std::str::from_utf8(chunk).unwrap_or("00")))
                        .collect();
                    format!("'{}'::BLOB", hex_bytes)
                };
                let expected = if v.expected_null.as_deref() == Some("NULL") {
                    ExpectedOutput::Null
                } else {
                    ExpectedOutput::String(v.expected.clone())
                };
                TestValue {
                    sql_literal,
                    expected,
                    comparison: ComparisonMode::Exact,
                    note: v.note.clone(),
                }
            })
            .collect();
        cases.push(TypeTestCase {
            db_type: "BLOB".to_string(),
            semantic_type: SemanticType::Binary,
            expected_arrow_type: DataType::Binary,
            values: blob_values,
            shape: TestShape::Scalar,
        });
    }

    // ========================================================================
    // HUGEINT type - 128-bit signed integer
    // DuckDB natively maps HUGEINT to Decimal128(38, 0) via query_arrow()
    // Note: Decimal128(38, 0) can only represent up to 38 decimal digits,
    // so we use values within that range rather than the full HUGEINT range.
    // ========================================================================
    let hugeint_values = vec![
        TestValue {
            sql_literal: "0".to_string(),
            expected: ExpectedOutput::String("0".to_string()),
            comparison: ComparisonMode::Exact,
            note: Some("Zero".to_string()),
        },
        TestValue {
            sql_literal: "99999999999999999999999999999999999999".to_string(), // 38 nines (max Decimal128)
            expected: ExpectedOutput::String("99999999999999999999999999999999999999".to_string()),
            comparison: ComparisonMode::Exact,
            note: Some("Max Decimal128(38, 0)".to_string()),
        },
        TestValue {
            sql_literal: "-99999999999999999999999999999999999999".to_string(), // 38 nines negative
            expected: ExpectedOutput::String("-99999999999999999999999999999999999999".to_string()),
            comparison: ComparisonMode::Exact,
            note: Some("Min Decimal128(38, 0)".to_string()),
        },
        TestValue {
            sql_literal: "NULL".to_string(),
            expected: ExpectedOutput::Null,
            comparison: ComparisonMode::Exact,
            note: Some("NULL HUGEINT".to_string()),
        },
    ];
    cases.push(TypeTestCase {
        db_type: "HUGEINT".to_string(),
        semantic_type: SemanticType::Integer {
            signed: true,
            bits: 128,
        },
        // DuckDB's query_arrow() returns Decimal128(38, 0) for HUGEINT
        expected_arrow_type: DataType::Decimal128(38, 0),
        values: hugeint_values,
        shape: TestShape::Scalar,
    });

    cases
}

// ============================================================================
// Test Infrastructure
// ============================================================================

/// Create a test SecretManager for use in tests.
async fn create_test_secret_manager(dir: &TempDir) -> SecretManager {
    let db_path = dir.path().join("test_catalog.db");
    let catalog = Arc::new(
        SqliteCatalogManager::new(db_path.to_str().unwrap())
            .await
            .unwrap(),
    );
    catalog.run_migrations().await.unwrap();

    let key = [0x42u8; 32];
    let backend = Arc::new(EncryptedCatalogBackend::new(key, catalog.clone()));

    SecretManager::new(backend, catalog, ENCRYPTED_PROVIDER_TYPE)
}

// ============================================================================
// Test Runner
// ============================================================================

/// Run a single type test case using NativeFetcher and CapturingBatchWriter.
///
/// This validates actual Arrow values produced by the production fetch path,
/// not text cast representations.
async fn run_test_case(
    db_path: &std::path::Path,
    secrets: &SecretManager,
    case: &TypeTestCase,
) -> TypeTestResult {
    let table_name = format!(
        "test_{}",
        case.db_type
            .replace(['(', ')', ',', ' '], "_")
            .to_lowercase()
    );

    // Skip if no test values
    if case.values.is_empty() {
        return TypeTestResult::skipped(&case.db_type, "No compatible test values");
    }

    // Setup: Create table and insert values using direct DuckDB connection
    {
        let conn = match Connection::open(db_path) {
            Ok(c) => c,
            Err(e) => {
                return TypeTestResult::failed(
                    &case.db_type,
                    FailureReason::FetchError {
                        message: format!("Failed to open database: {}", e),
                    },
                );
            }
        };

        // Drop table if exists (cleanup from previous run)
        let drop_sql = format!("DROP TABLE IF EXISTS {}", table_name);
        let _ = conn.execute(&drop_sql, []);

        // Create schema if needed
        let _ = conn.execute("CREATE SCHEMA IF NOT EXISTS main", []);

        // Create table with id column for deterministic ordering
        // (SELECT * doesn't guarantee order, so we sort by id during validation)
        let create_sql = format!(
            "CREATE TABLE {} (id BIGINT PRIMARY KEY, val {})",
            table_name, case.db_type
        );
        if let Err(e) = conn.execute(&create_sql, []) {
            return TypeTestResult::failed(
                &case.db_type,
                FailureReason::FetchError {
                    message: format!("Failed to create table '{}': {}", create_sql, e),
                },
            );
        }

        // NOTE: We don't validate duckdb_type_to_arrow here because DuckDB's fetch_table
        // uses query_arrow() which returns DuckDB's native Arrow types directly,
        // not our mapped types. The actual type is validated after fetch_table.

        // Insert all test values with explicit id for ordering
        for (idx, test_value) in case.values.iter().enumerate() {
            let insert_sql = format!(
                "INSERT INTO {} (id, val) VALUES ({}, {})",
                table_name, idx, test_value.sql_literal
            );
            if let Err(e) = conn.execute(&insert_sql, []) {
                return TypeTestResult::failed(
                    &case.db_type,
                    FailureReason::FetchError {
                        message: format!(
                            "Failed to insert value '{}': {}",
                            test_value.sql_literal, e
                        ),
                    },
                );
            }
        }
    } // Connection dropped here

    // Fetch using NativeFetcher with CapturingBatchWriter
    let fetcher = NativeFetcher::new();
    let source = Source::Duckdb {
        path: db_path.to_str().unwrap().to_string(),
    };

    let mut writer = CapturingBatchWriter::new();
    let fetch_result = fetcher
        .fetch_table(&source, secrets, None, "main", &table_name, &mut writer)
        .await;

    if let Err(e) = fetch_result {
        return TypeTestResult::failed(
            &case.db_type,
            FailureReason::FetchError {
                message: format!("fetch_table failed: {}", e),
            },
        );
    }

    // Validate schema - check the 'val' column type specifically
    let batches = writer.batches();
    let actual_val_type = match get_val_column_type(batches) {
        Some(t) => t,
        None => {
            return TypeTestResult::failed(
                &case.db_type,
                FailureReason::FetchError {
                    message: "No 'val' column found in batches".to_string(),
                },
            );
        }
    };

    if actual_val_type != case.expected_arrow_type {
        return TypeTestResult::failed(
            &case.db_type,
            FailureReason::ArrowTypeMismatch {
                expected: case.expected_arrow_type.clone(),
                actual: actual_val_type,
            },
        );
    }

    // Validate values using the harness utility
    if let Err(reason) = validate_batch_values(batches, &case.values, &case.semantic_type) {
        return TypeTestResult::failed(&case.db_type, reason);
    }

    TypeTestResult::passed(&case.db_type)
}

// ============================================================================
// Main Test
// ============================================================================

#[tokio::test]
async fn test_duckdb_type_coverage() {
    // Create a temporary directory for the DuckDB database and secret manager
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.duckdb");
    let secrets = create_test_secret_manager(&temp_dir).await;

    // Build test cases
    let test_cases = build_duckdb_test_cases();
    println!("\nRunning {} DuckDB type test cases...\n", test_cases.len());

    // Run all test cases
    let mut report = TestReport::new("duckdb");
    for case in &test_cases {
        println!("  Testing {}...", case.db_type);
        let result = run_test_case(&db_path, &secrets, case).await;
        match &result.status {
            crate::harness::TestStatus::Passed => println!("    PASSED"),
            crate::harness::TestStatus::Failed(reason) => {
                println!("    FAILED: {:?}", reason)
            }
            crate::harness::TestStatus::Skipped(reason) => {
                println!("    SKIPPED: {}", reason)
            }
        }
        report.add_result(result);
    }

    // Print summary
    println!("\n--- Test Summary ---");
    println!("  Passed:  {}", report.passed_count());
    println!("  Failed:  {}", report.failed_count());
    println!("  Skipped: {}", report.skipped_count());

    // Assert all passed
    report.assert_all_passed();
}
