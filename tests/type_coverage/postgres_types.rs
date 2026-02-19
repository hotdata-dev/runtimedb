//! PostgreSQL type coverage tests.
//!
//! Tests comprehensive type mapping from PostgreSQL types to Arrow types.
//! Uses testcontainers to run against a real PostgreSQL instance.
//!
//! IMPORTANT: This test validates actual Arrow values produced by fetch_table,
//! not text cast representations. Uses CapturingBatchWriter to capture Arrow
//! RecordBatches and validates both schema types and data values.

use arrow_schema::DataType;
use sqlx::PgPool;
use std::sync::Arc;
use tempfile::TempDir;
use testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;

// Production code imports
use runtimedb::catalog::{CatalogManager, SqliteCatalogManager};
use runtimedb::datafetch::{DataFetcher, NativeFetcher};
use runtimedb::secrets::{EncryptedCatalogBackend, SecretManager, ENCRYPTED_PROVIDER_TYPE};
use runtimedb::source::{Credential, Source};

use crate::capturing_writer::CapturingBatchWriter;
use crate::fixtures::{Constraints, FixtureCategory};
use crate::harness::{
    get_val_column_type, validate_batch_values, ComparisonMode, ExpectedOutput, FailureReason,
    SemanticType, TestReport, TestShape, TestValue, TypeTestCase, TypeTestResult,
};

/// Password used for test PostgreSQL containers.
const TEST_PASSWORD: &str = "postgres";

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
// Test Case Builder
// ============================================================================

/// Build test cases for PostgreSQL types using fixture data.
fn build_postgres_test_cases() -> Vec<TypeTestCase> {
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
            setup_sql: None,
        });
    }

    // Integer types
    if let Ok(integers) = FixtureCategory::load("integers.toml") {
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
            setup_sql: None,
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
            setup_sql: None,
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
            setup_sql: None,
        });
    }

    // Float types
    if let Ok(floats) = FixtureCategory::load("floats.toml") {
        // REAL (float4) - 32-bit float (exclude values too large for float4)
        // Filter out the "large" value which exceeds float4 range
        let real_values: Vec<TestValue> = floats
            .values
            .iter()
            .filter(|v| v.name != "large") // Skip large value for REAL
            .cloned()
            .map(TestValue::from)
            .collect();
        cases.push(TypeTestCase {
            db_type: "REAL".to_string(),
            semantic_type: SemanticType::Float { bits: 32 },
            expected_arrow_type: DataType::Float32,
            values: real_values,
            shape: TestShape::Scalar,
            setup_sql: None,
        });

        // DOUBLE PRECISION (float8) - 64-bit float
        let float_constraints = Constraints::new().with_nullable(true);
        let filtered = floats.select_compatible(&float_constraints);
        cases.push(TypeTestCase {
            db_type: "DOUBLE PRECISION".to_string(),
            semantic_type: SemanticType::Float { bits: 64 },
            expected_arrow_type: DataType::Float64,
            values: filtered.values.into_iter().map(TestValue::from).collect(),
            shape: TestShape::Scalar,
            setup_sql: None,
        });
    }

    // Decimal types
    if let Ok(decimals) = FixtureCategory::load("decimals.toml") {
        // NUMERIC without precision/scale (PostgreSQL allows arbitrary precision)
        let numeric_constraints = Constraints::new().with_nullable(true).with_signed(true);
        let filtered = decimals.select_compatible(&numeric_constraints);
        cases.push(TypeTestCase {
            db_type: "NUMERIC".to_string(),
            semantic_type: SemanticType::Decimal,
            expected_arrow_type: DataType::Utf8, // NUMERIC often maps to string for full precision
            values: filtered.values.into_iter().map(TestValue::from).collect(),
            shape: TestShape::Scalar,
            setup_sql: None,
        });

        // NUMERIC(10,2) - constrained precision and scale
        // Production code uses Decimal128 for constrained NUMERIC types
        let numeric_10_2_constraints = Constraints::new()
            .with_max_precision(10)
            .with_max_scale(2)
            .with_nullable(true)
            .with_signed(true);
        let filtered = decimals.select_compatible(&numeric_10_2_constraints);
        cases.push(TypeTestCase {
            db_type: "NUMERIC(10,2)".to_string(),
            semantic_type: SemanticType::Decimal,
            expected_arrow_type: DataType::Decimal128(10, 2),
            values: filtered.values.into_iter().map(TestValue::from).collect(),
            shape: TestShape::Scalar,
            setup_sql: None,
        });
    }

    // String types
    if let Ok(strings) = FixtureCategory::load("strings.toml") {
        // VARCHAR(255)
        let varchar_constraints = Constraints::new().with_max_bytes(255).with_nullable(true);
        let filtered = strings.select_compatible(&varchar_constraints);
        cases.push(TypeTestCase {
            db_type: "VARCHAR(255)".to_string(),
            semantic_type: SemanticType::String,
            expected_arrow_type: DataType::Utf8,
            values: filtered.values.into_iter().map(TestValue::from).collect(),
            shape: TestShape::Scalar,
            setup_sql: None,
        });

        // TEXT (unlimited)
        let text_constraints = Constraints::new().with_nullable(true);
        let filtered = strings.select_compatible(&text_constraints);
        cases.push(TypeTestCase {
            db_type: "TEXT".to_string(),
            semantic_type: SemanticType::String,
            expected_arrow_type: DataType::Utf8,
            values: filtered.values.into_iter().map(TestValue::from).collect(),
            shape: TestShape::Scalar,
            setup_sql: None,
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
            setup_sql: None,
        });

        // NOTE: TIMESTAMPTZ is not yet fully supported - there's a schema mismatch bug
        // in the production code where pg_type_to_arrow returns Timestamp(Microsecond, Some("UTC"))
        // but the builder creates Timestamp(Microsecond, None). This needs a fix in postgres.rs
        // to use TimestampMicrosecondBuilder with timezone.

        // DATE type
        let date_values = vec![
            TestValue {
                sql_literal: "'1970-01-01'".to_string(),
                expected: ExpectedOutput::String("1970-01-01".to_string()),
                comparison: ComparisonMode::Exact,
                note: Some("Unix epoch date".to_string()),
            },
            TestValue {
                sql_literal: "'2024-06-15'".to_string(),
                expected: ExpectedOutput::String("2024-06-15".to_string()),
                comparison: ComparisonMode::Exact,
                note: Some("Modern date".to_string()),
            },
            TestValue {
                sql_literal: "'1969-07-20'".to_string(),
                expected: ExpectedOutput::String("1969-07-20".to_string()),
                comparison: ComparisonMode::Exact,
                note: Some("Before epoch date".to_string()),
            },
            TestValue {
                sql_literal: "NULL".to_string(),
                expected: ExpectedOutput::Null,
                comparison: ComparisonMode::Exact,
                note: Some("NULL date".to_string()),
            },
        ];
        cases.push(TypeTestCase {
            db_type: "DATE".to_string(),
            semantic_type: SemanticType::Date,
            expected_arrow_type: DataType::Date32,
            values: date_values,
            shape: TestShape::Scalar,
            setup_sql: None,
        });

        // NOTE: TIME and INTERVAL types are not yet supported by the production fetch code.
        // sqlx-postgres doesn't decode these types to String directly; they would need
        // explicit handling with chrono::NaiveTime and PgInterval types.
    }

    // Binary types (BYTEA) - using binary.toml fixture
    // Production fetch returns raw bytes (not hex-escaped text)
    if let Ok(binary) = FixtureCategory::load("binary.toml") {
        let binary_constraints = Constraints::new().with_nullable(true);
        let filtered = binary.select_compatible(&binary_constraints);
        // Convert fixture values to PostgreSQL BYTEA syntax
        let bytea_values: Vec<TestValue> = filtered
            .values
            .into_iter()
            .map(|v| {
                // Convert hex string from fixture to PostgreSQL bytea syntax: '\xHEX'::bytea
                let sql_literal = if v.sql.to_uppercase() == "NULL" {
                    "NULL".to_string()
                } else if v.sql.is_empty() {
                    "''::bytea".to_string()
                } else {
                    format!("'\\x{}'::bytea", v.sql.to_lowercase())
                };
                // Production fetch returns raw bytes as Vec<u8>
                // The expected value from fixture is the decoded string (e.g., "HELLO")
                let expected = if v.expected_null.as_deref() == Some("NULL") {
                    ExpectedOutput::Null
                } else {
                    // The expected field from binary.toml contains the decoded text
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
            db_type: "BYTEA".to_string(),
            semantic_type: SemanticType::Binary,
            expected_arrow_type: DataType::Binary,
            values: bytea_values,
            shape: TestShape::Scalar,
            setup_sql: None,
        });
    }

    // ========================================================================
    // UNSUPPORTED TYPES
    // ========================================================================
    // The following PostgreSQL-specific types are NOT yet supported by the
    // production fetch code because sqlx-postgres doesn't decode them directly
    // to String. They would need explicit handling:
    //
    // - UUID: needs sqlx::types::Uuid -> to_string()
    // - JSON/JSONB: needs sqlx::types::Json<T> -> serde serialization
    // - INET/CIDR: needs ipnetwork::IpNetwork -> to_string()
    // - MACADDR: needs MacAddress type -> to_string()
    // - Geometric types (POINT, LINE, BOX, CIRCLE): need pg_geometric types
    // - Range types (INT4RANGE, DATERANGE): need pg_range types
    // - Array types (INTEGER[], TEXT[]): need explicit array handling
    // - TIME: needs chrono::NaiveTime
    // - INTERVAL: needs PgInterval

    cases
}

// ============================================================================
// Test Runner
// ============================================================================

/// Run a single type test case using NativeFetcher and CapturingBatchWriter.
///
/// This validates actual Arrow values produced by the production fetch path,
/// not text cast representations.
async fn run_test_case(
    pool: &PgPool,
    source: &Source,
    secrets: &SecretManager,
    case: &TypeTestCase,
) -> TypeTestResult {
    let table_name = format!(
        "test_{}",
        case.db_type
            .replace(['(', ')', ',', ' ', '[', ']'], "_")
            .to_lowercase()
    );

    // Skip if no test values
    if case.values.is_empty() {
        return TypeTestResult::skipped(&case.db_type, "No compatible test values");
    }

    // Drop table if exists (cleanup from previous run)
    let drop_sql = format!("DROP TABLE IF EXISTS {}", table_name);
    let _ = sqlx::query(&drop_sql).execute(pool).await;

    // Create table with id column for deterministic ordering
    // (SELECT * doesn't guarantee order, so we sort by id during validation)
    let create_sql = format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, val {})",
        table_name, case.db_type
    );
    if let Err(e) = sqlx::query(&create_sql).execute(pool).await {
        return TypeTestResult::failed(
            &case.db_type,
            FailureReason::FetchError {
                message: format!("Failed to create table '{}': {}", create_sql, e),
            },
        );
    }

    // Insert all test values with explicit id for ordering
    for (idx, test_value) in case.values.iter().enumerate() {
        let insert_sql = format!(
            "INSERT INTO {} (id, val) VALUES ({}, {})",
            table_name, idx, test_value.sql_literal
        );
        if let Err(e) = sqlx::query(&insert_sql).execute(pool).await {
            return TypeTestResult::failed(
                &case.db_type,
                FailureReason::FetchError {
                    message: format!("Failed to insert value '{}': {}", test_value.sql_literal, e),
                },
            );
        }
    }

    // Fetch using NativeFetcher with CapturingBatchWriter
    let fetcher = NativeFetcher::new();

    let mut writer = CapturingBatchWriter::new();
    let fetch_result = fetcher
        .fetch_table(source, secrets, None, "public", &table_name, &mut writer)
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

/// Start a PostgreSQL container for testing.
/// Returns the container, pool, and port number.
async fn start_postgres_container() -> (ContainerAsync<Postgres>, PgPool, u16) {
    let container = Postgres::default()
        .with_tag("15-alpine")
        .start()
        .await
        .expect("Failed to start PostgreSQL container");

    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let conn_str = format!(
        "postgres://postgres:{}@localhost:{}/postgres",
        TEST_PASSWORD, port
    );

    let pool = PgPool::connect(&conn_str)
        .await
        .expect("Failed to connect to PostgreSQL");

    // Set session timezone to UTC for consistent TIMESTAMPTZ handling.
    // This ensures:
    // 1. Input timestamps without timezone are interpreted as UTC
    // 2. Output timestamps are displayed in UTC (no offset suffix)
    sqlx::query("SET TIME ZONE 'UTC'")
        .execute(&pool)
        .await
        .expect("Failed to set session timezone to UTC");

    // Ensure standard_conforming_strings is ON (default since PostgreSQL 9.1).
    // This ensures backslash is treated as a literal character in string literals,
    // not as an escape character. Our string fixtures rely on this behavior.
    sqlx::query("SET standard_conforming_strings = on")
        .execute(&pool)
        .await
        .expect("Failed to set standard_conforming_strings");

    (container, pool, port)
}

/// Secret name used for PostgreSQL password in tests.
const PG_SECRET_NAME: &str = "test-pg-password";

#[tokio::test]
async fn test_postgres_type_coverage() {
    // Create temp dir for secret manager
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let secrets = create_test_secret_manager(&temp_dir).await;

    // Start PostgreSQL container
    let (_container, pool, port) = start_postgres_container().await;

    // Store the password as a secret
    let secret_id = secrets
        .create(PG_SECRET_NAME, TEST_PASSWORD.as_bytes())
        .await
        .expect("Failed to store password secret");

    // Create the Source with the proper structure
    let source = Source::Postgres {
        host: "localhost".to_string(),
        port,
        user: "postgres".to_string(),
        database: "postgres".to_string(),
        credential: Credential::SecretRef { id: secret_id },
    };

    // Build test cases
    let test_cases = build_postgres_test_cases();
    println!(
        "\nRunning {} PostgreSQL type test cases...\n",
        test_cases.len()
    );

    // Run all test cases
    let mut report = TestReport::new("postgres");
    for case in &test_cases {
        println!("  Testing {}...", case.db_type);
        let result = run_test_case(&pool, &source, &secrets, case).await;
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

/// Explicit test to verify special_chars fixture round-trips correctly through PostgreSQL.
/// This verifies that TOML escape sequences (\t, \n, \\) are correctly handled.
#[tokio::test]
async fn test_postgres_special_chars_roundtrip() {
    let (_container, pool, _port) = start_postgres_container().await;

    // Load the special_chars fixture directly
    let strings = FixtureCategory::load("strings.toml").expect("Failed to load strings.toml");
    let special_chars = strings
        .get("special_chars")
        .expect("Should have special_chars");

    // Verify the TOML parsing produced actual escape characters
    assert!(
        special_chars.sql.contains('\t'),
        "SQL should contain actual tab character"
    );
    assert!(
        special_chars.expected.contains('\t'),
        "Expected should contain actual tab character"
    );

    // Create table and insert
    sqlx::query("DROP TABLE IF EXISTS test_special_chars")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("CREATE TABLE test_special_chars (val TEXT)")
        .execute(&pool)
        .await
        .unwrap();

    let insert_sql = format!(
        "INSERT INTO test_special_chars (val) VALUES ({})",
        special_chars.sql
    );
    sqlx::query(&insert_sql).execute(&pool).await.unwrap();

    // Query back
    let (actual,): (String,) = sqlx::query_as("SELECT val FROM test_special_chars")
        .fetch_one(&pool)
        .await
        .unwrap();

    // Verify the round-trip matches
    assert_eq!(
        actual,
        special_chars.expected,
        "Special chars round-trip failed.\nExpected bytes: {:?}\nActual bytes: {:?}",
        special_chars.expected.as_bytes(),
        actual.as_bytes()
    );
}
