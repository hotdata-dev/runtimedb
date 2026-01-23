//! MySQL type coverage tests.
//!
//! Tests comprehensive type mapping from MySQL types to Arrow types.
//! Uses testcontainers to run against a real MySQL instance.
//!
//! IMPORTANT: This test validates actual Arrow values produced by fetch_table,
//! not text cast representations. Uses CapturingBatchWriter to capture Arrow
//! RecordBatches and validates both schema types and data values.

use arrow_schema::DataType;
use sqlx::MySqlPool;
use std::sync::Arc;
use tempfile::TempDir;
use testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt};
use testcontainers_modules::mysql::Mysql;

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

/// Password used for test MySQL containers.
const TEST_PASSWORD: &str = "root";

/// Secret name used for MySQL password in tests.
const MYSQL_SECRET_NAME: &str = "test-mysql-password";

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

/// Build test cases for MySQL types using fixture data.
fn build_mysql_test_cases() -> Vec<TypeTestCase> {
    let mut cases = Vec::new();

    // Boolean type (MySQL uses TINYINT(1) for booleans)
    if let Ok(booleans) = FixtureCategory::load("booleans.toml") {
        let constraints = Constraints::new().with_nullable(true);
        let filtered = booleans.select_compatible(&constraints);
        // Convert boolean values to MySQL format (0/1 instead of true/false)
        let values: Vec<TestValue> = filtered
            .values
            .into_iter()
            .map(|v| {
                let mut tv = TestValue::from(v);
                // MySQL TINYINT(1) uses 0/1
                if tv.sql_literal.to_lowercase() == "true" {
                    tv.sql_literal = "1".to_string();
                } else if tv.sql_literal.to_lowercase() == "false" {
                    tv.sql_literal = "0".to_string();
                }
                tv
            })
            .collect();
        cases.push(TypeTestCase {
            db_type: "TINYINT(1)".to_string(),
            semantic_type: SemanticType::Boolean,
            expected_arrow_type: DataType::Boolean, // TINYINT(1) is MySQL's boolean convention
            values,
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

        // INT: -2147483648 to 2147483647
        let int_constraints = Constraints::new()
            .with_min_value("-2147483648")
            .with_max_value("2147483647")
            .with_signed(true)
            .with_nullable(true);
        let filtered = integers.select_compatible(&int_constraints);
        cases.push(TypeTestCase {
            db_type: "INT".to_string(),
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

        // TINYINT UNSIGNED: 0 to 255
        let tinyint_unsigned_constraints = Constraints::new()
            .with_min_value("0")
            .with_max_value("255")
            .with_signed(false)
            .with_nullable(true);
        let filtered = integers.select_compatible(&tinyint_unsigned_constraints);
        cases.push(TypeTestCase {
            db_type: "TINYINT UNSIGNED".to_string(),
            semantic_type: SemanticType::Integer {
                signed: false,
                bits: 8,
            },
            expected_arrow_type: DataType::UInt8,
            values: filtered.values.into_iter().map(TestValue::from).collect(),
            shape: TestShape::Scalar,
        });

        // INT UNSIGNED: 0 to 4294967295
        let int_unsigned_constraints = Constraints::new()
            .with_min_value("0")
            .with_max_value("4294967295")
            .with_signed(false)
            .with_nullable(true);
        let filtered = integers.select_compatible(&int_unsigned_constraints);
        cases.push(TypeTestCase {
            db_type: "INT UNSIGNED".to_string(),
            semantic_type: SemanticType::Integer {
                signed: false,
                bits: 32,
            },
            expected_arrow_type: DataType::UInt32,
            values: filtered.values.into_iter().map(TestValue::from).collect(),
            shape: TestShape::Scalar,
        });

        // BIGINT UNSIGNED: 0 to 18446744073709551615
        let bigint_unsigned_constraints = Constraints::new()
            .with_min_value("0")
            .with_max_value("18446744073709551615")
            .with_signed(false)
            .with_nullable(true);
        let filtered = integers.select_compatible(&bigint_unsigned_constraints);
        cases.push(TypeTestCase {
            db_type: "BIGINT UNSIGNED".to_string(),
            semantic_type: SemanticType::Integer {
                signed: false,
                bits: 64,
            },
            expected_arrow_type: DataType::UInt64,
            values: filtered.values.into_iter().map(TestValue::from).collect(),
            shape: TestShape::Scalar,
        });

        // SMALLINT UNSIGNED: 0 to 65535
        let smallint_unsigned_constraints = Constraints::new()
            .with_min_value("0")
            .with_max_value("65535")
            .with_signed(false)
            .with_nullable(true);
        let filtered = integers.select_compatible(&smallint_unsigned_constraints);
        cases.push(TypeTestCase {
            db_type: "SMALLINT UNSIGNED".to_string(),
            semantic_type: SemanticType::Integer {
                signed: false,
                bits: 16,
            },
            expected_arrow_type: DataType::UInt16,
            values: filtered.values.into_iter().map(TestValue::from).collect(),
            shape: TestShape::Scalar,
        });

        // MEDIUMINT: -8388608 to 8388607
        let mediumint_constraints = Constraints::new()
            .with_min_value("-8388608")
            .with_max_value("8388607")
            .with_signed(true)
            .with_nullable(true);
        let filtered = integers.select_compatible(&mediumint_constraints);
        cases.push(TypeTestCase {
            db_type: "MEDIUMINT".to_string(),
            semantic_type: SemanticType::Integer {
                signed: true,
                bits: 24,
            },
            expected_arrow_type: DataType::Int32, // MEDIUMINT maps to Int32
            values: filtered.values.into_iter().map(TestValue::from).collect(),
            shape: TestShape::Scalar,
        });

        // MEDIUMINT UNSIGNED: 0 to 16777215
        let mediumint_unsigned_constraints = Constraints::new()
            .with_min_value("0")
            .with_max_value("16777215")
            .with_signed(false)
            .with_nullable(true);
        let filtered = integers.select_compatible(&mediumint_unsigned_constraints);
        cases.push(TypeTestCase {
            db_type: "MEDIUMINT UNSIGNED".to_string(),
            semantic_type: SemanticType::Integer {
                signed: false,
                bits: 24,
            },
            expected_arrow_type: DataType::UInt32, // MEDIUMINT UNSIGNED maps to UInt32
            values: filtered.values.into_iter().map(TestValue::from).collect(),
            shape: TestShape::Scalar,
        });

        // YEAR: 1901 to 2155 (or 0)
        let year_constraints = Constraints::new()
            .with_min_value("1901")
            .with_max_value("2155")
            .with_signed(true)
            .with_nullable(true);
        let filtered = integers.select_compatible(&year_constraints);
        cases.push(TypeTestCase {
            db_type: "YEAR".to_string(),
            semantic_type: SemanticType::Integer {
                signed: true,
                bits: 16,
            },
            expected_arrow_type: DataType::Int32, // YEAR maps to Int32
            values: filtered.values.into_iter().map(TestValue::from).collect(),
            shape: TestShape::Scalar,
        });
    }

    // Float types
    if let Ok(floats) = FixtureCategory::load("floats.toml") {
        // FLOAT (32-bit) - exclude NaN/Infinity (MySQL doesn't support literals)
        // Also exclude "large" and "pi" values - CAST truncates float precision
        let float_values: Vec<TestValue> = floats
            .values
            .iter()
            .filter(|v| {
                !matches!(
                    v.name.as_str(),
                    "nan" | "pos_infinity" | "neg_infinity" | "large" | "pi"
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

        // DOUBLE (64-bit) - exclude NaN/Infinity (MySQL doesn't support literals)
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

    // Decimal types
    // NOTE: MySQL DECIMAL types are not yet fully supported by the production fetch code.
    // sqlx-mysql doesn't decode DECIMAL to String directly. They would need explicit
    // handling with rust_decimal::Decimal -> to_string().

    // String types
    if let Ok(strings) = FixtureCategory::load("strings.toml") {
        // VARCHAR(255)
        // Exclude special_chars which uses PostgreSQL-specific escape syntax
        let varchar_constraints = Constraints::new().with_max_bytes(255).with_nullable(true);
        let filtered = strings.select_compatible(&varchar_constraints);
        let varchar_values: Vec<TestValue> = filtered
            .values
            .into_iter()
            .filter(|v| v.name != "special_chars")
            .map(TestValue::from)
            .collect();
        cases.push(TypeTestCase {
            db_type: "VARCHAR(255)".to_string(),
            semantic_type: SemanticType::String,
            expected_arrow_type: DataType::Utf8,
            values: varchar_values,
            shape: TestShape::Scalar,
        });

        // TEXT
        // Exclude special_chars which uses PostgreSQL-specific escape syntax
        let text_constraints = Constraints::new().with_nullable(true);
        let filtered = strings.select_compatible(&text_constraints);
        let text_values: Vec<TestValue> = filtered
            .values
            .into_iter()
            .filter(|v| v.name != "special_chars")
            .map(TestValue::from)
            .collect();
        cases.push(TypeTestCase {
            db_type: "TEXT".to_string(),
            semantic_type: SemanticType::String,
            expected_arrow_type: DataType::Utf8,
            values: text_values,
            shape: TestShape::Scalar,
        });
    }

    // Timestamp types
    if let Ok(timestamps) = FixtureCategory::load("timestamps.toml") {
        let ts_constraints = Constraints::new().with_nullable(true);

        // DATETIME(6) (with microsecond precision, without timezone)
        let filtered = timestamps.select_compatible(&ts_constraints);
        cases.push(TypeTestCase {
            db_type: "DATETIME(6)".to_string(),
            semantic_type: SemanticType::Timestamp { with_tz: false },
            expected_arrow_type: DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            values: filtered.values.into_iter().map(TestValue::from).collect(),
            shape: TestShape::Scalar,
        });

        // NOTE: TIMESTAMP(6) is not yet fully supported - there's a schema mismatch bug
        // in the production code where mysql_type_to_arrow returns Timestamp with timezone
        // but the builder creates Timestamp without timezone. Needs fix in mysql.rs.

        // DATE type
        // We'll use simple date values that work across all databases
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
        });

        // NOTE: TIME type is not yet supported by the production fetch code.
        // sqlx-mysql doesn't decode TIME to String directly.
    }

    // Binary types - MySQL uses X'...' syntax (using binary.toml fixture)
    if let Ok(binary) = FixtureCategory::load("binary.toml") {
        let binary_constraints = Constraints::new().with_nullable(true);
        let filtered = binary.select_compatible(&binary_constraints);
        // Convert fixture values to MySQL BLOB syntax
        let blob_values: Vec<TestValue> = filtered
            .values
            .into_iter()
            .map(|v| {
                // Convert hex string from fixture to MySQL binary syntax: X'HEX'
                let sql_literal = if v.sql.to_uppercase() == "NULL" {
                    "NULL".to_string()
                } else if v.sql.is_empty() {
                    "X''".to_string()
                } else {
                    format!("X'{}'", v.sql)
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

    // NOTE: JSON type is not yet supported by the production fetch code.
    // sqlx-mysql doesn't decode JSON to String directly. Would need explicit
    // handling with serde_json::Value -> to_string().

    // ========================================================================
    // ENUM type - maps to Utf8
    // ========================================================================
    let enum_values = vec![
        TestValue {
            sql_literal: "'small'".to_string(),
            expected: ExpectedOutput::String("small".to_string()),
            comparison: ComparisonMode::Exact,
            note: Some("First enum value".to_string()),
        },
        TestValue {
            sql_literal: "'medium'".to_string(),
            expected: ExpectedOutput::String("medium".to_string()),
            comparison: ComparisonMode::Exact,
            note: Some("Second enum value".to_string()),
        },
        TestValue {
            sql_literal: "'large'".to_string(),
            expected: ExpectedOutput::String("large".to_string()),
            comparison: ComparisonMode::Exact,
            note: Some("Third enum value".to_string()),
        },
        TestValue {
            sql_literal: "NULL".to_string(),
            expected: ExpectedOutput::Null,
            comparison: ComparisonMode::Exact,
            note: Some("NULL enum".to_string()),
        },
    ];
    cases.push(TypeTestCase {
        db_type: "ENUM('small','medium','large')".to_string(),
        semantic_type: SemanticType::String, // ENUM is conceptually a constrained string
        expected_arrow_type: DataType::Utf8,
        values: enum_values,
        shape: TestShape::Scalar,
    });

    // ========================================================================
    // SET type - maps to Utf8
    // ========================================================================
    let set_values = vec![
        TestValue {
            sql_literal: "'a'".to_string(),
            expected: ExpectedOutput::String("a".to_string()),
            comparison: ComparisonMode::Exact,
            note: Some("Single value".to_string()),
        },
        TestValue {
            sql_literal: "'a,b'".to_string(),
            expected: ExpectedOutput::String("a,b".to_string()),
            comparison: ComparisonMode::Exact,
            note: Some("Multiple values".to_string()),
        },
        TestValue {
            sql_literal: "'a,b,c'".to_string(),
            expected: ExpectedOutput::String("a,b,c".to_string()),
            comparison: ComparisonMode::Exact,
            note: Some("All values".to_string()),
        },
        TestValue {
            sql_literal: "''".to_string(),
            expected: ExpectedOutput::String("".to_string()),
            comparison: ComparisonMode::Exact,
            note: Some("Empty set".to_string()),
        },
        TestValue {
            sql_literal: "NULL".to_string(),
            expected: ExpectedOutput::Null,
            comparison: ComparisonMode::Exact,
            note: Some("NULL set".to_string()),
        },
    ];
    cases.push(TypeTestCase {
        db_type: "SET('a','b','c')".to_string(),
        semantic_type: SemanticType::String, // SET is conceptually a set of strings
        expected_arrow_type: DataType::Utf8,
        values: set_values,
        shape: TestShape::Scalar,
    });

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
    pool: &MySqlPool,
    source: &Source,
    secrets: &SecretManager,
    case: &TypeTestCase,
) -> TypeTestResult {
    let table_name = format!(
        "test_{}",
        case.db_type
            .replace(['(', ')', ',', ' ', '\''], "_")
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
        .fetch_table(
            source,
            secrets,
            None,
            "type_tests",
            &table_name,
            &mut writer,
        )
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

/// Start a MySQL container for testing.
/// Returns the container, pool, and port number.
async fn start_mysql_container() -> (ContainerAsync<Mysql>, MySqlPool, u16) {
    let container = Mysql::default()
        .with_tag("8.0")
        .with_env_var("MYSQL_ROOT_PASSWORD", TEST_PASSWORD)
        .start()
        .await
        .expect("Failed to start MySQL container");

    let port = container.get_host_port_ipv4(3306).await.unwrap();
    let conn_str = format!("mysql://root:{}@localhost:{}/mysql", TEST_PASSWORD, port);

    let pool = MySqlPool::connect(&conn_str)
        .await
        .expect("Failed to connect to MySQL");

    // Create test database
    sqlx::query("CREATE DATABASE IF NOT EXISTS type_tests")
        .execute(&pool)
        .await
        .expect("Failed to create test database");

    // Reconnect to the test database
    let conn_str = format!(
        "mysql://root:{}@localhost:{}/type_tests",
        TEST_PASSWORD, port
    );
    let pool = MySqlPool::connect(&conn_str)
        .await
        .expect("Failed to connect to test database");

    (container, pool, port)
}

#[tokio::test]
async fn test_mysql_type_coverage() {
    // Create temp dir for secret manager
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let secrets = create_test_secret_manager(&temp_dir).await;

    // Start MySQL container
    let (_container, pool, port) = start_mysql_container().await;

    // Store the password as a secret
    let secret_id = secrets
        .create(MYSQL_SECRET_NAME, TEST_PASSWORD.as_bytes())
        .await
        .expect("Failed to store password secret");

    // Create the Source with the proper structure
    let source = Source::Mysql {
        host: "localhost".to_string(),
        port,
        user: "root".to_string(),
        database: "type_tests".to_string(),
        credential: Credential::SecretRef { id: secret_id },
    };

    // Build test cases
    let test_cases = build_mysql_test_cases();
    println!("\nRunning {} MySQL type test cases...\n", test_cases.len());

    // Run all test cases
    let mut report = TestReport::new("mysql");
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
