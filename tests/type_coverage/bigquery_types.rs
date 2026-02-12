//! BigQuery type coverage tests.
//!
//! Tests comprehensive type mapping from BigQuery types to Arrow types.
//! Requires a real GCP project with BigQuery access.
//!
//! Required environment variables (test skipped if any are missing):
//! - `BQ_SERVICE_ACCOUNT_KEY_PATH` — path to GCP service account JSON key file
//! - `BQ_PROJECT_ID` — GCP project ID
//! - `BQ_DATASET` — BigQuery dataset to use for test tables
//!
//! Run with:
//! ```bash
//! BQ_SERVICE_ACCOUNT_KEY_PATH=/path/to/key.json \
//! BQ_PROJECT_ID=my-project \
//! BQ_DATASET=test_dataset \
//! cargo test --test type_coverage bigquery -- --nocapture
//! ```

use arrow_schema::DataType;
use gcp_bigquery_client::model::query_request::QueryRequest;
use gcp_bigquery_client::Client;
use tempfile::TempDir;

use runtimedb::datafetch::{DataFetcher, NativeFetcher};
use runtimedb::source::{Credential, Source};

use crate::capturing_writer::CapturingBatchWriter;
use crate::fixtures::{Constraints, FixtureCategory};
use crate::harness::{
    create_test_secret_manager, get_val_column_type, validate_batch_values, ComparisonMode,
    ExpectedOutput, FailureReason, SecretManager, SemanticType, TestReport, TestShape, TestValue,
    TypeTestCase, TypeTestResult,
};

/// Secret name used for BigQuery credentials in tests.
const BQ_SECRET_NAME: &str = "test-bq-credential";

/// Check for required environment variables.
/// Returns None if any are missing (test should skip).
fn bigquery_env() -> Option<(String, String, String)> {
    let key_path = std::env::var("BQ_SERVICE_ACCOUNT_KEY_PATH").ok()?;
    let project_id = std::env::var("BQ_PROJECT_ID").ok()?;
    let dataset = std::env::var("BQ_DATASET").ok()?;
    Some((key_path, project_id, dataset))
}

/// Execute a SQL statement against BigQuery (for DDL/DML setup).
async fn execute_bq_sql(client: &Client, project_id: &str, sql: &str) -> Result<(), String> {
    let request = QueryRequest::new(sql);
    client
        .job()
        .query(project_id, request)
        .await
        .map_err(|e| format!("BigQuery SQL failed: {}", e))?;
    Ok(())
}

/// Format a fully-qualified BigQuery table reference.
fn table_ref(project_id: &str, dataset: &str, table: &str) -> String {
    format!("`{}`.`{}`.`{}`", project_id, dataset, table)
}

/// Escape special characters inside a BigQuery SQL string literal.
///
/// BigQuery Standard SQL interprets `\n`, `\t`, `\\` etc. as escape sequences
/// inside string literals. Raw newlines/tabs inside `'...'` are syntax errors,
/// so we convert them to the corresponding escape sequences.
fn escape_bq_string_literal(sql: &str) -> String {
    if sql.to_uppercase() == "NULL" {
        return sql.to_string();
    }
    // Only escape content inside the outermost quotes
    if let Some(inner) = sql.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')) {
        let escaped = inner
            .replace('\\', "\\\\")
            .replace('\n', "\\n")
            .replace('\t', "\\t");
        format!("'{}'", escaped)
    } else {
        sql.to_string()
    }
}

// ============================================================================
// Test Case Builder
// ============================================================================

/// Build test cases for BigQuery types using fixture data.
fn build_bigquery_test_cases() -> Vec<TypeTestCase> {
    let mut cases = Vec::new();

    // ========================================================================
    // BOOL → Boolean
    // ========================================================================
    if let Ok(booleans) = FixtureCategory::load("booleans.toml") {
        let constraints = Constraints::new().with_nullable(true);
        let filtered = booleans.select_compatible(&constraints);
        cases.push(TypeTestCase {
            db_type: "BOOL".to_string(),
            semantic_type: SemanticType::Boolean,
            expected_arrow_type: DataType::Boolean,
            values: filtered.values.into_iter().map(TestValue::from).collect(),
            shape: TestShape::Scalar,
        });
    }

    // ========================================================================
    // INT64 → Int64
    // ========================================================================
    if let Ok(integers) = FixtureCategory::load("integers.toml") {
        // BigQuery INT64: all integer aliases map to INT64 (-2^63 to 2^63-1)
        let int64_constraints = Constraints::new()
            .with_min_value("-9223372036854775808")
            .with_max_value("9223372036854775807")
            .with_signed(true)
            .with_nullable(true);
        let filtered = integers.select_compatible(&int64_constraints);
        cases.push(TypeTestCase {
            db_type: "INT64".to_string(),
            semantic_type: SemanticType::Integer {
                signed: true,
                bits: 64,
            },
            expected_arrow_type: DataType::Int64,
            values: filtered.values.into_iter().map(TestValue::from).collect(),
            shape: TestShape::Scalar,
        });
    }

    // ========================================================================
    // FLOAT64 → Float64
    // ========================================================================
    if let Ok(floats) = FixtureCategory::load("floats.toml") {
        // Exclude NaN/Infinity (BQ REST API may return them unparseable)
        // Exclude 'large' (may come back in scientific notation that doesn't exact-match)
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
            db_type: "FLOAT64".to_string(),
            semantic_type: SemanticType::Float { bits: 64 },
            expected_arrow_type: DataType::Float64,
            values: float_values,
            shape: TestShape::Scalar,
        });
    }

    // ========================================================================
    // NUMERIC → Utf8 (default precision 29, scale 9)
    // ========================================================================
    if let Ok(decimals) = FixtureCategory::load("decimals.toml") {
        let numeric_constraints = Constraints::new()
            .with_max_precision(29)
            .with_max_scale(9)
            .with_nullable(true);
        let filtered = decimals.select_compatible(&numeric_constraints);
        cases.push(TypeTestCase {
            db_type: "NUMERIC".to_string(),
            semantic_type: SemanticType::Decimal,
            expected_arrow_type: DataType::Utf8,
            values: filtered.values.into_iter().map(TestValue::from).collect(),
            shape: TestShape::Scalar,
        });
    }

    // ========================================================================
    // BIGNUMERIC → Utf8 (precision 76, scale 38)
    // ========================================================================
    if let Ok(decimals) = FixtureCategory::load("decimals.toml") {
        let bignumeric_constraints = Constraints::new()
            .with_max_precision(76)
            .with_max_scale(38)
            .with_nullable(true);
        let filtered = decimals.select_compatible(&bignumeric_constraints);
        cases.push(TypeTestCase {
            db_type: "BIGNUMERIC".to_string(),
            semantic_type: SemanticType::Decimal,
            expected_arrow_type: DataType::Utf8,
            values: filtered.values.into_iter().map(TestValue::from).collect(),
            shape: TestShape::Scalar,
        });
    }

    // ========================================================================
    // STRING → Utf8
    // ========================================================================
    if let Ok(strings) = FixtureCategory::load("strings.toml") {
        let string_constraints = Constraints::new().with_nullable(true);
        let filtered = strings.select_compatible(&string_constraints);
        // Exclude special_chars (PG-specific escape syntax)
        let string_values: Vec<TestValue> = filtered
            .values
            .into_iter()
            .filter(|v| v.name != "special_chars")
            .map(|v| {
                let mut tv = TestValue::from(v);
                // BigQuery string literals don't allow raw newlines/tabs —
                // escape them so BQ interprets the escape sequences.
                tv.sql_literal = escape_bq_string_literal(&tv.sql_literal);
                tv
            })
            .collect();
        cases.push(TypeTestCase {
            db_type: "STRING".to_string(),
            semantic_type: SemanticType::String,
            expected_arrow_type: DataType::Utf8,
            values: string_values,
            shape: TestShape::Scalar,
        });
    }

    // ========================================================================
    // BYTES → Binary
    // ========================================================================
    if let Ok(binary) = FixtureCategory::load("binary.toml") {
        let binary_constraints = Constraints::new().with_nullable(true);
        let filtered = binary.select_compatible(&binary_constraints);
        // Convert fixture hex values to BigQuery FROM_HEX() syntax
        let bytes_values: Vec<TestValue> = filtered
            .values
            .into_iter()
            .map(|v| {
                let sql_literal = if v.sql.to_uppercase() == "NULL" {
                    "NULL".to_string()
                } else if v.sql.is_empty() {
                    "FROM_HEX('')".to_string()
                } else {
                    format!("FROM_HEX('{}')", v.sql)
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
            db_type: "BYTES".to_string(),
            semantic_type: SemanticType::Binary,
            expected_arrow_type: DataType::Binary,
            values: bytes_values,
            shape: TestShape::Scalar,
        });
    }

    // ========================================================================
    // DATE → Date32
    // ========================================================================
    {
        let date_values = vec![
            TestValue {
                sql_literal: "DATE '1970-01-01'".to_string(),
                expected: ExpectedOutput::String("1970-01-01".to_string()),
                comparison: ComparisonMode::Exact,
                note: Some("Unix epoch date".to_string()),
            },
            TestValue {
                sql_literal: "DATE '2024-06-15'".to_string(),
                expected: ExpectedOutput::String("2024-06-15".to_string()),
                comparison: ComparisonMode::Exact,
                note: Some("Modern date".to_string()),
            },
            TestValue {
                sql_literal: "DATE '1969-07-20'".to_string(),
                expected: ExpectedOutput::String("1969-07-20".to_string()),
                comparison: ComparisonMode::Exact,
                note: Some("Before epoch date".to_string()),
            },
            TestValue {
                sql_literal: "DATE '2099-12-31'".to_string(),
                expected: ExpectedOutput::String("2099-12-31".to_string()),
                comparison: ComparisonMode::Exact,
                note: Some("Far future date".to_string()),
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
    }

    // ========================================================================
    // DATETIME → Timestamp(Microsecond, None)
    // ========================================================================
    if let Ok(timestamps) = FixtureCategory::load("timestamps.toml") {
        let ts_constraints = Constraints::new().with_nullable(true);
        let filtered = timestamps.select_compatible(&ts_constraints);
        // Convert fixture SQL to BigQuery DATETIME literal syntax
        let datetime_values: Vec<TestValue> = filtered
            .values
            .into_iter()
            .map(|v| {
                let sql_literal = if v.sql.to_uppercase() == "NULL" {
                    "NULL".to_string()
                } else {
                    // Strip surrounding quotes and wrap with DATETIME prefix
                    let inner = v.sql.trim_matches('\'');
                    format!("DATETIME '{}'", inner)
                };
                let expected = if v.expected_null.as_deref() == Some("NULL") {
                    ExpectedOutput::Null
                } else {
                    ExpectedOutput::String(v.expected.clone())
                };
                let comparison = match v.comparison {
                    crate::fixtures::Comparison::Exact => ComparisonMode::Exact,
                    crate::fixtures::Comparison::Approximate => ComparisonMode::Approx {
                        epsilon: v.epsilon.unwrap_or(1e-9),
                    },
                    crate::fixtures::Comparison::String => ComparisonMode::Exact,
                };
                TestValue {
                    sql_literal,
                    expected,
                    comparison,
                    note: v.note.clone(),
                }
            })
            .collect();
        cases.push(TypeTestCase {
            db_type: "DATETIME".to_string(),
            semantic_type: SemanticType::Timestamp { with_tz: false },
            expected_arrow_type: DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            values: datetime_values,
            shape: TestShape::Scalar,
        });
    }

    // ========================================================================
    // TIMESTAMP → Timestamp(Microsecond, Some("UTC"))
    // ========================================================================
    if let Ok(timestamps) = FixtureCategory::load("timestamps.toml") {
        let ts_constraints = Constraints::new().with_nullable(true);
        let filtered = timestamps.select_compatible(&ts_constraints);
        // Convert fixture SQL to BigQuery TIMESTAMP literal syntax (append UTC)
        let timestamp_values: Vec<TestValue> = filtered
            .values
            .into_iter()
            .map(|v| {
                let sql_literal = if v.sql.to_uppercase() == "NULL" {
                    "NULL".to_string()
                } else {
                    let inner = v.sql.trim_matches('\'');
                    format!("TIMESTAMP '{} UTC'", inner)
                };
                let expected = if v.expected_null.as_deref() == Some("NULL") {
                    ExpectedOutput::Null
                } else {
                    ExpectedOutput::String(v.expected.clone())
                };
                let comparison = match v.comparison {
                    crate::fixtures::Comparison::Exact => ComparisonMode::Exact,
                    crate::fixtures::Comparison::Approximate => ComparisonMode::Approx {
                        epsilon: v.epsilon.unwrap_or(1e-9),
                    },
                    crate::fixtures::Comparison::String => ComparisonMode::Exact,
                };
                TestValue {
                    sql_literal,
                    expected,
                    comparison,
                    note: v.note.clone(),
                }
            })
            .collect();
        cases.push(TypeTestCase {
            db_type: "TIMESTAMP".to_string(),
            semantic_type: SemanticType::Timestamp { with_tz: true },
            expected_arrow_type: DataType::Timestamp(
                arrow_schema::TimeUnit::Microsecond,
                Some("UTC".into()),
            ),
            values: timestamp_values,
            shape: TestShape::Scalar,
        });
    }

    // ========================================================================
    // TIME → Utf8
    // ========================================================================
    {
        let time_values = vec![
            TestValue {
                sql_literal: "TIME '00:00:00'".to_string(),
                expected: ExpectedOutput::String("00:00:00".to_string()),
                comparison: ComparisonMode::Exact,
                note: Some("Midnight".to_string()),
            },
            TestValue {
                sql_literal: "TIME '12:30:00'".to_string(),
                expected: ExpectedOutput::String("12:30:00".to_string()),
                comparison: ComparisonMode::Exact,
                note: Some("Midday time".to_string()),
            },
            TestValue {
                sql_literal: "TIME '23:59:59'".to_string(),
                expected: ExpectedOutput::String("23:59:59".to_string()),
                comparison: ComparisonMode::Exact,
                note: Some("End of day".to_string()),
            },
            TestValue {
                sql_literal: "TIME '10:30:45.123456'".to_string(),
                expected: ExpectedOutput::String("10:30:45.123456".to_string()),
                comparison: ComparisonMode::Exact,
                note: Some("Time with microseconds".to_string()),
            },
            TestValue {
                sql_literal: "NULL".to_string(),
                expected: ExpectedOutput::Null,
                comparison: ComparisonMode::Exact,
                note: Some("NULL time".to_string()),
            },
        ];
        cases.push(TypeTestCase {
            db_type: "TIME".to_string(),
            semantic_type: SemanticType::Time,
            expected_arrow_type: DataType::Utf8,
            values: time_values,
            shape: TestShape::Scalar,
        });
    }

    // ========================================================================
    // JSON → Utf8
    // ========================================================================
    if let Ok(json_fixtures) = FixtureCategory::load("json.toml") {
        let json_constraints = Constraints::new().with_nullable(true);
        let filtered = json_fixtures.select_compatible(&json_constraints);
        // Convert fixture SQL to BigQuery JSON literal syntax
        let json_values: Vec<TestValue> = filtered
            .values
            .into_iter()
            .map(|v| {
                let sql_literal = if v.sql.to_uppercase() == "NULL" {
                    "NULL".to_string()
                } else {
                    // Fixture sql is like '{"key":"val"}' — wrap with JSON prefix
                    format!("JSON {}", v.sql)
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
            db_type: "JSON".to_string(),
            semantic_type: SemanticType::Json,
            expected_arrow_type: DataType::Utf8,
            values: json_values,
            shape: TestShape::Scalar,
        });
    }

    cases
}

// ============================================================================
// Test Runner
// ============================================================================

/// Run a single type test case against BigQuery.
///
/// Uses the BigQuery client directly for DDL/DML, then validates via
/// NativeFetcher + CapturingBatchWriter.
async fn run_test_case(
    client: &Client,
    project_id: &str,
    dataset: &str,
    source: &Source,
    secrets: &SecretManager,
    case: &TypeTestCase,
) -> TypeTestResult {
    let table_name = format!(
        "tc_bq_{}",
        case.db_type
            .replace(['(', ')', ',', ' ', '\'', '<', '>'], "_")
            .to_lowercase()
    );
    let fq_table = table_ref(project_id, dataset, &table_name);

    // Skip if no test values
    if case.values.is_empty() {
        return TypeTestResult::skipped(&case.db_type, "No compatible test values");
    }

    // Drop table if exists (cleanup from previous run).
    // Ignore errors — BigQuery returns permission denied even when the table
    // doesn't exist if the SA lacks dataset-level delete permissions.
    let drop_sql = format!("DROP TABLE IF EXISTS {}", fq_table);
    let _ = execute_bq_sql(client, project_id, &drop_sql).await;

    // Create table (use CREATE OR REPLACE to handle leftover tables from failed runs)
    let create_sql = format!(
        "CREATE OR REPLACE TABLE {} (id INT64, val {})",
        fq_table, case.db_type
    );
    if let Err(e) = execute_bq_sql(client, project_id, &create_sql).await {
        return TypeTestResult::failed(
            &case.db_type,
            FailureReason::FetchError {
                message: format!("Failed to create table '{}': {}", create_sql, e),
            },
        );
    }

    // Insert all test values in a single DML statement
    let value_rows: Vec<String> = case
        .values
        .iter()
        .enumerate()
        .map(|(idx, tv)| format!("({}, {})", idx, tv.sql_literal))
        .collect();
    let insert_sql = format!(
        "INSERT INTO {} (id, val) VALUES {}",
        fq_table,
        value_rows.join(", ")
    );
    if let Err(e) = execute_bq_sql(client, project_id, &insert_sql).await {
        // Cleanup on failure
        let _ = execute_bq_sql(client, project_id, &drop_sql).await;
        return TypeTestResult::failed(
            &case.db_type,
            FailureReason::FetchError {
                message: format!("Failed to insert values: {}", e),
            },
        );
    }

    // Fetch using NativeFetcher with CapturingBatchWriter
    let fetcher = NativeFetcher::new();
    let mut writer = CapturingBatchWriter::new();
    let fetch_result = fetcher
        .fetch_table(source, secrets, None, dataset, &table_name, &mut writer)
        .await;

    // Cleanup table regardless of fetch result
    let _ = execute_bq_sql(client, project_id, &drop_sql).await;

    if let Err(e) = fetch_result {
        return TypeTestResult::failed(
            &case.db_type,
            FailureReason::FetchError {
                message: format!("fetch_table failed: {}", e),
            },
        );
    }

    // Validate schema — check the 'val' column type
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
async fn test_bigquery_type_coverage() {
    // Check environment variables — skip gracefully if not set
    let (key_path, project_id, dataset) = match bigquery_env() {
        Some(env) => env,
        None => {
            println!(
                "SKIPPED: BigQuery type coverage tests require BQ_SERVICE_ACCOUNT_KEY_PATH, \
                 BQ_PROJECT_ID, and BQ_DATASET environment variables"
            );
            return;
        }
    };

    // Read service account key from file
    let key_json =
        std::fs::read_to_string(&key_path).expect("Failed to read service account key file");

    // Create BigQuery client directly for DDL/DML setup
    let sa_key = serde_json::from_str(&key_json).expect("Invalid service account key JSON");
    let client = Client::from_service_account_key(sa_key, false)
        .await
        .expect("Failed to create BigQuery client");

    // Create temp dir for secret manager
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let secrets = create_test_secret_manager(&temp_dir).await;

    // Store the service account key as a secret
    let secret_id = secrets
        .create(BQ_SECRET_NAME, key_json.as_bytes())
        .await
        .expect("Failed to store BigQuery credential secret");

    // Create the Source with the proper structure
    let source = Source::Bigquery {
        project_id: project_id.clone(),
        dataset: Some(dataset.clone()),
        region: "us".to_string(),
        credential: Credential::SecretRef { id: secret_id },
    };

    // Build test cases
    let test_cases = build_bigquery_test_cases();
    println!(
        "\nRunning {} BigQuery type test cases...\n",
        test_cases.len()
    );

    // Run all test cases
    let mut report = TestReport::new("bigquery");
    for case in &test_cases {
        println!("  Testing {}...", case.db_type);
        let result = run_test_case(&client, &project_id, &dataset, &source, &secrets, case).await;
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
    println!("\n--- BigQuery Test Summary ---");
    println!("  Passed:  {}", report.passed_count());
    println!("  Failed:  {}", report.failed_count());
    println!("  Skipped: {}", report.skipped_count());

    // Assert all passed
    report.assert_all_passed();
}
