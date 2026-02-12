//! Test harness for type coverage tests.
//!
//! This module provides infrastructure for running type conversion tests against
//! different database backends. It defines semantic types (what the data means),
//! test shapes (how values are structured), and comparison modes for validation.

use std::sync::Arc;

use arrow_schema::DataType;
use rust_decimal::Decimal;
use tempfile::TempDir;

use runtimedb::catalog::{CatalogManager, SqliteCatalogManager};
pub use runtimedb::secrets::SecretManager;
use runtimedb::secrets::{EncryptedCatalogBackend, ENCRYPTED_PROVIDER_TYPE};

use crate::fixtures::{Comparison, FixtureValue};

// ============================================================================
// Semantic Types
// ============================================================================

/// Semantic meaning of a database type, independent of representation.
///
/// This describes what the data represents conceptually, allowing us to map
/// different database-specific types to a common understanding.
#[derive(Debug, Clone, PartialEq)]
pub enum SemanticType {
    /// Boolean true/false values.
    Boolean,
    /// Integer numeric values.
    Integer {
        /// Whether the integer is signed.
        signed: bool,
        /// Bit width (8, 16, 32, 64, 128).
        bits: u8,
    },
    /// Floating-point numeric values.
    Float {
        /// Bit width (16, 32, 64).
        bits: u8,
    },
    /// Exact decimal numeric values with precision and scale.
    Decimal,
    /// Unicode text strings.
    String,
    /// Raw binary data.
    Binary,
    /// Point in time (date + time).
    Timestamp {
        /// Whether timezone information is preserved.
        with_tz: bool,
    },
    /// Calendar date without time.
    Date,
    /// Time of day without date.
    Time,
    /// Duration or time span.
    Interval,
    /// JSON data (object, array, or primitive).
    Json,
    /// Universally unique identifier.
    #[allow(dead_code)] // Not yet supported by production fetch code
    Uuid,
    /// Array/list of elements.
    #[allow(dead_code)] // Not yet supported by production fetch code
    Array {
        /// Type of elements in the array.
        element: Box<SemanticType>,
    },
    /// Composite struct/record type.
    #[allow(dead_code)] // Reserved for future struct type tests
    Struct,
    /// Network address types (IP, CIDR, MAC) - backend-specific.
    #[allow(dead_code)] // Not yet supported by production fetch code
    Network,
    /// Geometric types (point, line, polygon) - backend-specific.
    Geometric,
    /// Range types - backend-specific.
    #[allow(dead_code)] // Not yet supported by production fetch code
    Range,
}

// ============================================================================
// Test Shape
// ============================================================================

/// Shape of test data, affecting insert/select strategy.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum TestShape {
    /// Single scalar value (default).
    #[default]
    Scalar,
    /// Array/list of values.
    #[allow(dead_code)] // Reserved for future array shape tests
    Array,
    /// Composite struct/record.
    #[allow(dead_code)] // Reserved for future struct shape tests
    Struct,
}

// ============================================================================
// Comparison Mode
// ============================================================================

/// Strategy for comparing expected vs actual values.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum ComparisonMode {
    /// Exact string equality.
    #[default]
    Exact,
    /// Approximate numeric comparison within epsilon.
    Approx {
        /// Maximum allowed difference.
        epsilon: f64,
    },
    /// Special value handling (NaN, Infinity).
    Special,
    /// Semantic JSON comparison (order-independent for objects).
    Json,
}

// ============================================================================
// Expected Output
// ============================================================================

/// Expected output value from a test.
#[derive(Debug, Clone, PartialEq)]
pub enum ExpectedOutput {
    /// Expected string value.
    String(String),
    /// Expected integer value.
    Int(i64),
    /// Expected floating-point value.
    Float(f64),
    /// Expected boolean value.
    Bool(bool),
    /// Expected NULL value.
    Null,
    /// Expected error with message pattern.
    Error(String),
}

// ============================================================================
// Test Value
// ============================================================================

/// A single test value with SQL representation and expected output.
#[derive(Debug, Clone)]
pub struct TestValue {
    /// SQL literal or expression to insert/select.
    pub sql_literal: String,
    /// Expected output after round-trip.
    pub expected: ExpectedOutput,
    /// How to compare expected vs actual.
    pub comparison: ComparisonMode,
    /// Optional note about this test case.
    pub note: Option<String>,
}

impl From<FixtureValue> for TestValue {
    fn from(fixture: FixtureValue) -> Self {
        // Determine expected output
        let expected = if fixture.expect_error {
            ExpectedOutput::Error(fixture.expected.clone())
        } else if fixture.requires.nullable == Some(true)
            && (fixture.expected.is_empty() || fixture.expected.to_lowercase() == "null")
            && fixture.sql.to_uppercase() == "NULL"
        {
            ExpectedOutput::Null
        } else {
            // Try to parse as different types based on the expected string
            ExpectedOutput::String(fixture.expected.clone())
        };

        // Convert comparison mode
        let comparison = match fixture.comparison {
            Comparison::Exact => ComparisonMode::Exact,
            Comparison::Approximate => {
                let epsilon = fixture.epsilon.unwrap_or(1e-9);
                ComparisonMode::Approx { epsilon }
            }
            Comparison::String => ComparisonMode::Exact,
        };

        Self {
            sql_literal: fixture.sql,
            expected,
            comparison,
            note: fixture.note,
        }
    }
}

// ============================================================================
// Type Test Case
// ============================================================================

/// A complete test case for a database type.
#[derive(Debug, Clone)]
pub struct TypeTestCase {
    /// SQL type name (e.g., "NUMERIC(10,2)", "VARCHAR(255)").
    pub db_type: String,
    /// Semantic meaning of this type.
    pub semantic_type: SemanticType,
    /// Expected Arrow type after conversion.
    pub expected_arrow_type: DataType,
    /// Test values to verify.
    pub values: Vec<TestValue>,
    /// Shape of the test data.
    #[allow(dead_code)] // Reserved for future non-scalar test shapes
    pub shape: TestShape,
    /// Optional SQL to execute before table creation (e.g., loading extensions).
    pub setup_sql: Option<String>,
}

// ============================================================================
// Test Results
// ============================================================================

/// Status of a single type test.
#[derive(Debug, Clone, PartialEq)]
pub enum TestStatus {
    /// Test passed successfully.
    Passed,
    /// Test failed with a reason.
    Failed(FailureReason),
    /// Test was skipped with a reason.
    Skipped(String),
}

/// Reason why a test failed.
#[derive(Debug, Clone, PartialEq)]
pub enum FailureReason {
    /// Arrow type did not match expected.
    ArrowTypeMismatch {
        /// Expected Arrow type.
        expected: DataType,
        /// Actual Arrow type.
        actual: DataType,
    },
    /// Value did not match expected output.
    ValueMismatch {
        /// Index of the failing value.
        index: usize,
        /// Input SQL that produced the value.
        input_sql: String,
        /// Expected output.
        expected: String,
        /// Actual output.
        actual: String,
    },
    /// Expected non-null but got null.
    #[allow(dead_code)] // Reserved for stricter null checking
    UnexpectedNull {
        /// Index of the failing value.
        index: usize,
        /// Input SQL that produced null.
        input_sql: String,
    },
    /// Expected null but got a value.
    #[allow(dead_code)] // Reserved for stricter null checking
    UnexpectedNonNull {
        /// Index of the failing value.
        index: usize,
        /// Input SQL that produced the value.
        input_sql: String,
        /// Actual value received.
        actual: String,
    },
    /// Error occurred while fetching data.
    FetchError {
        /// Error message.
        message: String,
    },
}

/// Result of a single type test.
#[derive(Debug, Clone)]
pub struct TypeTestResult {
    /// The database type being tested.
    pub db_type: String,
    /// Status of the test.
    pub status: TestStatus,
}

impl TypeTestResult {
    /// Create a passed result.
    pub fn passed(db_type: impl Into<String>) -> Self {
        Self {
            db_type: db_type.into(),
            status: TestStatus::Passed,
        }
    }

    /// Create a failed result.
    pub fn failed(db_type: impl Into<String>, reason: FailureReason) -> Self {
        Self {
            db_type: db_type.into(),
            status: TestStatus::Failed(reason),
        }
    }

    /// Create a skipped result.
    pub fn skipped(db_type: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            db_type: db_type.into(),
            status: TestStatus::Skipped(reason.into()),
        }
    }

    /// Check if the test passed.
    pub fn is_passed(&self) -> bool {
        matches!(self.status, TestStatus::Passed)
    }

    /// Check if the test failed.
    pub fn is_failed(&self) -> bool {
        matches!(self.status, TestStatus::Failed(_))
    }

    /// Check if the test was skipped.
    pub fn is_skipped(&self) -> bool {
        matches!(self.status, TestStatus::Skipped(_))
    }
}

// ============================================================================
// Test Report
// ============================================================================

/// Aggregated report of all type tests for a backend.
#[derive(Debug, Clone)]
pub struct TestReport {
    /// Backend name (e.g., "postgres", "mysql", "duckdb").
    pub backend: String,
    /// Results for each type test.
    pub results: Vec<TypeTestResult>,
}

impl TestReport {
    /// Create a new empty test report.
    pub fn new(backend: impl Into<String>) -> Self {
        Self {
            backend: backend.into(),
            results: Vec::new(),
        }
    }

    /// Add a test result to the report.
    pub fn add_result(&mut self, result: TypeTestResult) {
        self.results.push(result);
    }

    /// Get the number of passed tests.
    pub fn passed_count(&self) -> usize {
        self.results.iter().filter(|r| r.is_passed()).count()
    }

    /// Get the number of failed tests.
    pub fn failed_count(&self) -> usize {
        self.results.iter().filter(|r| r.is_failed()).count()
    }

    /// Get the number of skipped tests.
    pub fn skipped_count(&self) -> usize {
        self.results.iter().filter(|r| r.is_skipped()).count()
    }

    /// Get all failed test results.
    pub fn failures(&self) -> Vec<&TypeTestResult> {
        self.results.iter().filter(|r| r.is_failed()).collect()
    }

    /// Assert that all tests passed, panicking with details if any failed.
    pub fn assert_all_passed(&self) {
        let failures = self.failures();
        if failures.is_empty() {
            return;
        }

        let mut message = format!(
            "\n{} type test(s) failed for backend '{}':\n",
            failures.len(),
            self.backend
        );

        for result in failures {
            message.push_str(&format!("\n  - {}: ", result.db_type));
            if let TestStatus::Failed(reason) = &result.status {
                match reason {
                    FailureReason::ArrowTypeMismatch { expected, actual } => {
                        message.push_str(&format!(
                            "Arrow type mismatch\n    expected: {:?}\n    actual:   {:?}",
                            expected, actual
                        ));
                    }
                    FailureReason::ValueMismatch {
                        index,
                        input_sql,
                        expected,
                        actual,
                    } => {
                        message.push_str(&format!(
                            "Value mismatch at index {}\n    input:    {}\n    expected: {}\n    actual:   {}",
                            index, input_sql, expected, actual
                        ));
                    }
                    FailureReason::UnexpectedNull { index, input_sql } => {
                        message.push_str(&format!(
                            "Unexpected NULL at index {}\n    input: {}",
                            index, input_sql
                        ));
                    }
                    FailureReason::UnexpectedNonNull {
                        index,
                        input_sql,
                        actual,
                    } => {
                        message.push_str(&format!(
                            "Expected NULL but got value at index {}\n    input:  {}\n    actual: {}",
                            index, input_sql, actual
                        ));
                    }
                    FailureReason::FetchError { message: err } => {
                        message.push_str(&format!("Fetch error: {}", err));
                    }
                }
            }
            message.push('\n');
        }

        panic!("{}", message);
    }
}

// ============================================================================
// Value Comparison
// ============================================================================

/// Compare an expected output with an actual string value.
///
/// Returns `Ok(())` if the values match according to the comparison mode,
/// or `Err(message)` describing the mismatch.
pub fn compare_values(
    expected: &ExpectedOutput,
    actual: Option<&str>,
    mode: &ComparisonMode,
) -> Result<(), String> {
    match (expected, actual) {
        // NULL handling
        (ExpectedOutput::Null, None) => Ok(()),
        (ExpectedOutput::Null, Some(val)) => Err(format!("Expected NULL but got '{}'", val)),
        (_, None) => Err("Expected value but got NULL".to_string()),

        // Error handling - we can't compare errors with actual values
        (ExpectedOutput::Error(pattern), Some(val)) => Err(format!(
            "Expected error '{}' but got value '{}'",
            pattern, val
        )),

        // String comparison
        (ExpectedOutput::String(expected_str), Some(actual_str)) => {
            compare_strings(expected_str, actual_str, mode)
        }

        // Integer comparison
        (ExpectedOutput::Int(expected_int), Some(actual_str)) => {
            let actual_int: i64 = actual_str
                .parse()
                .map_err(|_| format!("Cannot parse '{}' as integer", actual_str))?;
            if *expected_int == actual_int {
                Ok(())
            } else {
                Err(format!(
                    "Integer mismatch: expected {} but got {}",
                    expected_int, actual_int
                ))
            }
        }

        // Float comparison
        (ExpectedOutput::Float(expected_float), Some(actual_str)) => {
            let actual_float: f64 = actual_str
                .parse()
                .map_err(|_| format!("Cannot parse '{}' as float", actual_str))?;

            match mode {
                ComparisonMode::Approx { epsilon } => {
                    if (expected_float - actual_float).abs() <= *epsilon {
                        Ok(())
                    } else {
                        Err(format!(
                            "Float mismatch: expected {} but got {} (epsilon: {})",
                            expected_float, actual_float, epsilon
                        ))
                    }
                }
                ComparisonMode::Special => {
                    // Handle NaN and Infinity
                    let matches = (expected_float.is_nan() && actual_float.is_nan())
                        || (expected_float.is_infinite()
                            && actual_float.is_infinite()
                            && expected_float.signum() == actual_float.signum())
                        || (*expected_float == actual_float);
                    if matches {
                        Ok(())
                    } else {
                        Err(format!(
                            "Float mismatch: expected {} but got {}",
                            expected_float, actual_float
                        ))
                    }
                }
                _ => {
                    if *expected_float == actual_float {
                        Ok(())
                    } else {
                        Err(format!(
                            "Float mismatch: expected {} but got {}",
                            expected_float, actual_float
                        ))
                    }
                }
            }
        }

        // Boolean comparison
        (ExpectedOutput::Bool(expected_bool), Some(actual_str)) => {
            let actual_bool = match actual_str.to_lowercase().as_str() {
                "true" | "t" | "1" | "yes" | "on" => true,
                "false" | "f" | "0" | "no" | "off" => false,
                _ => return Err(format!("Cannot parse '{}' as boolean", actual_str)),
            };
            if *expected_bool == actual_bool {
                Ok(())
            } else {
                Err(format!(
                    "Boolean mismatch: expected {} but got {}",
                    expected_bool, actual_bool
                ))
            }
        }
    }
}

/// Compare two strings according to the comparison mode.
fn compare_strings(expected: &str, actual: &str, mode: &ComparisonMode) -> Result<(), String> {
    match mode {
        ComparisonMode::Exact => {
            if expected == actual {
                Ok(())
            } else {
                Err(format!(
                    "String mismatch: expected '{}' but got '{}'",
                    expected, actual
                ))
            }
        }
        ComparisonMode::Approx { epsilon } => {
            // Try to parse as high-precision decimals first to avoid precision loss
            if let (Ok(expected_dec), Ok(actual_dec)) =
                (expected.parse::<Decimal>(), actual.parse::<Decimal>())
            {
                // Use rust_decimal's abs_diff for high-precision comparison
                let diff = if expected_dec > actual_dec {
                    expected_dec - actual_dec
                } else {
                    actual_dec - expected_dec
                };

                // Convert epsilon to Decimal for comparison
                let epsilon_dec = Decimal::try_from(*epsilon)
                    .map_err(|_| format!("Cannot convert epsilon {} to Decimal", epsilon))?;

                if diff <= epsilon_dec {
                    Ok(())
                } else {
                    Err(format!(
                        "Approximate mismatch: expected {} but got {} (epsilon: {}, diff: {})",
                        expected_dec, actual_dec, epsilon_dec, diff
                    ))
                }
            } else {
                // Fall back to f64 comparison for non-decimal strings (e.g., "NaN", "Infinity")
                let expected_float: f64 = expected
                    .parse()
                    .map_err(|_| format!("Cannot parse expected '{}' as numeric", expected))?;
                let actual_float: f64 = actual
                    .parse()
                    .map_err(|_| format!("Cannot parse actual '{}' as numeric", actual))?;

                if (expected_float - actual_float).abs() <= *epsilon {
                    Ok(())
                } else {
                    Err(format!(
                        "Approximate mismatch: expected {} but got {} (epsilon: {})",
                        expected_float, actual_float, epsilon
                    ))
                }
            }
        }
        ComparisonMode::Special => {
            // Handle special string representations of NaN, Infinity, etc.
            let expected_lower = expected.to_lowercase();
            let actual_lower = actual.to_lowercase();

            // Check for NaN
            if is_nan_string(&expected_lower) && is_nan_string(&actual_lower) {
                return Ok(());
            }

            // Check for positive infinity
            if is_pos_infinity_string(&expected_lower) && is_pos_infinity_string(&actual_lower) {
                return Ok(());
            }

            // Check for negative infinity
            if is_neg_infinity_string(&expected_lower) && is_neg_infinity_string(&actual_lower) {
                return Ok(());
            }

            // Fall back to exact comparison
            if expected == actual {
                Ok(())
            } else {
                Err(format!(
                    "Special value mismatch: expected '{}' but got '{}'",
                    expected, actual
                ))
            }
        }
        ComparisonMode::Json => {
            // Parse both as JSON and compare semantically
            let expected_json: serde_json::Value = serde_json::from_str(expected)
                .map_err(|e| format!("Cannot parse expected '{}' as JSON: {}", expected, e))?;
            let actual_json: serde_json::Value = serde_json::from_str(actual)
                .map_err(|e| format!("Cannot parse actual '{}' as JSON: {}", actual, e))?;

            if expected_json == actual_json {
                Ok(())
            } else {
                Err(format!(
                    "JSON mismatch:\n  expected: {}\n  actual:   {}",
                    expected, actual
                ))
            }
        }
    }
}

/// Check if a string represents NaN.
fn is_nan_string(s: &str) -> bool {
    matches!(s, "nan" | "NaN" | "NAN")
}

/// Check if a string represents positive infinity.
fn is_pos_infinity_string(s: &str) -> bool {
    matches!(s, "inf" | "infinity" | "+inf" | "+infinity" | "Infinity")
}

/// Check if a string represents negative infinity.
fn is_neg_infinity_string(s: &str) -> bool {
    matches!(s, "-inf" | "-infinity" | "-Infinity")
}

// ============================================================================
// Arrow Value Extraction and Comparison
// ============================================================================

use datafusion::arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, StringArray, Time64MicrosecondArray,
    TimestampMicrosecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use datafusion::arrow::record_batch::RecordBatch;

/// Represents an extracted Arrow value for comparison.
///
/// This enum provides a unified representation of Arrow array values
/// that can be compared against expected test outputs.
#[derive(Debug, Clone, PartialEq)]
pub enum ArrowValue {
    /// Null value.
    Null,
    /// Boolean value.
    Boolean(bool),
    /// Signed 8-bit integer.
    Int8(i8),
    /// Signed 16-bit integer.
    Int16(i16),
    /// Signed 32-bit integer.
    Int32(i32),
    /// Signed 64-bit integer.
    Int64(i64),
    /// Unsigned 8-bit integer.
    UInt8(u8),
    /// Unsigned 16-bit integer.
    UInt16(u16),
    /// Unsigned 32-bit integer.
    UInt32(u32),
    /// Unsigned 64-bit integer.
    UInt64(u64),
    /// 32-bit floating point.
    Float32(f32),
    /// 64-bit floating point.
    Float64(f64),
    /// Decimal128 value with precision and scale.
    Decimal128 {
        /// The raw i128 value (scaled).
        value: i128,
        /// Precision (total digits).
        precision: u8,
        /// Scale (digits after decimal point).
        scale: i8,
    },
    /// UTF-8 string.
    Utf8(String),
    /// Binary data.
    Binary(Vec<u8>),
    /// Date as days since epoch.
    Date32(i32),
    /// Time as microseconds since midnight.
    Time64Microsecond(i64),
    /// Timestamp as microseconds since epoch with optional timezone.
    TimestampMicrosecond {
        /// Microseconds since Unix epoch.
        value: i64,
        /// Optional timezone string.
        timezone: Option<String>,
    },
    /// Interval value stored as months, days, and nanoseconds.
    IntervalMonthDayNano {
        /// Months component.
        months: i32,
        /// Days component.
        days: i32,
        /// Nanoseconds component.
        nanos: i64,
    },
}

impl std::fmt::Display for ArrowValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArrowValue::Null => write!(f, "NULL"),
            ArrowValue::Boolean(v) => write!(f, "{}", v),
            ArrowValue::Int8(v) => write!(f, "{}", v),
            ArrowValue::Int16(v) => write!(f, "{}", v),
            ArrowValue::Int32(v) => write!(f, "{}", v),
            ArrowValue::Int64(v) => write!(f, "{}", v),
            ArrowValue::UInt8(v) => write!(f, "{}", v),
            ArrowValue::UInt16(v) => write!(f, "{}", v),
            ArrowValue::UInt32(v) => write!(f, "{}", v),
            ArrowValue::UInt64(v) => write!(f, "{}", v),
            ArrowValue::Float32(v) => write!(f, "{}", v),
            ArrowValue::Float64(v) => write!(f, "{}", v),
            ArrowValue::Decimal128 {
                value,
                precision: _,
                scale,
            } => {
                // Format decimal with proper scale
                if *scale == 0 {
                    write!(f, "{}", value)
                } else {
                    let divisor = 10i128.pow(*scale as u32);
                    let integer_part = value / divisor;
                    let fractional_part = (value % divisor).abs();
                    write!(
                        f,
                        "{}.{:0>width$}",
                        integer_part,
                        fractional_part,
                        width = *scale as usize
                    )
                }
            }
            ArrowValue::Utf8(v) => write!(f, "{}", v),
            ArrowValue::Binary(v) => {
                // Format as hex
                write!(f, "\\x")?;
                for byte in v {
                    write!(f, "{:02X}", byte)?;
                }
                Ok(())
            }
            ArrowValue::Date32(days) => {
                // Convert days since epoch to date string
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                if let Some(date) = epoch.checked_add_signed(chrono::Duration::days(*days as i64)) {
                    write!(f, "{}", date.format("%Y-%m-%d"))
                } else {
                    write!(f, "<invalid date: {} days>", days)
                }
            }
            ArrowValue::Time64Microsecond(micros) => {
                let secs = micros / 1_000_000;
                let remaining_micros = micros % 1_000_000;
                let hours = secs / 3600;
                let mins = (secs % 3600) / 60;
                let secs = secs % 60;
                if remaining_micros > 0 {
                    write!(
                        f,
                        "{:02}:{:02}:{:02}.{:06}",
                        hours, mins, secs, remaining_micros
                    )
                } else {
                    write!(f, "{:02}:{:02}:{:02}", hours, mins, secs)
                }
            }
            ArrowValue::TimestampMicrosecond { value, timezone } => {
                let secs = value / 1_000_000;
                let micros = (value % 1_000_000).abs();
                if let Some(dt) = chrono::DateTime::from_timestamp(secs, (micros * 1000) as u32) {
                    if let Some(tz) = timezone {
                        write!(f, "{} {}", dt.format("%Y-%m-%d %H:%M:%S%.6f"), tz)
                    } else {
                        write!(f, "{}", dt.format("%Y-%m-%d %H:%M:%S%.6f"))
                    }
                } else {
                    write!(f, "<invalid timestamp: {} micros>", value)
                }
            }
            ArrowValue::IntervalMonthDayNano {
                months,
                days,
                nanos,
            } => {
                let mut parts = Vec::new();
                if *months != 0 {
                    if *months == 1 || *months == -1 {
                        parts.push(format!("{} month", months));
                    } else {
                        parts.push(format!("{} months", months));
                    }
                }
                if *days != 0 {
                    if *days == 1 || *days == -1 {
                        parts.push(format!("{} day", days));
                    } else {
                        parts.push(format!("{} days", days));
                    }
                }
                if *nanos != 0 {
                    // Convert nanos to hours:mins:secs format
                    let total_secs = nanos / 1_000_000_000;
                    let hours = total_secs / 3600;
                    let mins = (total_secs % 3600) / 60;
                    let secs = total_secs % 60;
                    if hours != 0 || mins != 0 || secs != 0 {
                        parts.push(format!("{:02}:{:02}:{:02}", hours, mins, secs));
                    }
                }
                if parts.is_empty() {
                    write!(f, "0")
                } else {
                    write!(f, "{}", parts.join(" "))
                }
            }
        }
    }
}

/// Extract a value from an Arrow array at a given index.
///
/// Returns `ArrowValue::Null` if the value at the index is null.
pub fn extract_arrow_value(array: &dyn Array, index: usize) -> ArrowValue {
    if array.is_null(index) {
        return ArrowValue::Null;
    }

    match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            ArrowValue::Boolean(arr.value(index))
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            ArrowValue::Int8(arr.value(index))
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            ArrowValue::Int16(arr.value(index))
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            ArrowValue::Int32(arr.value(index))
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            ArrowValue::Int64(arr.value(index))
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            ArrowValue::UInt8(arr.value(index))
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            ArrowValue::UInt16(arr.value(index))
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            ArrowValue::UInt32(arr.value(index))
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            ArrowValue::UInt64(arr.value(index))
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            ArrowValue::Float32(arr.value(index))
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            ArrowValue::Float64(arr.value(index))
        }
        DataType::Decimal128(precision, scale) => {
            let arr = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            ArrowValue::Decimal128 {
                value: arr.value(index),
                precision: *precision,
                scale: *scale,
            }
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            ArrowValue::Utf8(arr.value(index).to_string())
        }
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            ArrowValue::Binary(arr.value(index).to_vec())
        }
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            ArrowValue::Date32(arr.value(index))
        }
        DataType::Time64(datafusion::arrow::datatypes::TimeUnit::Microsecond) => {
            let arr = array
                .as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .unwrap();
            ArrowValue::Time64Microsecond(arr.value(index))
        }
        DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, tz) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            ArrowValue::TimestampMicrosecond {
                value: arr.value(index),
                timezone: tz.as_ref().map(|s| s.to_string()),
            }
        }
        DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::MonthDayNano) => {
            use datafusion::arrow::array::IntervalMonthDayNanoArray;
            let arr = array
                .as_any()
                .downcast_ref::<IntervalMonthDayNanoArray>()
                .unwrap();
            // Arrow's IntervalMonthDayNano is a struct with months, days, nanoseconds
            let val = arr.value(index);
            ArrowValue::IntervalMonthDayNano {
                months: val.months,
                days: val.days,
                nanos: val.nanoseconds,
            }
        }
        // For unsupported types, try to get as string if possible
        other => {
            eprintln!(
                "[type_coverage] Warning: extract_arrow_value encountered unsupported Arrow type: {:?}",
                other
            );
            // Try StringArray as fallback for Utf8-like types
            if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
                ArrowValue::Utf8(arr.value(index).to_string())
            } else {
                ArrowValue::Utf8(format!("<unsupported type: {:?}>", array.data_type()))
            }
        }
    }
}

/// Compare an ArrowValue against an ExpectedOutput.
///
/// Returns `Ok(())` if the values match according to the comparison mode,
/// or `Err(message)` describing the mismatch.
pub fn compare_arrow_value(
    actual: &ArrowValue,
    expected: &ExpectedOutput,
    mode: &ComparisonMode,
    _semantic_type: &SemanticType,
) -> Result<(), String> {
    match (actual, expected) {
        // NULL handling
        (ArrowValue::Null, ExpectedOutput::Null) => Ok(()),
        (ArrowValue::Null, _) => Err("Expected value but got NULL".to_string()),
        (_, ExpectedOutput::Null) => Err(format!("Expected NULL but got {}", actual)),

        // Error handling - errors can't be compared with actual values
        (_, ExpectedOutput::Error(pattern)) => Err(format!(
            "Expected error '{}' but got value {}",
            pattern, actual
        )),

        // Boolean comparison
        (ArrowValue::Boolean(actual_bool), ExpectedOutput::Bool(expected_bool)) => {
            if actual_bool == expected_bool {
                Ok(())
            } else {
                Err(format!(
                    "Boolean mismatch: expected {} but got {}",
                    expected_bool, actual_bool
                ))
            }
        }
        (ArrowValue::Boolean(actual_bool), ExpectedOutput::String(s)) => {
            // Handle string representations of booleans
            let expected_bool = match s.to_lowercase().as_str() {
                "true" | "t" | "1" | "yes" | "on" => true,
                "false" | "f" | "0" | "no" | "off" => false,
                _ => return Err(format!("Cannot parse '{}' as boolean", s)),
            };
            if *actual_bool == expected_bool {
                Ok(())
            } else {
                Err(format!(
                    "Boolean mismatch: expected {} but got {}",
                    expected_bool, actual_bool
                ))
            }
        }

        // Integer comparisons
        (ArrowValue::Int8(v), ExpectedOutput::Int(expected)) => compare_int(*v as i64, *expected),
        (ArrowValue::Int16(v), ExpectedOutput::Int(expected)) => compare_int(*v as i64, *expected),
        (ArrowValue::Int32(v), ExpectedOutput::Int(expected)) => compare_int(*v as i64, *expected),
        (ArrowValue::Int64(v), ExpectedOutput::Int(expected)) => compare_int(*v, *expected),
        (ArrowValue::UInt8(v), ExpectedOutput::Int(expected)) => compare_int(*v as i64, *expected),
        (ArrowValue::UInt16(v), ExpectedOutput::Int(expected)) => compare_int(*v as i64, *expected),
        (ArrowValue::UInt32(v), ExpectedOutput::Int(expected)) => compare_int(*v as i64, *expected),
        (ArrowValue::UInt64(v), ExpectedOutput::Int(expected)) => {
            if *expected >= 0 && *v == *expected as u64 {
                Ok(())
            } else {
                Err(format!(
                    "Integer mismatch: expected {} but got {}",
                    expected, v
                ))
            }
        }

        // Integer from string
        (ArrowValue::Int8(v), ExpectedOutput::String(s)) => compare_int_string(*v as i64, s),
        (ArrowValue::Int16(v), ExpectedOutput::String(s)) => compare_int_string(*v as i64, s),
        (ArrowValue::Int32(v), ExpectedOutput::String(s)) => compare_int_string(*v as i64, s),
        (ArrowValue::Int64(v), ExpectedOutput::String(s)) => compare_int_string(*v, s),
        (ArrowValue::UInt8(v), ExpectedOutput::String(s)) => compare_uint_string(*v as u64, s),
        (ArrowValue::UInt16(v), ExpectedOutput::String(s)) => compare_uint_string(*v as u64, s),
        (ArrowValue::UInt32(v), ExpectedOutput::String(s)) => compare_uint_string(*v as u64, s),
        (ArrowValue::UInt64(v), ExpectedOutput::String(s)) => compare_uint_string(*v, s),

        // Float comparisons
        (ArrowValue::Float32(v), ExpectedOutput::Float(expected)) => {
            compare_float(*v as f64, *expected, mode)
        }
        (ArrowValue::Float64(v), ExpectedOutput::Float(expected)) => {
            compare_float(*v, *expected, mode)
        }
        (ArrowValue::Float32(v), ExpectedOutput::String(s)) => {
            compare_float_string(*v as f64, s, mode)
        }
        (ArrowValue::Float64(v), ExpectedOutput::String(s)) => compare_float_string(*v, s, mode),

        // Decimal comparison
        (
            ArrowValue::Decimal128 {
                value,
                precision: _,
                scale,
            },
            ExpectedOutput::String(expected_str),
        ) => compare_decimal128(*value, *scale, expected_str, mode),

        // String comparison - use JSON semantics if semantic type is JSON
        (ArrowValue::Utf8(actual_str), ExpectedOutput::String(expected_str)) => {
            if matches!(_semantic_type, SemanticType::Json) {
                compare_strings(expected_str, actual_str, &ComparisonMode::Json)
            } else {
                compare_strings(expected_str, actual_str, mode)
            }
        }

        // Binary comparison (expected as hex string)
        (ArrowValue::Binary(actual_bytes), ExpectedOutput::String(expected_hex)) => {
            compare_binary(actual_bytes, expected_hex)
        }

        // Date comparison
        (ArrowValue::Date32(days), ExpectedOutput::String(expected_str)) => {
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            if let Some(date) = epoch.checked_add_signed(chrono::Duration::days(*days as i64)) {
                let actual_str = date.format("%Y-%m-%d").to_string();
                if actual_str == *expected_str {
                    Ok(())
                } else {
                    Err(format!(
                        "Date mismatch: expected '{}' but got '{}'",
                        expected_str, actual_str
                    ))
                }
            } else {
                Err(format!("Invalid date: {} days since epoch", days))
            }
        }

        // Timestamp comparison (simplified - just compare string representations)
        (
            ArrowValue::TimestampMicrosecond { value, timezone: _ },
            ExpectedOutput::String(expected_str),
        ) => {
            let secs = value / 1_000_000;
            let micros = (value % 1_000_000).abs();
            if let Some(dt) = chrono::DateTime::from_timestamp(secs, (micros * 1000) as u32) {
                // Try multiple formats for comparison
                let formats = [
                    "%Y-%m-%d %H:%M:%S%.6f",
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%dT%H:%M:%S%.6f",
                    "%Y-%m-%dT%H:%M:%S",
                ];
                for fmt in formats {
                    let actual_str = dt.format(fmt).to_string();
                    if actual_str == *expected_str {
                        return Ok(());
                    }
                }
                // None matched, report the first format
                let actual_str = dt.format(formats[0]).to_string();
                Err(format!(
                    "Timestamp mismatch: expected '{}' but got '{}'",
                    expected_str, actual_str
                ))
            } else {
                Err(format!("Invalid timestamp: {} microseconds", value))
            }
        }

        // Time comparison
        (ArrowValue::Time64Microsecond(micros), ExpectedOutput::String(expected_str)) => {
            let total_secs = micros / 1_000_000;
            let remaining_micros = micros % 1_000_000;
            let hours = total_secs / 3600;
            let mins = (total_secs % 3600) / 60;
            let secs = total_secs % 60;

            let actual_str = if remaining_micros > 0 {
                format!(
                    "{:02}:{:02}:{:02}.{:06}",
                    hours, mins, secs, remaining_micros
                )
            } else {
                format!("{:02}:{:02}:{:02}", hours, mins, secs)
            };

            if actual_str == *expected_str {
                Ok(())
            } else {
                Err(format!(
                    "Time mismatch: expected '{}' but got '{}'",
                    expected_str, actual_str
                ))
            }
        }

        // Interval comparison
        (
            ArrowValue::IntervalMonthDayNano {
                months,
                days,
                nanos,
            },
            ExpectedOutput::String(expected_str),
        ) => {
            let actual_str = ArrowValue::IntervalMonthDayNano {
                months: *months,
                days: *days,
                nanos: *nanos,
            }
            .to_string();

            if actual_str == *expected_str {
                Ok(())
            } else {
                Err(format!(
                    "Interval mismatch: expected '{}' but got '{}'",
                    expected_str, actual_str
                ))
            }
        }

        // Type mismatch - this handles cases like comparing Int to Bool
        (actual_val, expected_val) => Err(format!(
            "Type mismatch: cannot compare {:?} with {:?}",
            actual_val, expected_val
        )),
    }
}

/// Helper to compare integers.
fn compare_int(actual: i64, expected: i64) -> Result<(), String> {
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "Integer mismatch: expected {} but got {}",
            expected, actual
        ))
    }
}

/// Helper to compare integer with string.
fn compare_int_string(actual: i64, expected_str: &str) -> Result<(), String> {
    let expected: i64 = expected_str
        .parse()
        .map_err(|_| format!("Cannot parse '{}' as integer", expected_str))?;
    compare_int(actual, expected)
}

/// Helper to compare unsigned integer with string.
fn compare_uint_string(actual: u64, expected_str: &str) -> Result<(), String> {
    let expected: u64 = expected_str
        .parse()
        .map_err(|_| format!("Cannot parse '{}' as unsigned integer", expected_str))?;
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "Integer mismatch: expected {} but got {}",
            expected, actual
        ))
    }
}

/// Helper to compare floats with mode.
fn compare_float(actual: f64, expected: f64, mode: &ComparisonMode) -> Result<(), String> {
    match mode {
        ComparisonMode::Approx { epsilon } => {
            if (actual - expected).abs() <= *epsilon {
                Ok(())
            } else {
                Err(format!(
                    "Float mismatch: expected {} but got {} (epsilon: {})",
                    expected, actual, epsilon
                ))
            }
        }
        ComparisonMode::Special => {
            let matches = (expected.is_nan() && actual.is_nan())
                || (expected.is_infinite()
                    && actual.is_infinite()
                    && expected.signum() == actual.signum())
                || (expected == actual);
            if matches {
                Ok(())
            } else {
                Err(format!(
                    "Float mismatch: expected {} but got {}",
                    expected, actual
                ))
            }
        }
        _ => {
            if expected == actual {
                Ok(())
            } else {
                Err(format!(
                    "Float mismatch: expected {} but got {}",
                    expected, actual
                ))
            }
        }
    }
}

/// Helper to compare float with string.
fn compare_float_string(
    actual: f64,
    expected_str: &str,
    mode: &ComparisonMode,
) -> Result<(), String> {
    // Handle special values
    let expected_lower = expected_str.to_lowercase();
    if expected_lower == "nan" {
        if actual.is_nan() {
            return Ok(());
        } else {
            return Err(format!("Expected NaN but got {}", actual));
        }
    }
    if expected_lower == "inf" || expected_lower == "infinity" {
        if actual.is_infinite() && actual.is_sign_positive() {
            return Ok(());
        } else {
            return Err(format!("Expected Infinity but got {}", actual));
        }
    }
    if expected_lower == "-inf" || expected_lower == "-infinity" {
        if actual.is_infinite() && actual.is_sign_negative() {
            return Ok(());
        } else {
            return Err(format!("Expected -Infinity but got {}", actual));
        }
    }

    let expected: f64 = expected_str
        .parse()
        .map_err(|_| format!("Cannot parse '{}' as float", expected_str))?;
    compare_float(actual, expected, mode)
}

/// Helper to compare Decimal128 with expected string.
///
/// Uses rust_decimal for normalization when possible (up to 28 digits).
/// For larger numbers (up to 38 digits), falls back to string comparison
/// with trailing zero normalization.
fn compare_decimal128(
    value: i128,
    scale: i8,
    expected_str: &str,
    _mode: &ComparisonMode,
) -> Result<(), String> {
    // Convert i128 with scale to decimal string
    let actual_str = if scale == 0 {
        value.to_string()
    } else {
        let divisor = 10i128.pow(scale as u32);
        let integer_part = value / divisor;
        let fractional_part = (value % divisor).abs();
        format!(
            "{}.{:0>width$}",
            integer_part,
            fractional_part,
            width = scale as usize
        )
    };

    // Try to parse both as high-precision Decimals to normalize (ignore trailing zeros)
    // rust_decimal supports up to 28 digits; for larger numbers we fall back to string comparison
    if let (Ok(expected_dec), Ok(actual_dec)) = (
        expected_str.parse::<Decimal>(),
        actual_str.parse::<Decimal>(),
    ) {
        if expected_dec == actual_dec {
            Ok(())
        } else {
            Err(format!(
                "Decimal mismatch: expected {} but got {}",
                expected_str, actual_str
            ))
        }
    } else {
        // Fall back to string comparison for very large numbers (> 28 digits)
        // Normalize by comparing the integer and fractional parts separately
        let normalize_decimal_str = |s: &str| -> (String, String) {
            if let Some((int_part, frac_part)) = s.split_once('.') {
                // Trim trailing zeros from fractional part
                let frac_trimmed = frac_part.trim_end_matches('0');
                (
                    int_part.to_string(),
                    if frac_trimmed.is_empty() {
                        String::new()
                    } else {
                        frac_trimmed.to_string()
                    },
                )
            } else {
                (s.to_string(), String::new())
            }
        };

        let (expected_int, expected_frac) = normalize_decimal_str(expected_str);
        let (actual_int, actual_frac) = normalize_decimal_str(&actual_str);

        if expected_int == actual_int && expected_frac == actual_frac {
            Ok(())
        } else {
            Err(format!(
                "Decimal mismatch: expected {} but got {}",
                expected_str, actual_str
            ))
        }
    }
}

/// Helper to compare binary with expected value.
/// Expected can be either:
/// - Plain text (e.g., "HELLO") - binary will be decoded as UTF-8
/// - Hex string (e.g., "48454C4C4F") - if it's all hex digits and even length
fn compare_binary(actual_bytes: &[u8], expected: &str) -> Result<(), String> {
    // First try to compare as UTF-8 text (most common case in fixtures)
    let actual_str = String::from_utf8_lossy(actual_bytes);
    if actual_str == expected {
        return Ok(());
    }

    // Try to interpret expected as hex if it looks like hex
    let expected_hex = expected
        .trim_start_matches("\\x")
        .trim_start_matches("0x")
        .trim_start_matches("X'")
        .trim_end_matches('\'');

    // Check if it looks like a hex string (all hex chars, even length)
    let is_hex = !expected_hex.is_empty()
        && expected_hex.len().is_multiple_of(2)
        && expected_hex.chars().all(|c| c.is_ascii_hexdigit());

    if is_hex {
        // Convert expected hex to bytes
        let expected_bytes: Result<Vec<u8>, _> = (0..expected_hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&expected_hex[i..i + 2], 16))
            .collect();

        if let Ok(expected) = expected_bytes {
            if actual_bytes == expected {
                return Ok(());
            }
        }
    }

    // Neither matched
    Err(format!(
        "Binary mismatch: expected '{}' but got '{}'",
        expected, actual_str
    ))
}

/// Extract an i64 id value from an Arrow array (supports Int32, Int64, UInt32, UInt64).
fn extract_id_value(array: &dyn Array, index: usize) -> Option<i64> {
    if array.is_null(index) {
        return None;
    }
    match array.data_type() {
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Some(arr.value(index) as i64)
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Some(arr.value(index))
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            Some(arr.value(index) as i64)
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            Some(arr.value(index) as i64)
        }
        _ => None,
    }
}

/// Find a column index by name in a RecordBatch schema.
fn find_column_index(batch: &RecordBatch, name: &str) -> Option<usize> {
    batch
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == name)
}

/// Validate all values in record batches against expected test values.
///
/// This function expects batches with columns `id` (integer) and `val` (the test type).
/// It sorts rows by `id` to ensure deterministic ordering regardless of database row order,
/// then compares the `val` values against expected values in insertion order.
pub fn validate_batch_values(
    batches: &[RecordBatch],
    expected_values: &[TestValue],
    semantic_type: &SemanticType,
) -> Result<(), FailureReason> {
    // Collect all (id, val) pairs from batches
    let mut id_val_pairs: Vec<(i64, ArrowValue)> = Vec::new();

    for batch in batches {
        if batch.num_columns() < 2 {
            return Err(FailureReason::FetchError {
                message: format!(
                    "Expected at least 2 columns (id, val), got {}",
                    batch.num_columns()
                ),
            });
        }

        // Find id and val columns by name
        let id_idx = find_column_index(batch, "id").ok_or_else(|| FailureReason::FetchError {
            message: "Column 'id' not found in batch".to_string(),
        })?;
        let val_idx = find_column_index(batch, "val").ok_or_else(|| FailureReason::FetchError {
            message: "Column 'val' not found in batch".to_string(),
        })?;

        let id_column = batch.column(id_idx);
        let val_column = batch.column(val_idx);

        for i in 0..batch.num_rows() {
            let id = extract_id_value(id_column.as_ref(), i).ok_or_else(|| {
                FailureReason::FetchError {
                    message: format!("Failed to extract id at row {}", i),
                }
            })?;
            let val = extract_arrow_value(val_column.as_ref(), i);
            id_val_pairs.push((id, val));
        }
    }

    // Sort by id to get deterministic order
    id_val_pairs.sort_by_key(|(id, _)| *id);

    // Extract just the values in sorted order
    let actual_values: Vec<ArrowValue> = id_val_pairs.into_iter().map(|(_, val)| val).collect();

    // Check count matches
    if actual_values.len() != expected_values.len() {
        return Err(FailureReason::FetchError {
            message: format!(
                "Row count mismatch: expected {} values but got {}",
                expected_values.len(),
                actual_values.len()
            ),
        });
    }

    // Compare each value
    for (index, (actual, expected)) in actual_values.iter().zip(expected_values.iter()).enumerate()
    {
        if compare_arrow_value(
            actual,
            &expected.expected,
            &expected.comparison,
            semantic_type,
        )
        .is_err()
        {
            return Err(FailureReason::ValueMismatch {
                index,
                input_sql: expected.sql_literal.clone(),
                expected: format!("{:?}", expected.expected),
                actual: actual.to_string(),
            });
        }
    }

    Ok(())
}

/// Get the Arrow DataType of the 'val' column from the first batch.
///
/// Returns None if no batches exist or if the 'val' column is not found.
pub fn get_val_column_type(batches: &[RecordBatch]) -> Option<DataType> {
    batches.first().and_then(|batch| {
        find_column_index(batch, "val").map(|idx| batch.schema().field(idx).data_type().clone())
    })
}

// ============================================================================
// Secret Manager
// ============================================================================

/// Create a throwaway [`SecretManager`] backed by an in-memory SQLite catalog.
///
/// Used by every backend-specific test to store credentials (passwords,
/// service-account keys, etc.) through the same path the production code uses.
pub async fn create_test_secret_manager(dir: &TempDir) -> SecretManager {
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
// Tests
// ============================================================================

#[cfg(test)]
pub mod tests {
    use super::*;

    // ------------------------------------------------------------------------
    // compare_values tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_compare_null_values() {
        // NULL == NULL
        assert!(compare_values(&ExpectedOutput::Null, None, &ComparisonMode::Exact).is_ok());

        // NULL != "value"
        let result = compare_values(&ExpectedOutput::Null, Some("value"), &ComparisonMode::Exact);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Expected NULL but got"));

        // "value" != NULL
        let result = compare_values(
            &ExpectedOutput::String("value".to_string()),
            None,
            &ComparisonMode::Exact,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Expected value but got NULL"));
    }

    #[test]
    fn test_compare_exact_strings() {
        // Exact match
        assert!(compare_values(
            &ExpectedOutput::String("hello".to_string()),
            Some("hello"),
            &ComparisonMode::Exact,
        )
        .is_ok());

        // Mismatch
        let result = compare_values(
            &ExpectedOutput::String("hello".to_string()),
            Some("world"),
            &ComparisonMode::Exact,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("String mismatch"));
    }

    #[test]
    fn test_compare_integers() {
        // Exact match
        assert!(
            compare_values(&ExpectedOutput::Int(42), Some("42"), &ComparisonMode::Exact,).is_ok()
        );

        // Negative
        assert!(compare_values(
            &ExpectedOutput::Int(-100),
            Some("-100"),
            &ComparisonMode::Exact,
        )
        .is_ok());

        // Mismatch
        let result = compare_values(&ExpectedOutput::Int(42), Some("43"), &ComparisonMode::Exact);
        assert!(result.is_err());

        // Invalid parse
        let result = compare_values(
            &ExpectedOutput::Int(42),
            Some("not_a_number"),
            &ComparisonMode::Exact,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Cannot parse"));
    }

    #[test]
    fn test_compare_floats_exact() {
        // Exact match
        assert!(compare_values(
            &ExpectedOutput::Float(1.234),
            Some("1.234"),
            &ComparisonMode::Exact,
        )
        .is_ok());
    }

    #[test]
    fn test_compare_floats_approximate() {
        // Within epsilon
        assert!(compare_values(
            &ExpectedOutput::Float(1.23456),
            Some("1.23455"),
            &ComparisonMode::Approx { epsilon: 0.001 },
        )
        .is_ok());

        // Outside epsilon
        let result = compare_values(
            &ExpectedOutput::Float(1.23456),
            Some("1.25"),
            &ComparisonMode::Approx { epsilon: 0.001 },
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_compare_floats_special() {
        // NaN == NaN in special mode
        assert!(compare_values(
            &ExpectedOutput::Float(f64::NAN),
            Some("NaN"),
            &ComparisonMode::Special,
        )
        .is_ok());

        // +Inf == +Inf
        assert!(compare_values(
            &ExpectedOutput::Float(f64::INFINITY),
            Some("Infinity"),
            &ComparisonMode::Special,
        )
        .is_ok());

        // -Inf == -Inf
        assert!(compare_values(
            &ExpectedOutput::Float(f64::NEG_INFINITY),
            Some("-Infinity"),
            &ComparisonMode::Special,
        )
        .is_ok());
    }

    #[test]
    fn test_compare_booleans() {
        // True variants
        assert!(compare_values(
            &ExpectedOutput::Bool(true),
            Some("true"),
            &ComparisonMode::Exact,
        )
        .is_ok());
        assert!(compare_values(
            &ExpectedOutput::Bool(true),
            Some("t"),
            &ComparisonMode::Exact,
        )
        .is_ok());
        assert!(compare_values(
            &ExpectedOutput::Bool(true),
            Some("1"),
            &ComparisonMode::Exact,
        )
        .is_ok());

        // False variants
        assert!(compare_values(
            &ExpectedOutput::Bool(false),
            Some("false"),
            &ComparisonMode::Exact,
        )
        .is_ok());
        assert!(compare_values(
            &ExpectedOutput::Bool(false),
            Some("f"),
            &ComparisonMode::Exact,
        )
        .is_ok());
        assert!(compare_values(
            &ExpectedOutput::Bool(false),
            Some("0"),
            &ComparisonMode::Exact,
        )
        .is_ok());

        // Mismatch
        let result = compare_values(
            &ExpectedOutput::Bool(true),
            Some("false"),
            &ComparisonMode::Exact,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_compare_strings_approximate() {
        // Numeric strings with epsilon
        assert!(compare_values(
            &ExpectedOutput::String("3.14159".to_string()),
            Some("3.14158"),
            &ComparisonMode::Approx { epsilon: 0.001 },
        )
        .is_ok());
    }

    #[test]
    fn test_compare_high_precision_decimals() {
        // High-precision decimals that would lose precision with f64
        // f64 has ~15-17 significant digits; these values have more
        let expected = "12345678901234567890.123456789";
        let actual = "12345678901234567890.123456789";

        // Exact comparison should work
        assert!(compare_strings(expected, actual, &ComparisonMode::Exact).is_ok());

        // Approximate comparison with zero epsilon should also work
        assert!(
            compare_strings(expected, actual, &ComparisonMode::Approx { epsilon: 0.0 }).is_ok()
        );

        // High-precision values that differ slightly
        let expected2 = "12345678901234567890.123456789";
        let actual2 = "12345678901234567890.123456790"; // Differs in last digit

        // Should fail with exact comparison
        assert!(compare_strings(expected2, actual2, &ComparisonMode::Exact).is_err());

        // Should succeed with appropriate epsilon (0.000000001)
        assert!(compare_strings(
            expected2,
            actual2,
            &ComparisonMode::Approx {
                epsilon: 0.000000002
            }
        )
        .is_ok());

        // Should fail with too-small epsilon
        assert!(compare_strings(
            expected2,
            actual2,
            &ComparisonMode::Approx {
                epsilon: 0.0000000001
            }
        )
        .is_err());
    }

    #[test]
    fn test_compare_strings_special_nan() {
        // Various NaN representations
        assert!(compare_strings("NaN", "nan", &ComparisonMode::Special).is_ok());
        assert!(compare_strings("NaN", "NAN", &ComparisonMode::Special).is_ok());
    }

    #[test]
    fn test_compare_strings_special_infinity() {
        // Various infinity representations
        assert!(compare_strings("Infinity", "inf", &ComparisonMode::Special).is_ok());
        assert!(compare_strings("+infinity", "Infinity", &ComparisonMode::Special).is_ok());
        assert!(compare_strings("-Infinity", "-inf", &ComparisonMode::Special).is_ok());
    }

    #[test]
    fn test_compare_json() {
        // Same object, different key order
        assert!(compare_strings(
            r#"{"a":1,"b":2}"#,
            r#"{"b":2,"a":1}"#,
            &ComparisonMode::Json,
        )
        .is_ok());

        // Different values
        let result = compare_strings(r#"{"a":1}"#, r#"{"a":2}"#, &ComparisonMode::Json);
        assert!(result.is_err());

        // Nested objects
        assert!(compare_strings(
            r#"{"outer":{"inner":"value"}}"#,
            r#"{"outer":{"inner":"value"}}"#,
            &ComparisonMode::Json,
        )
        .is_ok());
    }

    // ------------------------------------------------------------------------
    // TestValue conversion tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_fixture_value_to_test_value() {
        use crate::fixtures::{Comparison, Requirements};

        let fixture = FixtureValue {
            name: "test".to_string(),
            sql: "42".to_string(),
            expected: "42".to_string(),
            expected_null: None,
            expect_error: false,
            comparison: Comparison::Exact,
            epsilon: None,
            requires: Requirements::default(),
            note: Some("Test note".to_string()),
        };

        let test_value: TestValue = fixture.into();

        assert_eq!(test_value.sql_literal, "42");
        assert_eq!(
            test_value.expected,
            ExpectedOutput::String("42".to_string())
        );
        assert_eq!(test_value.comparison, ComparisonMode::Exact);
        assert_eq!(test_value.note, Some("Test note".to_string()));
    }

    #[test]
    fn test_fixture_value_with_approximate_comparison() {
        use crate::fixtures::{Comparison, Requirements};

        let fixture = FixtureValue {
            name: "pi".to_string(),
            sql: "3.14159".to_string(),
            expected: "3.14159".to_string(),
            expected_null: None,
            expect_error: false,
            comparison: Comparison::Approximate,
            epsilon: Some(0.0001),
            requires: Requirements::default(),
            note: None,
        };

        let test_value: TestValue = fixture.into();

        assert_eq!(
            test_value.comparison,
            ComparisonMode::Approx { epsilon: 0.0001 }
        );
    }

    #[test]
    fn test_fixture_value_with_error() {
        use crate::fixtures::{Comparison, Requirements};

        let fixture = FixtureValue {
            name: "overflow".to_string(),
            sql: "999999999999999999999".to_string(),
            expected: "overflow".to_string(),
            expected_null: None,
            expect_error: true,
            comparison: Comparison::Exact,
            epsilon: None,
            requires: Requirements::default(),
            note: None,
        };

        let test_value: TestValue = fixture.into();

        assert_eq!(
            test_value.expected,
            ExpectedOutput::Error("overflow".to_string())
        );
    }

    // ------------------------------------------------------------------------
    // TestReport tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_report_counts() {
        let mut report = TestReport::new("postgres");

        report.add_result(TypeTestResult::passed("INTEGER"));
        report.add_result(TypeTestResult::passed("BIGINT"));
        report.add_result(TypeTestResult::failed(
            "NUMERIC",
            FailureReason::ValueMismatch {
                index: 0,
                input_sql: "1.23".to_string(),
                expected: "1.23".to_string(),
                actual: "1.230000".to_string(),
            },
        ));
        report.add_result(TypeTestResult::skipped("MONEY", "Not supported"));

        assert_eq!(report.passed_count(), 2);
        assert_eq!(report.failed_count(), 1);
        assert_eq!(report.skipped_count(), 1);
        assert_eq!(report.failures().len(), 1);
    }

    #[test]
    fn test_report_assert_all_passed_success() {
        let mut report = TestReport::new("postgres");
        report.add_result(TypeTestResult::passed("INTEGER"));
        report.add_result(TypeTestResult::passed("BIGINT"));

        // Should not panic
        report.assert_all_passed();
    }

    #[test]
    #[should_panic(expected = "type test(s) failed")]
    fn test_report_assert_all_passed_failure() {
        let mut report = TestReport::new("postgres");
        report.add_result(TypeTestResult::passed("INTEGER"));
        report.add_result(TypeTestResult::failed(
            "NUMERIC",
            FailureReason::ArrowTypeMismatch {
                expected: DataType::Decimal128(10, 2),
                actual: DataType::Utf8,
            },
        ));

        // Should panic
        report.assert_all_passed();
    }

    #[test]
    fn test_result_status_checks() {
        let passed = TypeTestResult::passed("INT");
        assert!(passed.is_passed());
        assert!(!passed.is_failed());
        assert!(!passed.is_skipped());

        let failed = TypeTestResult::failed(
            "INT",
            FailureReason::FetchError {
                message: "Connection lost".to_string(),
            },
        );
        assert!(!failed.is_passed());
        assert!(failed.is_failed());
        assert!(!failed.is_skipped());

        let skipped = TypeTestResult::skipped("INT", "Not implemented");
        assert!(!skipped.is_passed());
        assert!(!skipped.is_failed());
        assert!(skipped.is_skipped());
    }

    // ------------------------------------------------------------------------
    // SemanticType tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_semantic_type_integer_variants() {
        let signed_i32 = SemanticType::Integer {
            signed: true,
            bits: 32,
        };
        let unsigned_i32 = SemanticType::Integer {
            signed: false,
            bits: 32,
        };

        assert_ne!(signed_i32, unsigned_i32);

        let i64_type = SemanticType::Integer {
            signed: true,
            bits: 64,
        };
        assert_ne!(signed_i32, i64_type);
    }

    #[test]
    fn test_semantic_type_array() {
        let int_array = SemanticType::Array {
            element: Box::new(SemanticType::Integer {
                signed: true,
                bits: 32,
            }),
        };

        let string_array = SemanticType::Array {
            element: Box::new(SemanticType::String),
        };

        assert_ne!(int_array, string_array);
    }

    #[test]
    fn test_semantic_type_timestamp_variants() {
        let with_tz = SemanticType::Timestamp { with_tz: true };
        let without_tz = SemanticType::Timestamp { with_tz: false };

        assert_ne!(with_tz, without_tz);
    }
}
