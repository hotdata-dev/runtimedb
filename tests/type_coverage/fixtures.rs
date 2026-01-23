//! Fixture loading and constraint-based selection for type coverage tests.
//!
//! This module provides infrastructure for loading test values from TOML fixtures
//! and filtering them based on database-specific constraints.

use serde::Deserialize;
use std::path::PathBuf;

/// Category metadata describing a group of test values.
#[derive(Debug, Clone, Deserialize)]
pub struct CategoryMeta {
    /// Name of the category (e.g., "integers", "decimals").
    pub name: String,
    /// Human-readable description of the category.
    #[allow(dead_code)] // Required for deserialization, may be used in future
    pub description: String,
    /// Optional semantic type hint (e.g., "Integer", "Decimal", "Timestamp").
    #[allow(dead_code)] // Required for deserialization, may be used in future
    pub semantic_type: Option<String>,
}

/// Comparison strategy for validating expected values.
#[derive(Debug, Clone, Deserialize, PartialEq, Default)]
#[serde(rename_all = "snake_case")]
pub enum Comparison {
    /// Exact equality comparison (default).
    #[default]
    Exact,
    /// Approximate comparison within epsilon tolerance.
    Approximate,
    /// Compare string representation.
    String,
}

/// Constraints that a fixture value may require to be compatible.
///
/// These constraints allow filtering test values based on what a specific
/// database type can handle. For example, a SMALLINT column has max_precision=5
/// and can't store values larger than 32767.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct Constraints {
    /// Maximum numeric precision (total digits).
    pub max_precision: Option<u32>,
    /// Maximum numeric scale (digits after decimal).
    pub max_scale: Option<u32>,
    /// Minimum value (as string for arbitrary precision).
    pub min_value: Option<String>,
    /// Maximum value (as string for arbitrary precision).
    pub max_value: Option<String>,
    /// Whether signed values are supported.
    pub signed: Option<bool>,
    /// Whether NULL values are supported.
    pub nullable: Option<bool>,
    /// Maximum byte size for variable-length types.
    pub max_bytes: Option<u64>,
}

impl Constraints {
    /// Create new constraints with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set maximum precision.
    pub fn with_max_precision(mut self, precision: u32) -> Self {
        self.max_precision = Some(precision);
        self
    }

    /// Set maximum scale.
    pub fn with_max_scale(mut self, scale: u32) -> Self {
        self.max_scale = Some(scale);
        self
    }

    /// Set minimum value.
    pub fn with_min_value(mut self, value: impl Into<String>) -> Self {
        self.min_value = Some(value.into());
        self
    }

    /// Set maximum value.
    pub fn with_max_value(mut self, value: impl Into<String>) -> Self {
        self.max_value = Some(value.into());
        self
    }

    /// Set signed constraint.
    pub fn with_signed(mut self, signed: bool) -> Self {
        self.signed = Some(signed);
        self
    }

    /// Set nullable constraint.
    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.nullable = Some(nullable);
        self
    }

    /// Set maximum bytes.
    pub fn with_max_bytes(mut self, bytes: u64) -> Self {
        self.max_bytes = Some(bytes);
        self
    }
}

/// Requirements that a test value has on the target type.
///
/// These are the constraints the value needs to be stored correctly.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct Requirements {
    /// Minimum precision needed.
    pub min_precision: Option<u32>,
    /// Minimum scale needed.
    pub min_scale: Option<u32>,
    /// Whether the value is negative (requires signed type).
    pub signed: Option<bool>,
    /// Whether this is a NULL value (requires nullable type).
    pub nullable: Option<bool>,
    /// Minimum bytes needed.
    pub min_bytes: Option<u64>,
}

/// A single test value with its SQL representation and expected result.
#[derive(Debug, Clone, Deserialize)]
pub struct FixtureValue {
    /// Unique name for this test value.
    pub name: String,
    /// SQL literal or expression to insert/select.
    pub sql: String,
    /// Expected value after round-trip (as string for comparison).
    pub expected: String,
    /// Expected value when NULL (optional, for nullable tests).
    pub expected_null: Option<String>,
    /// Whether this value is expected to cause an error.
    #[serde(default)]
    pub expect_error: bool,
    /// How to compare the expected value.
    #[serde(default)]
    pub comparison: Comparison,
    /// Epsilon for approximate comparisons.
    pub epsilon: Option<f64>,
    /// Constraints required for this value to be compatible.
    #[serde(default)]
    pub requires: Requirements,
    /// Optional note about this test case.
    pub note: Option<String>,
}

impl FixtureValue {
    /// Check if this value is compatible with the given constraints.
    ///
    /// Returns true if the value can be stored in a column with the given constraints.
    pub fn is_compatible(&self, constraints: &Constraints) -> bool {
        // Check precision requirements
        if let (Some(min_prec), Some(max_prec)) =
            (self.requires.min_precision, constraints.max_precision)
        {
            if min_prec > max_prec {
                return false;
            }
        }

        // Check scale requirements
        if let (Some(min_scale), Some(max_scale)) = (self.requires.min_scale, constraints.max_scale)
        {
            if min_scale > max_scale {
                return false;
            }
        }

        // Check signed requirements
        if let (Some(requires_signed), Some(supports_signed)) =
            (self.requires.signed, constraints.signed)
        {
            if requires_signed && !supports_signed {
                return false;
            }
        }

        // Check nullable requirements
        if let (Some(requires_nullable), Some(supports_nullable)) =
            (self.requires.nullable, constraints.nullable)
        {
            if requires_nullable && !supports_nullable {
                return false;
            }
        }

        // Check byte requirements
        if let (Some(min_bytes), Some(max_bytes)) = (self.requires.min_bytes, constraints.max_bytes)
        {
            if min_bytes > max_bytes {
                return false;
            }
        }

        // Check value range constraints (using string comparison for now)
        if let Some(ref min_value) = constraints.min_value {
            if let Ok(expected) = self.expected.parse::<i128>() {
                if let Ok(min) = min_value.parse::<i128>() {
                    if expected < min {
                        return false;
                    }
                }
            }
        }

        if let Some(ref max_value) = constraints.max_value {
            if let Ok(expected) = self.expected.parse::<i128>() {
                if let Ok(max) = max_value.parse::<i128>() {
                    if expected > max {
                        return false;
                    }
                }
            }
        }

        true
    }
}

/// A category of fixture values loaded from a TOML file.
#[derive(Debug, Clone, Deserialize)]
pub struct FixtureCategory {
    /// Metadata about this category.
    pub category: CategoryMeta,
    /// Test values in this category.
    pub values: Vec<FixtureValue>,
}

impl FixtureCategory {
    /// Load a fixture category from a TOML file.
    ///
    /// The filename should be relative to `tests/type_coverage/fixtures/`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let integers = FixtureCategory::load("integers.toml")?;
    /// ```
    pub fn load(filename: &str) -> Result<Self, FixtureError> {
        let path = Self::fixtures_dir().join(filename);
        let content = std::fs::read_to_string(&path).map_err(|e| FixtureError::Io {
            path: path.clone(),
            source: e,
        })?;
        toml::from_str(&content).map_err(|e| FixtureError::Parse { path, source: e })
    }

    /// Get the fixtures directory path.
    fn fixtures_dir() -> PathBuf {
        // Find the project root by looking for Cargo.toml
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("."));
        manifest_dir
            .join("tests")
            .join("type_coverage")
            .join("fixtures")
    }

    /// Select values that are compatible with the given constraints.
    ///
    /// Returns a new category containing only the compatible values.
    pub fn select_compatible(&self, constraints: &Constraints) -> Self {
        let compatible_values: Vec<FixtureValue> = self
            .values
            .iter()
            .filter(|v| v.is_compatible(constraints))
            .cloned()
            .collect();

        Self {
            category: self.category.clone(),
            values: compatible_values,
        }
    }

    /// Get a value by name.
    pub fn get(&self, name: &str) -> Option<&FixtureValue> {
        self.values.iter().find(|v| v.name == name)
    }
}

/// Errors that can occur when loading fixtures.
#[derive(Debug)]
pub enum FixtureError {
    /// IO error reading the fixture file.
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    /// Parse error in the TOML content.
    Parse {
        path: PathBuf,
        source: toml::de::Error,
    },
}

impl std::fmt::Display for FixtureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io { path, source } => {
                write!(f, "Failed to read fixture file {:?}: {}", path, source)
            }
            Self::Parse { path, source } => {
                write!(f, "Failed to parse fixture file {:?}: {}", path, source)
            }
        }
    }
}

impl std::error::Error for FixtureError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Parse { source, .. } => Some(source),
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[test]
    fn test_load_integers_fixture() {
        let category =
            FixtureCategory::load("integers.toml").expect("Failed to load integers.toml");

        assert_eq!(category.category.name, "integers");
        assert!(
            !category.values.is_empty(),
            "Should have at least one value"
        );

        // Check that we have the zero value
        let zero = category.get("zero").expect("Should have zero value");
        assert_eq!(zero.sql, "0");
        assert_eq!(zero.expected, "0");
    }

    #[test]
    fn test_constraint_filtering_precision() {
        let category =
            FixtureCategory::load("integers.toml").expect("Failed to load integers.toml");

        // SMALLINT constraints: max value 32767, min value -32768
        let smallint_constraints = Constraints::new()
            .with_max_value("32767")
            .with_min_value("-32768")
            .with_signed(true);

        let compatible = category.select_compatible(&smallint_constraints);

        // Should include small values
        assert!(
            compatible.get("zero").is_some(),
            "zero should be compatible"
        );
        assert!(
            compatible.get("positive_small").is_some(),
            "positive_small should be compatible"
        );
        assert!(
            compatible.get("negative_small").is_some(),
            "negative_small should be compatible"
        );
        assert!(
            compatible.get("i16_max").is_some(),
            "i16_max should be compatible"
        );
        assert!(
            compatible.get("i16_min").is_some(),
            "i16_min should be compatible"
        );

        // Should NOT include large values
        assert!(
            compatible.get("i32_max").is_none(),
            "i32_max should NOT be compatible with SMALLINT"
        );
        assert!(
            compatible.get("i64_max").is_none(),
            "i64_max should NOT be compatible with SMALLINT"
        );
    }

    #[test]
    fn test_constraint_filtering_unsigned() {
        let category =
            FixtureCategory::load("integers.toml").expect("Failed to load integers.toml");

        // Unsigned constraints: no negative values
        let unsigned_constraints = Constraints::new()
            .with_signed(false)
            .with_min_value("0")
            .with_max_value("65535");

        let compatible = category.select_compatible(&unsigned_constraints);

        // Should include unsigned values
        assert!(
            compatible.get("zero").is_some(),
            "zero should be compatible"
        );
        assert!(
            compatible.get("positive_small").is_some(),
            "positive_small should be compatible"
        );
        assert!(
            compatible.get("u16_max").is_some(),
            "u16_max should be compatible"
        );

        // Should NOT include negative values
        assert!(
            compatible.get("negative_small").is_none(),
            "negative_small should NOT be compatible"
        );
        assert!(
            compatible.get("i16_min").is_none(),
            "i16_min should NOT be compatible"
        );
    }

    #[test]
    fn test_constraint_filtering_nullable() {
        let category =
            FixtureCategory::load("integers.toml").expect("Failed to load integers.toml");

        // Non-nullable constraints
        let non_nullable_constraints = Constraints::new().with_nullable(false).with_signed(true);

        let compatible = category.select_compatible(&non_nullable_constraints);

        // Should NOT include NULL value
        assert!(
            compatible.get("null").is_none(),
            "null should NOT be compatible with non-nullable"
        );

        // Should include regular values
        assert!(
            compatible.get("zero").is_some(),
            "zero should be compatible"
        );
    }

    #[test]
    fn test_value_is_compatible_basic() {
        let value = FixtureValue {
            name: "test".to_string(),
            sql: "42".to_string(),
            expected: "42".to_string(),
            expected_null: None,
            expect_error: false,
            comparison: Comparison::Exact,
            epsilon: None,
            requires: Requirements::default(),
            note: None,
        };

        // Should be compatible with no constraints
        assert!(value.is_compatible(&Constraints::default()));

        // Should be compatible with matching constraints
        let constraints = Constraints::new().with_max_value("100").with_min_value("0");
        assert!(value.is_compatible(&constraints));

        // Should NOT be compatible with too-low max
        let constraints = Constraints::new().with_max_value("10");
        assert!(!value.is_compatible(&constraints));
    }

    #[test]
    fn test_value_is_compatible_with_requirements() {
        let value = FixtureValue {
            name: "test".to_string(),
            sql: "-100".to_string(),
            expected: "-100".to_string(),
            expected_null: None,
            expect_error: false,
            comparison: Comparison::Exact,
            epsilon: None,
            requires: Requirements {
                min_precision: Some(3),
                min_scale: None,
                signed: Some(true),
                nullable: None,
                min_bytes: None,
            },
            note: None,
        };

        // Should be compatible with signed type
        let constraints = Constraints::new().with_signed(true).with_max_precision(5);
        assert!(value.is_compatible(&constraints));

        // Should NOT be compatible with unsigned type
        let constraints = Constraints::new().with_signed(false);
        assert!(!value.is_compatible(&constraints));

        // Should NOT be compatible with insufficient precision
        let constraints = Constraints::new().with_signed(true).with_max_precision(2);
        assert!(!value.is_compatible(&constraints));
    }

    #[test]
    fn test_comparison_deserialization() {
        // Test that comparison enum deserializes correctly from TOML
        let toml_content = r#"
            [category]
            name = "test"
            description = "Test category"

            [[values]]
            name = "exact_test"
            sql = "1"
            expected = "1"
            comparison = "exact"

            [[values]]
            name = "approx_test"
            sql = "1.5"
            expected = "1.5"
            comparison = "approximate"
            epsilon = 0.001

            [[values]]
            name = "string_test"
            sql = "'hello'"
            expected = "hello"
            comparison = "string"
        "#;

        let category: FixtureCategory = toml::from_str(toml_content).expect("Failed to parse TOML");

        assert_eq!(
            category.get("exact_test").unwrap().comparison,
            Comparison::Exact
        );
        assert_eq!(
            category.get("approx_test").unwrap().comparison,
            Comparison::Approximate
        );
        assert_eq!(category.get("approx_test").unwrap().epsilon, Some(0.001));
        assert_eq!(
            category.get("string_test").unwrap().comparison,
            Comparison::String
        );
    }

    #[test]
    fn test_load_floats_fixture() {
        let category = FixtureCategory::load("floats.toml").expect("Failed to load floats.toml");

        assert_eq!(category.category.name, "floats");
        assert!(!category.values.is_empty(), "Should have values");

        // Check basic values
        assert!(category.get("zero").is_some(), "Should have zero");
        assert!(category.get("one").is_some(), "Should have one");
        assert!(category.get("negative").is_some(), "Should have negative");

        // Check special IEEE 754 values
        let nan = category.get("nan").expect("Should have NaN");
        assert_eq!(nan.comparison, Comparison::String);

        let pi = category.get("pi").expect("Should have pi");
        assert_eq!(pi.comparison, Comparison::Approximate);
        assert!(pi.epsilon.is_some(), "pi should have epsilon");

        // Check infinity values
        assert!(
            category.get("pos_infinity").is_some(),
            "Should have pos_infinity"
        );
        assert!(
            category.get("neg_infinity").is_some(),
            "Should have neg_infinity"
        );
    }

    #[test]
    fn test_load_decimals_fixture() {
        let category =
            FixtureCategory::load("decimals.toml").expect("Failed to load decimals.toml");

        assert_eq!(category.category.name, "decimals");
        assert!(!category.values.is_empty(), "Should have values");

        // Check basic values
        assert!(category.get("zero").is_some(), "Should have zero");
        assert!(category.get("simple").is_some(), "Should have simple");
        assert!(category.get("negative").is_some(), "Should have negative");

        // Check precision requirements
        let high_precision = category
            .get("high_precision")
            .expect("Should have high_precision");
        assert!(
            high_precision.requires.min_precision.is_some(),
            "high_precision should require min_precision"
        );

        // Check scale requirements
        let leading_zeros = category
            .get("leading_zeros_scale")
            .expect("Should have leading_zeros_scale");
        assert!(
            leading_zeros.requires.min_scale.is_some(),
            "leading_zeros_scale should require min_scale"
        );
    }

    #[test]
    fn test_load_strings_fixture() {
        let category = FixtureCategory::load("strings.toml").expect("Failed to load strings.toml");

        assert_eq!(category.category.name, "strings");
        assert!(!category.values.is_empty(), "Should have values");

        // Check basic values
        assert!(category.get("empty").is_some(), "Should have empty");
        assert!(category.get("simple").is_some(), "Should have simple");
        assert!(category.get("unicode").is_some(), "Should have unicode");

        // Check special characters
        let special_chars = category
            .get("special_chars")
            .expect("Should have special_chars");
        // Verify TOML parsing produces actual tab/backslash characters, not escape sequences
        assert!(
            special_chars.sql.contains('\t'),
            "SQL should contain actual tab character after TOML parsing, got bytes: {:?}",
            special_chars.sql.as_bytes()
        );
        assert!(
            special_chars.expected.contains('\t'),
            "Expected should contain actual tab character after TOML parsing"
        );
        // The backslash should be a single backslash, not \\
        assert!(
            special_chars.expected.ends_with('\\'),
            "Expected should end with single backslash"
        );

        assert!(category.get("newlines").is_some(), "Should have newlines");
        let newlines = category.get("newlines").expect("Should have newlines");
        // Verify newlines are actual newline characters
        assert!(
            newlines.sql.contains('\n'),
            "SQL should contain actual newline character after TOML parsing"
        );
        assert!(
            newlines.expected.contains('\n'),
            "Expected should contain actual newline character after TOML parsing"
        );

        // Check byte requirements
        let long_string = category
            .get("long_string")
            .expect("Should have long_string");
        assert!(
            long_string.requires.min_bytes.is_some(),
            "long_string should require min_bytes"
        );
    }

    #[test]
    fn test_load_timestamps_fixture() {
        let category =
            FixtureCategory::load("timestamps.toml").expect("Failed to load timestamps.toml");

        assert_eq!(category.category.name, "timestamps");
        assert!(!category.values.is_empty(), "Should have values");

        // Check epoch values
        assert!(category.get("epoch").is_some(), "Should have epoch");
        assert!(
            category.get("before_epoch").is_some(),
            "Should have before_epoch"
        );

        // Check time precision
        let microseconds = category
            .get("microseconds")
            .expect("Should have microseconds");
        assert!(
            microseconds.requires.min_scale.is_some(),
            "microseconds should require min_scale"
        );

        // Check time of day edge cases
        assert!(category.get("midnight").is_some(), "Should have midnight");
        assert!(
            category.get("end_of_day").is_some(),
            "Should have end_of_day"
        );
    }

    #[test]
    fn test_load_booleans_fixture() {
        let category =
            FixtureCategory::load("booleans.toml").expect("Failed to load booleans.toml");

        assert_eq!(category.category.name, "booleans");
        assert_eq!(category.values.len(), 3, "Should have exactly 3 values");

        // Check all boolean values
        let true_val = category.get("true").expect("Should have true");
        assert_eq!(true_val.expected, "true");

        let false_val = category.get("false").expect("Should have false");
        assert_eq!(false_val.expected, "false");

        let null_val = category.get("null").expect("Should have null");
        assert!(
            null_val.requires.nullable.is_some(),
            "null should require nullable"
        );
    }

    #[test]
    fn test_load_binary_fixture() {
        let category = FixtureCategory::load("binary.toml").expect("Failed to load binary.toml");

        assert_eq!(category.category.name, "binary");
        assert!(!category.values.is_empty(), "Should have values");

        // Check basic values
        assert!(category.get("empty").is_some(), "Should have empty");
        assert!(
            category.get("simple_ascii").is_some(),
            "Should have simple_ascii"
        );
        assert!(
            category.get("single_byte").is_some(),
            "Should have single_byte"
        );
        assert!(
            category.get("lowercase_text").is_some(),
            "Should have lowercase_text"
        );

        // Check byte requirements
        let simple = category
            .get("simple_ascii")
            .expect("Should have simple_ascii");
        assert!(
            simple.requires.min_bytes.is_some(),
            "simple_ascii should require min_bytes"
        );
    }

    #[test]
    fn test_load_json_fixture() {
        let category = FixtureCategory::load("json.toml").expect("Failed to load json.toml");

        assert_eq!(category.category.name, "json");
        assert!(!category.values.is_empty(), "Should have values");

        // Check basic structures
        assert!(
            category.get("empty_object").is_some(),
            "Should have empty_object"
        );
        assert!(
            category.get("empty_array").is_some(),
            "Should have empty_array"
        );
        assert!(
            category.get("simple_object").is_some(),
            "Should have simple_object"
        );

        // Check nested and complex structures
        assert!(category.get("nested").is_some(), "Should have nested");
        assert!(
            category.get("with_types").is_some(),
            "Should have with_types"
        );

        // All JSON values should use string comparison
        let simple_object = category
            .get("simple_object")
            .expect("Should have simple_object");
        assert_eq!(
            simple_object.comparison,
            Comparison::String,
            "JSON should use string comparison"
        );
    }

    #[test]
    fn test_all_fixtures_have_null_value() {
        let fixture_files = [
            "integers.toml",
            "floats.toml",
            "decimals.toml",
            "strings.toml",
            "timestamps.toml",
            "booleans.toml",
            "binary.toml",
            "json.toml",
        ];

        for filename in fixture_files {
            let category = FixtureCategory::load(filename)
                .unwrap_or_else(|_| panic!("Failed to load {}", filename));

            let null_value = category.get("null");
            assert!(
                null_value.is_some(),
                "{} should have a null value",
                filename
            );

            let null_value = null_value.unwrap();
            assert!(
                null_value.requires.nullable.is_some(),
                "{} null value should require nullable",
                filename
            );
        }
    }
}
