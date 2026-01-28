//! Schema building from explicit column definitions.

use crate::http::models::ColumnDefinition;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::collections::HashMap;
use std::sync::Arc;

/// Error when parsing a column type specification.
#[derive(Debug, Clone)]
pub struct ColumnTypeError {
    pub column_name: String,
    pub message: String,
}

impl std::fmt::Display for ColumnTypeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Column '{}': {}", self.column_name, self.message)
    }
}

impl std::error::Error for ColumnTypeError {}

/// Error when building schema from column definitions.
#[derive(Debug)]
pub enum SchemaError {
    /// A column type could not be parsed.
    InvalidType(ColumnTypeError),
    /// A column defined in columns is not present in the data.
    ColumnNotInData { column_name: String },
    /// A column in the data is not defined in columns.
    ColumnNotDefined { column_name: String },
}

impl std::fmt::Display for SchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidType(e) => write!(f, "{}", e),
            Self::ColumnNotInData { column_name } => {
                write!(
                    f,
                    "Column '{}' is defined but not found in data",
                    column_name
                )
            }
            Self::ColumnNotDefined { column_name } => {
                write!(
                    f,
                    "Column '{}' found in data but not defined in columns",
                    column_name
                )
            }
        }
    }
}

impl std::error::Error for SchemaError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidType(e) => Some(e),
            _ => None,
        }
    }
}

/// Supported type names for error messages.
const SUPPORTED_TYPES: &[&str] = &[
    "VARCHAR",
    "TEXT",
    "STRING",
    "CHAR",
    "BOOLEAN",
    "BOOL",
    "TINYINT",
    "SMALLINT",
    "INT",
    "INTEGER",
    "BIGINT",
    "UTINYINT",
    "USMALLINT",
    "UINT",
    "UINTEGER",
    "UBIGINT",
    "FLOAT",
    "REAL",
    "DOUBLE",
    "DECIMAL",
    "NUMERIC",
    "DATE",
    "TIMESTAMP",
    "DATETIME",
    "TIMESTAMPTZ",
    "TIME",
    "BINARY",
    "BLOB",
    "BYTEA",
    "UUID",
    "JSON",
];

/// Parse a column definition into an Arrow DataType.
pub fn parse_column_type(
    column_name: &str,
    definition: &ColumnDefinition,
) -> Result<DataType, ColumnTypeError> {
    match definition {
        ColumnDefinition::Simple(type_str) => parse_type_string(column_name, type_str, None, None),
        ColumnDefinition::Detailed(spec) => {
            parse_type_string(column_name, &spec.data_type, spec.precision, spec.scale)
        }
    }
}

/// Parse a type string into an Arrow DataType.
fn parse_type_string(
    column_name: &str,
    type_str: &str,
    precision: Option<u8>,
    scale: Option<i8>,
) -> Result<DataType, ColumnTypeError> {
    let type_upper = type_str.to_uppercase();

    // Check for mismatched parentheses early (before extracting base type)
    let has_open_paren = type_upper.contains('(');
    let has_close_paren = type_upper.contains(')');
    if has_open_paren != has_close_paren {
        return Err(ColumnTypeError {
            column_name: column_name.to_string(),
            message: format!("Malformed type '{}': mismatched parentheses", type_str),
        });
    }

    // Handle parameterized types like DECIMAL(10,2) by extracting base type
    let base_type = type_upper.split('(').next().unwrap_or(&type_upper).trim();

    let data_type = match base_type {
        // String types
        "VARCHAR" | "TEXT" | "STRING" | "CHAR" | "BPCHAR" => DataType::Utf8,

        // Boolean
        "BOOLEAN" | "BOOL" => DataType::Boolean,

        // Signed integers
        "TINYINT" | "INT1" => DataType::Int8,
        "SMALLINT" | "INT2" => DataType::Int16,
        "INTEGER" | "INT" | "INT4" => DataType::Int32,
        "BIGINT" | "INT8" => DataType::Int64,

        // Unsigned integers
        "UTINYINT" => DataType::UInt8,
        "USMALLINT" => DataType::UInt16,
        "UINTEGER" | "UINT" => DataType::UInt32,
        "UBIGINT" => DataType::UInt64,

        // Floating point
        "REAL" | "FLOAT4" | "FLOAT" => DataType::Float32,
        "DOUBLE" | "FLOAT8" => DataType::Float64,

        // Decimal - use explicit precision/scale if provided, else parse from string or default
        "DECIMAL" | "NUMERIC" => {
            // Determine precision and scale from explicit params or parse from string
            let (p, s) = match (precision, scale) {
                (Some(p), Some(s)) => (p, s),
                (Some(p), None) => (p, 0), // precision only → scale defaults to 0
                (None, Some(_)) => {
                    return Err(ColumnTypeError {
                        column_name: column_name.to_string(),
                        message: "DECIMAL scale requires precision to also be specified"
                            .to_string(),
                    });
                }
                (None, None) => {
                    // Try to parse from string like DECIMAL(10,2)
                    parse_decimal_params(column_name, &type_upper)?
                }
            };
            // Validate precision and scale
            validate_decimal_params(column_name, p, s)?;
            DataType::Decimal128(p, s)
        }

        // Date/Time
        "DATE" => DataType::Date32,
        "TIME" => DataType::Time64(TimeUnit::Microsecond),
        "TIMESTAMP" | "DATETIME" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        }

        // Binary
        "BLOB" | "BYTEA" | "BINARY" | "VARBINARY" => DataType::Binary,

        // Special string-backed types
        "UUID" => DataType::Utf8,
        "JSON" => DataType::Utf8,

        _ => {
            return Err(ColumnTypeError {
                column_name: column_name.to_string(),
                message: format!(
                    "Unknown type '{}'. Supported types: {}",
                    type_str,
                    SUPPORTED_TYPES.join(", ")
                ),
            });
        }
    };

    Ok(data_type)
}

/// Parse DECIMAL(precision, scale) parameters from type string.
/// Returns (precision, scale) or error if invalid.
fn parse_decimal_params(column_name: &str, type_str: &str) -> Result<(u8, i8), ColumnTypeError> {
    // Look for pattern like DECIMAL(10,2) or DECIMAL(10)
    let has_open_paren = type_str.contains('(');
    let has_close_paren = type_str.contains(')');

    // Error on malformed parentheses (has one but not the other)
    if has_open_paren != has_close_paren {
        return Err(ColumnTypeError {
            column_name: column_name.to_string(),
            message: format!(
                "Malformed DECIMAL type '{}': mismatched parentheses",
                type_str
            ),
        });
    }

    if let Some(start) = type_str.find('(') {
        if let Some(end) = type_str.find(')') {
            // Ensure closing paren comes after opening paren
            if end <= start {
                return Err(ColumnTypeError {
                    column_name: column_name.to_string(),
                    message: format!(
                        "Malformed DECIMAL type '{}': invalid parentheses order",
                        type_str
                    ),
                });
            }
            let params = &type_str[start + 1..end];
            let parts: Vec<&str> = params.split(',').map(|s| s.trim()).collect();
            if parts.len() == 2 {
                let p = parts[0].parse::<u8>().map_err(|_| ColumnTypeError {
                    column_name: column_name.to_string(),
                    message: format!(
                        "Invalid DECIMAL precision '{}': must be a number 1-38",
                        parts[0]
                    ),
                })?;
                let s = parts[1].parse::<i8>().map_err(|_| ColumnTypeError {
                    column_name: column_name.to_string(),
                    message: format!("Invalid DECIMAL scale '{}': must be a number", parts[1]),
                })?;
                return Ok((p, s));
            } else if parts.len() == 1 && !parts[0].is_empty() {
                // DECIMAL(p) with default scale of 0
                let p = parts[0].parse::<u8>().map_err(|_| ColumnTypeError {
                    column_name: column_name.to_string(),
                    message: format!(
                        "Invalid DECIMAL precision '{}': must be a number 1-38",
                        parts[0]
                    ),
                })?;
                return Ok((p, 0));
            } else {
                // Empty or malformed parameters like DECIMAL() or DECIMAL(,,)
                return Err(ColumnTypeError {
                    column_name: column_name.to_string(),
                    message: format!(
                        "Malformed DECIMAL type '{}': expected DECIMAL(precision) or DECIMAL(precision, scale)",
                        type_str
                    ),
                });
            }
        }
    }
    // No parameters specified: default to (38, 10) like DuckDB
    Ok((38, 10))
}

/// Validate DECIMAL precision and scale values.
fn validate_decimal_params(
    column_name: &str,
    precision: u8,
    scale: i8,
) -> Result<(), ColumnTypeError> {
    if precision == 0 || precision > 38 {
        return Err(ColumnTypeError {
            column_name: column_name.to_string(),
            message: format!(
                "Invalid DECIMAL precision {}: must be between 1 and 38",
                precision
            ),
        });
    }
    if scale < 0 || scale as u8 > precision {
        return Err(ColumnTypeError {
            column_name: column_name.to_string(),
            message: format!(
                "Invalid DECIMAL scale {}: must be between 0 and precision ({})",
                scale, precision
            ),
        });
    }
    Ok(())
}

/// Build an Arrow schema from explicit column definitions, ordered by data column order.
///
/// The `data_columns` parameter provides the column names in the order they appear in the data.
/// This function validates that all columns match and builds the schema in data order.
pub fn build_schema_from_columns(
    columns: &HashMap<String, ColumnDefinition>,
    data_columns: &[String],
) -> Result<Arc<Schema>, SchemaError> {
    // Check for columns in data but not defined
    for data_col in data_columns {
        if !columns.contains_key(data_col) {
            return Err(SchemaError::ColumnNotDefined {
                column_name: data_col.clone(),
            });
        }
    }

    // Check for columns defined but not in data
    for defined_col in columns.keys() {
        if !data_columns.iter().any(|c| c == defined_col) {
            return Err(SchemaError::ColumnNotInData {
                column_name: defined_col.clone(),
            });
        }
    }

    // Build fields in data order
    let mut fields = Vec::with_capacity(data_columns.len());
    for col_name in data_columns {
        let definition = columns.get(col_name).unwrap(); // Safe: validated above
        let data_type =
            parse_column_type(col_name, definition).map_err(SchemaError::InvalidType)?;
        // Default to nullable=true for flexibility
        fields.push(Field::new(col_name, data_type, true));
    }

    Ok(Arc::new(Schema::new(fields)))
}

/// Build an Arrow schema directly from explicit column definitions, validated against observed fields.
///
/// The `observed_fields` parameter is the union of all field names seen when sampling the JSON data.
/// This allows sparse fields (fields that don't appear in every record) while still catching typos
/// (columns that don't appear anywhere in the data).
///
/// The schema is built in sorted column name order for determinism.
/// Missing fields in individual records will be treated as null by the reader.
///
/// Use this for formats like JSON where fields can be sparse/optional across records.
pub fn build_schema_from_columns_for_json(
    columns: &HashMap<String, ColumnDefinition>,
    observed_fields: &[String],
) -> Result<Arc<Schema>, SchemaError> {
    // Check for columns defined but never observed in the data
    // This catches typos like "scroe" instead of "score"
    for defined_col in columns.keys() {
        if !observed_fields.iter().any(|f| f == defined_col) {
            return Err(SchemaError::ColumnNotInData {
                column_name: defined_col.clone(),
            });
        }
    }

    // Check for observed fields not in column definitions
    for observed in observed_fields {
        if !columns.contains_key(observed) {
            return Err(SchemaError::ColumnNotDefined {
                column_name: observed.clone(),
            });
        }
    }

    // Sort column names for deterministic order
    let mut col_names: Vec<&String> = columns.keys().collect();
    col_names.sort();

    let mut fields = Vec::with_capacity(columns.len());
    for col_name in col_names {
        let definition = columns.get(col_name).unwrap();
        let data_type =
            parse_column_type(col_name, definition).map_err(SchemaError::InvalidType)?;
        // Default to nullable=true since JSON fields can be missing
        fields.push(Field::new(col_name.as_str(), data_type, true));
    }

    Ok(Arc::new(Schema::new(fields)))
}

/// Build an Arrow schema directly from explicit column definitions without validation.
///
/// This function does NOT validate against data columns at all.
/// The schema is built in sorted column name order for determinism.
///
/// WARNING: This can silently produce all-null columns if column names don't match data.
/// Prefer `build_schema_from_columns_for_json` which validates against observed fields.
pub fn build_schema_from_columns_unchecked(
    columns: &HashMap<String, ColumnDefinition>,
) -> Result<Arc<Schema>, SchemaError> {
    // Sort column names for deterministic order
    let mut col_names: Vec<&String> = columns.keys().collect();
    col_names.sort();

    let mut fields = Vec::with_capacity(columns.len());
    for col_name in col_names {
        let definition = columns.get(col_name).unwrap();
        let data_type =
            parse_column_type(col_name, definition).map_err(SchemaError::InvalidType)?;
        // Default to nullable=true since JSON fields can be missing
        fields.push(Field::new(col_name.as_str(), data_type, true));
    }

    Ok(Arc::new(Schema::new(fields)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::models::ColumnTypeSpec;

    #[test]
    fn test_parse_simple_varchar() {
        let result = parse_column_type("name", &ColumnDefinition::Simple("VARCHAR".to_string()));
        assert!(matches!(result, Ok(DataType::Utf8)));
    }

    #[test]
    fn test_parse_simple_int() {
        let result = parse_column_type("count", &ColumnDefinition::Simple("INT".to_string()));
        assert!(matches!(result, Ok(DataType::Int32)));
    }

    #[test]
    fn test_parse_simple_bigint() {
        let result = parse_column_type("id", &ColumnDefinition::Simple("BIGINT".to_string()));
        assert!(matches!(result, Ok(DataType::Int64)));
    }

    #[test]
    fn test_parse_simple_date() {
        let result = parse_column_type("created", &ColumnDefinition::Simple("DATE".to_string()));
        assert!(matches!(result, Ok(DataType::Date32)));
    }

    #[test]
    fn test_parse_simple_boolean() {
        let result = parse_column_type("active", &ColumnDefinition::Simple("BOOL".to_string()));
        assert!(matches!(result, Ok(DataType::Boolean)));
    }

    #[test]
    fn test_parse_case_insensitive() {
        let result = parse_column_type("name", &ColumnDefinition::Simple("varchar".to_string()));
        assert!(matches!(result, Ok(DataType::Utf8)));

        let result = parse_column_type("count", &ColumnDefinition::Simple("Integer".to_string()));
        assert!(matches!(result, Ok(DataType::Int32)));
    }

    #[test]
    fn test_parse_decimal_string() {
        let result = parse_column_type(
            "price",
            &ColumnDefinition::Simple("DECIMAL(10,2)".to_string()),
        );
        assert!(matches!(result, Ok(DataType::Decimal128(10, 2))));
    }

    #[test]
    fn test_parse_decimal_detailed() {
        let result = parse_column_type(
            "amount",
            &ColumnDefinition::Detailed(ColumnTypeSpec {
                data_type: "DECIMAL".to_string(),
                precision: Some(12),
                scale: Some(4),
            }),
        );
        assert!(matches!(result, Ok(DataType::Decimal128(12, 4))));
    }

    #[test]
    fn test_parse_unknown_type() {
        let result = parse_column_type("field", &ColumnDefinition::Simple("FOOBAR".to_string()));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("Unknown type"));
        assert!(err.message.contains("FOOBAR"));
    }

    #[test]
    fn test_build_schema_success() {
        let mut columns = HashMap::new();
        columns.insert(
            "name".to_string(),
            ColumnDefinition::Simple("VARCHAR".to_string()),
        );
        columns.insert(
            "age".to_string(),
            ColumnDefinition::Simple("INT".to_string()),
        );

        let data_columns = vec!["name".to_string(), "age".to_string()];
        let schema = build_schema_from_columns(&columns, &data_columns).unwrap();

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "name");
        assert!(matches!(schema.field(0).data_type(), DataType::Utf8));
        assert_eq!(schema.field(1).name(), "age");
        assert!(matches!(schema.field(1).data_type(), DataType::Int32));
    }

    #[test]
    fn test_build_schema_column_not_defined() {
        let mut columns = HashMap::new();
        columns.insert(
            "name".to_string(),
            ColumnDefinition::Simple("VARCHAR".to_string()),
        );

        let data_columns = vec!["name".to_string(), "extra".to_string()];
        let result = build_schema_from_columns(&columns, &data_columns);

        assert!(
            matches!(result, Err(SchemaError::ColumnNotDefined { column_name }) if column_name == "extra")
        );
    }

    #[test]
    fn test_build_schema_column_not_in_data() {
        let mut columns = HashMap::new();
        columns.insert(
            "name".to_string(),
            ColumnDefinition::Simple("VARCHAR".to_string()),
        );
        columns.insert(
            "missing".to_string(),
            ColumnDefinition::Simple("INT".to_string()),
        );

        let data_columns = vec!["name".to_string()];
        let result = build_schema_from_columns(&columns, &data_columns);

        assert!(
            matches!(result, Err(SchemaError::ColumnNotInData { column_name }) if column_name == "missing")
        );
    }

    #[test]
    fn test_build_schema_preserves_data_order() {
        let mut columns = HashMap::new();
        columns.insert(
            "z_col".to_string(),
            ColumnDefinition::Simple("VARCHAR".to_string()),
        );
        columns.insert(
            "a_col".to_string(),
            ColumnDefinition::Simple("INT".to_string()),
        );
        columns.insert(
            "m_col".to_string(),
            ColumnDefinition::Simple("DATE".to_string()),
        );

        // Data order is different from alphabetical
        let data_columns = vec![
            "a_col".to_string(),
            "z_col".to_string(),
            "m_col".to_string(),
        ];
        let schema = build_schema_from_columns(&columns, &data_columns).unwrap();

        assert_eq!(schema.field(0).name(), "a_col");
        assert_eq!(schema.field(1).name(), "z_col");
        assert_eq!(schema.field(2).name(), "m_col");
    }

    // =========================================================================
    // JSON schema building tests (validated against observed fields)
    // =========================================================================

    #[test]
    fn test_build_schema_for_json_success() {
        let mut columns = HashMap::new();
        columns.insert(
            "name".to_string(),
            ColumnDefinition::Simple("VARCHAR".to_string()),
        );
        columns.insert(
            "age".to_string(),
            ColumnDefinition::Simple("INT".to_string()),
        );

        let observed = vec!["name".to_string(), "age".to_string()];
        let schema = build_schema_from_columns_for_json(&columns, &observed).unwrap();

        // Schema should be in sorted order
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "age");
        assert_eq!(schema.field(1).name(), "name");
    }

    #[test]
    fn test_build_schema_for_json_typo_rejected() {
        let mut columns = HashMap::new();
        columns.insert(
            "name".to_string(),
            ColumnDefinition::Simple("VARCHAR".to_string()),
        );
        columns.insert(
            "scroe".to_string(),
            ColumnDefinition::Simple("INT".to_string()),
        ); // typo

        // Observed fields have the correct spelling
        let observed = vec!["name".to_string(), "score".to_string()];
        let result = build_schema_from_columns_for_json(&columns, &observed);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, SchemaError::ColumnNotInData { .. }));
    }

    #[test]
    fn test_build_schema_for_json_extra_observed_rejected() {
        let mut columns = HashMap::new();
        columns.insert(
            "name".to_string(),
            ColumnDefinition::Simple("VARCHAR".to_string()),
        );

        // Observed has an extra field not in columns
        let observed = vec!["name".to_string(), "extra".to_string()];
        let result = build_schema_from_columns_for_json(&columns, &observed);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, SchemaError::ColumnNotDefined { .. }));
    }

    #[test]
    fn test_build_schema_for_json_all_nullable() {
        let mut columns = HashMap::new();
        columns.insert(
            "name".to_string(),
            ColumnDefinition::Simple("VARCHAR".to_string()),
        );
        columns.insert(
            "age".to_string(),
            ColumnDefinition::Simple("INT".to_string()),
        );

        let observed = vec!["name".to_string(), "age".to_string()];
        let schema = build_schema_from_columns_for_json(&columns, &observed).unwrap();

        // All fields should be nullable for sparse JSON support
        assert!(schema.field(0).is_nullable());
        assert!(schema.field(1).is_nullable());
    }

    // =========================================================================
    // Unchecked schema building tests (legacy, prefer build_schema_from_columns_for_json)
    // =========================================================================

    #[test]
    fn test_build_schema_unchecked_sorted_order() {
        let mut columns = HashMap::new();
        columns.insert(
            "z_col".to_string(),
            ColumnDefinition::Simple("VARCHAR".to_string()),
        );
        columns.insert(
            "a_col".to_string(),
            ColumnDefinition::Simple("INT".to_string()),
        );
        columns.insert(
            "m_col".to_string(),
            ColumnDefinition::Simple("DATE".to_string()),
        );

        let schema = build_schema_from_columns_unchecked(&columns).unwrap();

        // Should be in sorted order
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "a_col");
        assert_eq!(schema.field(1).name(), "m_col");
        assert_eq!(schema.field(2).name(), "z_col");
    }

    #[test]
    fn test_build_schema_unchecked_all_nullable() {
        let mut columns = HashMap::new();
        columns.insert(
            "name".to_string(),
            ColumnDefinition::Simple("VARCHAR".to_string()),
        );
        columns.insert(
            "age".to_string(),
            ColumnDefinition::Simple("INT".to_string()),
        );

        let schema = build_schema_from_columns_unchecked(&columns).unwrap();

        // All fields should be nullable for sparse JSON support
        assert!(schema.field(0).is_nullable());
        assert!(schema.field(1).is_nullable());
    }

    // =========================================================================
    // DECIMAL validation tests
    // =========================================================================

    #[test]
    fn test_parse_decimal_precision_only() {
        // Precision-only should default scale to 0
        let result = parse_column_type(
            "amount",
            &ColumnDefinition::Detailed(ColumnTypeSpec {
                data_type: "DECIMAL".to_string(),
                precision: Some(12),
                scale: None,
            }),
        );
        assert!(matches!(result, Ok(DataType::Decimal128(12, 0))));
    }

    #[test]
    fn test_parse_decimal_string_precision_only() {
        let result = parse_column_type(
            "amount",
            &ColumnDefinition::Simple("DECIMAL(18)".to_string()),
        );
        assert!(matches!(result, Ok(DataType::Decimal128(18, 0))));
    }

    #[test]
    fn test_parse_decimal_default() {
        // No params should default to (38, 10)
        let result = parse_column_type("amount", &ColumnDefinition::Simple("DECIMAL".to_string()));
        assert!(matches!(result, Ok(DataType::Decimal128(38, 10))));
    }

    #[test]
    fn test_decimal_precision_zero_rejected() {
        let result = parse_column_type(
            "amount",
            &ColumnDefinition::Detailed(ColumnTypeSpec {
                data_type: "DECIMAL".to_string(),
                precision: Some(0),
                scale: Some(0),
            }),
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("precision"));
        assert!(err.message.contains("1 and 38"));
    }

    #[test]
    fn test_decimal_precision_too_large_rejected() {
        let result = parse_column_type(
            "amount",
            &ColumnDefinition::Detailed(ColumnTypeSpec {
                data_type: "DECIMAL".to_string(),
                precision: Some(39),
                scale: Some(2),
            }),
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("precision"));
    }

    #[test]
    fn test_decimal_scale_negative_rejected() {
        let result = parse_column_type(
            "amount",
            &ColumnDefinition::Detailed(ColumnTypeSpec {
                data_type: "DECIMAL".to_string(),
                precision: Some(10),
                scale: Some(-1),
            }),
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("scale"));
    }

    #[test]
    fn test_decimal_scale_exceeds_precision_rejected() {
        let result = parse_column_type(
            "amount",
            &ColumnDefinition::Detailed(ColumnTypeSpec {
                data_type: "DECIMAL".to_string(),
                precision: Some(5),
                scale: Some(10),
            }),
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("scale"));
        assert!(err.message.contains("precision"));
    }

    #[test]
    fn test_decimal_string_invalid_precision_rejected() {
        let result = parse_column_type(
            "amount",
            &ColumnDefinition::Simple("DECIMAL(abc,2)".to_string()),
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("precision"));
    }

    #[test]
    fn test_decimal_missing_close_paren_rejected() {
        let result = parse_column_type(
            "amount",
            &ColumnDefinition::Simple("DECIMAL(10,2".to_string()),
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("mismatched parentheses"));
    }

    #[test]
    fn test_decimal_missing_open_paren_rejected() {
        let result = parse_column_type(
            "amount",
            &ColumnDefinition::Simple("DECIMAL10,2)".to_string()),
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("mismatched parentheses"));
    }

    #[test]
    fn test_decimal_empty_parens_rejected() {
        let result =
            parse_column_type("amount", &ColumnDefinition::Simple("DECIMAL()".to_string()));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("Malformed DECIMAL"));
    }
}
