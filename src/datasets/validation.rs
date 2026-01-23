//! Validation utilities for dataset table names.

use std::collections::HashSet;
use std::sync::LazyLock;

/// Maximum length for a table name.
pub const MAX_TABLE_NAME_LENGTH: usize = 128;

/// SQL reserved words that cannot be used as table names.
static RESERVED_WORDS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "select", "from", "where", "insert", "update", "delete", "create", "drop", "alter",
        "table", "index", "view", "and", "or", "not", "null", "true", "false", "in", "is", "like",
        "between", "join", "on", "as", "order", "by", "group", "having", "limit", "offset",
        "union", "all", "distinct", "case", "when", "then", "else", "end", "exists", "any", "some",
    ]
    .into_iter()
    .collect()
});

/// Error type for table name validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableNameError {
    Empty,
    TooLong(usize),
    InvalidFirstChar(char),
    InvalidChar(char),
    ReservedWord(String),
}

impl std::fmt::Display for TableNameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(f, "table name cannot be empty"),
            Self::TooLong(len) => write!(
                f,
                "table name exceeds maximum length of {} (got {})",
                MAX_TABLE_NAME_LENGTH, len
            ),
            Self::InvalidFirstChar(c) => write!(
                f,
                "table name must start with a letter or underscore, got '{}'",
                c
            ),
            Self::InvalidChar(c) => {
                write!(f, "table name contains invalid character '{}'", c)
            }
            Self::ReservedWord(word) => write!(
                f,
                "'{}' is a SQL reserved word and cannot be used as a table name",
                word
            ),
        }
    }
}

impl std::error::Error for TableNameError {}

/// Validate a table name.
///
/// A valid table name must:
/// - Not be empty
/// - Not exceed 128 characters
/// - Start with a letter or underscore
/// - Contain only alphanumeric characters and underscores
/// - Not be a SQL reserved word
pub fn validate_table_name(name: &str) -> Result<(), TableNameError> {
    if name.is_empty() {
        return Err(TableNameError::Empty);
    }

    if name.len() > MAX_TABLE_NAME_LENGTH {
        return Err(TableNameError::TooLong(name.len()));
    }

    let mut chars = name.chars();

    // First character must be letter or underscore
    if let Some(first) = chars.next() {
        if !first.is_ascii_alphabetic() && first != '_' {
            return Err(TableNameError::InvalidFirstChar(first));
        }
    }

    // Remaining characters must be alphanumeric or underscore
    for c in chars {
        if !c.is_ascii_alphanumeric() && c != '_' {
            return Err(TableNameError::InvalidChar(c));
        }
    }

    // Check for reserved words
    let lower = name.to_lowercase();
    if RESERVED_WORDS.contains(lower.as_str()) {
        return Err(TableNameError::ReservedWord(name.to_string()));
    }

    Ok(())
}

/// Generate a table name from a label.
///
/// Converts a human-readable label into a valid SQL table name by:
/// - Converting to lowercase
/// - Replacing whitespace and dashes with underscores
/// - Stripping non-alphanumeric characters (except underscores)
/// - Prefixing with underscore if it starts with a digit
/// - Truncating to maximum length
/// - Using "_unnamed" for empty results
pub fn table_name_from_label(label: &str) -> String {
    let mut result = String::with_capacity(label.len());

    for c in label.chars() {
        if c.is_ascii_alphanumeric() {
            result.push(c.to_ascii_lowercase());
        } else if (c.is_whitespace() || c == '-') && !result.ends_with('_') {
            result.push('_');
        }
        // Skip other characters
    }

    // Trim trailing underscores
    while result.ends_with('_') {
        result.pop();
    }

    // If empty after processing, use a default
    if result.is_empty() {
        return "_unnamed".to_string();
    }

    // Ensure starts with letter or underscore
    if result
        .chars()
        .next()
        .map(|c| c.is_ascii_digit())
        .unwrap_or(false)
    {
        result = format!("_{}", result);
    }

    // Truncate if needed
    if result.len() > MAX_TABLE_NAME_LENGTH {
        result.truncate(MAX_TABLE_NAME_LENGTH);
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    // === Valid table names ===

    #[test]
    fn test_valid_simple_name() {
        assert!(validate_table_name("users").is_ok());
    }

    #[test]
    fn test_valid_with_underscore_prefix() {
        assert!(validate_table_name("_private").is_ok());
    }

    #[test]
    fn test_valid_with_numbers() {
        assert!(validate_table_name("table_123").is_ok());
    }

    #[test]
    fn test_valid_mixed_case() {
        assert!(validate_table_name("CamelCase").is_ok());
    }

    #[test]
    fn test_valid_single_char() {
        assert!(validate_table_name("a").is_ok());
    }

    #[test]
    fn test_valid_underscore_only() {
        assert!(validate_table_name("_").is_ok());
    }

    // === Invalid: empty ===

    #[test]
    fn test_invalid_empty() {
        assert!(matches!(
            validate_table_name(""),
            Err(TableNameError::Empty)
        ));
    }

    // === Invalid: first character ===

    #[test]
    fn test_invalid_starts_with_number() {
        assert!(matches!(
            validate_table_name("123abc"),
            Err(TableNameError::InvalidFirstChar('1'))
        ));
    }

    #[test]
    fn test_invalid_starts_with_dash() {
        assert!(matches!(
            validate_table_name("-test"),
            Err(TableNameError::InvalidFirstChar('-'))
        ));
    }

    #[test]
    fn test_invalid_starts_with_space() {
        assert!(matches!(
            validate_table_name(" test"),
            Err(TableNameError::InvalidFirstChar(' '))
        ));
    }

    // === Invalid: characters ===

    #[test]
    fn test_invalid_contains_dash() {
        assert!(matches!(
            validate_table_name("table-name"),
            Err(TableNameError::InvalidChar('-'))
        ));
    }

    #[test]
    fn test_invalid_contains_space() {
        assert!(matches!(
            validate_table_name("table name"),
            Err(TableNameError::InvalidChar(' '))
        ));
    }

    #[test]
    fn test_invalid_contains_dot() {
        assert!(matches!(
            validate_table_name("table.name"),
            Err(TableNameError::InvalidChar('.'))
        ));
    }

    // === Invalid: reserved words ===

    #[test]
    fn test_invalid_reserved_select() {
        assert!(matches!(
            validate_table_name("select"),
            Err(TableNameError::ReservedWord(_))
        ));
    }

    #[test]
    fn test_invalid_reserved_from_uppercase() {
        assert!(matches!(
            validate_table_name("FROM"),
            Err(TableNameError::ReservedWord(_))
        ));
    }

    #[test]
    fn test_invalid_reserved_table() {
        assert!(matches!(
            validate_table_name("table"),
            Err(TableNameError::ReservedWord(_))
        ));
    }

    // === Invalid: too long ===

    #[test]
    fn test_invalid_too_long() {
        let long_name = "a".repeat(129);
        assert!(matches!(
            validate_table_name(&long_name),
            Err(TableNameError::TooLong(129))
        ));
    }

    #[test]
    fn test_valid_max_length() {
        let max_name = "a".repeat(128);
        assert!(validate_table_name(&max_name).is_ok());
    }

    // === Label to table name generation ===

    #[test]
    fn test_label_simple_conversion() {
        assert_eq!(table_name_from_label("Q1 Sales Report"), "q1_sales_report");
    }

    #[test]
    fn test_label_with_leading_number() {
        assert_eq!(table_name_from_label("123 Numbers"), "_123_numbers");
    }

    #[test]
    fn test_label_with_dashes() {
        assert_eq!(table_name_from_label("Hello-World"), "hello_world");
    }

    #[test]
    fn test_label_empty() {
        assert_eq!(table_name_from_label(""), "_unnamed");
    }

    #[test]
    fn test_label_special_chars_stripped() {
        assert_eq!(table_name_from_label("Test! @#$% Data"), "test_data");
    }

    #[test]
    fn test_label_unicode_stripped() {
        assert_eq!(table_name_from_label("Sales 📊 Report"), "sales_report");
    }

    #[test]
    fn test_label_truncated_if_too_long() {
        let long_label = "a".repeat(200);
        let result = table_name_from_label(&long_label);
        assert!(result.len() <= MAX_TABLE_NAME_LENGTH);
    }
}
