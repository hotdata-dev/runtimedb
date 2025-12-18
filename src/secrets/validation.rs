/// Validates a secret name and returns the normalized (lowercase) form.
/// Returns an error if the name doesn't match the allowed pattern.
///
/// Valid names: 1-128 characters, alphanumeric with _ and - only.
pub fn validate_and_normalize_name(name: &str) -> Result<String, ValidationError> {
    if name.is_empty() || name.len() > 128 {
        return Err(ValidationError::InvalidSecretName(name.to_string()));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(ValidationError::InvalidSecretName(name.to_string()));
    }
    Ok(name.to_lowercase())
}

#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error(
        "Invalid secret name '{0:.128}': must be 1-128 characters, alphanumeric with _ and - only"
    )]
    InvalidSecretName(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_names() {
        assert_eq!(
            validate_and_normalize_name("my-secret").unwrap(),
            "my-secret"
        );
        assert_eq!(
            validate_and_normalize_name("MY_SECRET").unwrap(),
            "my_secret"
        );
        assert_eq!(
            validate_and_normalize_name("secret123").unwrap(),
            "secret123"
        );
        assert_eq!(validate_and_normalize_name("a").unwrap(), "a");
        assert_eq!(validate_and_normalize_name("A-B_c-1").unwrap(), "a-b_c-1");
    }

    #[test]
    fn test_invalid_names() {
        assert!(validate_and_normalize_name("").is_err());
        assert!(validate_and_normalize_name("has space").is_err());
        assert!(validate_and_normalize_name("has.dot").is_err());
        assert!(validate_and_normalize_name("has/slash").is_err());
        assert!(validate_and_normalize_name(&"a".repeat(129)).is_err());
    }

    #[test]
    fn test_max_length_name() {
        let max_name = "a".repeat(128);
        assert!(validate_and_normalize_name(&max_name).is_ok());
    }
}
