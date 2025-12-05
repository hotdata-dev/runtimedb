use crate::datafetch::DataFetchError;
use std::collections::HashMap;
use std::path::PathBuf;

/// Manages loading of ADBC driver shared libraries
#[derive(Debug)]
pub struct DriverManager {
    driver_paths: HashMap<String, PathBuf>,
}

impl DriverManager {
    pub fn new() -> Self {
        Self {
            driver_paths: Self::discover_driver_paths(),
        }
    }

    /// Get the path to the driver library for a given source type
    pub fn driver_path(&self, source_type: &str) -> Result<&PathBuf, DataFetchError> {
        // Normalize source type (motherduck uses duckdb driver)
        let driver_key = match source_type {
            "motherduck" => "duckdb",
            other => other,
        };

        self.driver_paths
            .get(driver_key)
            .ok_or_else(|| DataFetchError::UnsupportedDriver(source_type.to_string()))
    }

    fn discover_driver_paths() -> HashMap<String, PathBuf> {
        let mut paths = HashMap::new();

        // Check environment variables first
        if let Ok(path) = std::env::var("RIVETDB_DRIVER_DUCKDB") {
            paths.insert("duckdb".to_string(), PathBuf::from(path));
        }
        if let Ok(path) = std::env::var("RIVETDB_DRIVER_POSTGRESQL") {
            paths.insert("postgres".to_string(), PathBuf::from(path));
        }
        if let Ok(path) = std::env::var("RIVETDB_DRIVER_SNOWFLAKE") {
            paths.insert("snowflake".to_string(), PathBuf::from(path));
        }

        // TODO: Add vendored driver paths from OUT_DIR

        paths
    }

    /// Get the driver entry point function name for a source type
    pub fn driver_entrypoint(&self, source_type: &str) -> &'static str {
        match source_type {
            "duckdb" | "motherduck" => "duckdb_adbc_init",
            "postgres" => "AdbcDriverPostgreSQLInit",
            "snowflake" => "AdbcDriverSnowflakeInit",
            _ => "AdbcDriverInit",
        }
    }
}

impl Default for DriverManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_motherduck_uses_duckdb_driver() {
        let manager = DriverManager::new();
        // Both should resolve to same driver (if configured)
        let duckdb_result = manager.driver_path("duckdb");
        let motherduck_result = manager.driver_path("motherduck");

        // They should either both succeed or both fail
        assert_eq!(duckdb_result.is_ok(), motherduck_result.is_ok());
    }

    #[test]
    fn test_unsupported_driver() {
        let manager = DriverManager::new();
        let result = manager.driver_path("unsupported_db");
        assert!(matches!(result, Err(DataFetchError::UnsupportedDriver(_))));
    }
}