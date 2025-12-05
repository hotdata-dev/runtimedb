use crate::datafetch::DataFetchError;
use std::collections::HashMap;
use std::path::PathBuf;

/// Manages loading of ADBC driver shared libraries
#[derive(Debug)]
pub struct DriverManager {
    driver_paths: HashMap<String, PathBuf>,
}

impl DriverManager {
    /// Create a new DriverManager and discover available drivers
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

        // First, check for vendored drivers in OUT_DIR
        if let Ok(driver_dir) = std::env::var("RIVETDB_DRIVER_DIR") {
            let driver_path = PathBuf::from(&driver_dir);

            // Platform-specific driver filenames
            #[cfg(target_os = "macos")]
            let driver_specs = [
                ("duckdb", "libduckdb.dylib"),
                ("postgres", "libadbc_driver_postgresql.dylib"),
                ("snowflake", "libadbc_driver_snowflake.dylib"),
            ];

            #[cfg(target_os = "linux")]
            let driver_specs = [
                ("duckdb", "libduckdb.so"),
                ("postgres", "libadbc_driver_postgresql.so"),
                ("snowflake", "libadbc_driver_snowflake.so"),
            ];

            #[cfg(target_os = "windows")]
            let driver_specs = [
                ("duckdb", "duckdb.dll"),
                ("postgres", "adbc_driver_postgresql.dll"),
                ("snowflake", "adbc_driver_snowflake.dll"),
            ];

            for (driver_name, filename) in driver_specs {
                let full_path = driver_path.join(filename);
                if full_path.exists() {
                    paths.insert(driver_name.to_string(), full_path);
                }
            }
        }

        // Environment variables override vendored drivers
        if let Ok(path) = std::env::var("RIVETDB_DRIVER_DUCKDB") {
            paths.insert("duckdb".to_string(), PathBuf::from(path));
        }
        if let Ok(path) = std::env::var("RIVETDB_DRIVER_POSTGRESQL") {
            paths.insert("postgres".to_string(), PathBuf::from(path));
        }
        if let Ok(path) = std::env::var("RIVETDB_DRIVER_SNOWFLAKE") {
            paths.insert("snowflake".to_string(), PathBuf::from(path));
        }

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