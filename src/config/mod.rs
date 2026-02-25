use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub catalog: CatalogConfig,
    pub storage: StorageConfig,
    #[serde(default)]
    pub paths: PathsConfig,
    #[serde(default)]
    pub secrets: SecretsConfig,
    #[serde(default)]
    pub liquid_cache: LiquidCacheConfig,
    #[serde(default)]
    pub cache: CacheConfig,
    #[serde(default)]
    pub engine: EngineConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
}

fn default_host() -> String {
    "127.0.0.1".to_string()
}

fn default_port() -> u16 {
    3000
}

#[derive(Clone, Deserialize, Serialize)]
pub struct CatalogConfig {
    #[serde(rename = "type")]
    pub catalog_type: String,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub database: Option<String>,
    pub user: Option<String>,
    pub password: Option<String>,
}

impl std::fmt::Debug for CatalogConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogConfig")
            .field("catalog_type", &self.catalog_type)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("database", &self.database)
            .field("user", &self.user)
            .field(
                "password",
                if self.password.is_some() {
                    &"[REDACTED]" as &dyn std::fmt::Debug
                } else {
                    &"[NOT SET]" as &dyn std::fmt::Debug
                },
            )
            .finish()
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub struct StorageConfig {
    #[serde(rename = "type")]
    pub storage_type: String,
    pub bucket: Option<String>,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    /// Optional raw `Authorization` header value for endpoint S3 requests.
    ///
    /// Example: `Bearer <jwt-token>`.
    /// When this is set, RuntimeDB disables SigV4 signing for endpoint storage
    /// so this value is preserved on outbound requests.
    pub authorization_header: Option<String>,
    /// Enable S3 compatibility layer for non-standard S3 backends (e.g. NVIDIA AIStore).
    /// Wraps the object store to handle quirks like non-standard HTTP error codes.
    #[serde(default)]
    pub s3_compat: bool,
}

impl std::fmt::Debug for StorageConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageConfig")
            .field("storage_type", &self.storage_type)
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("endpoint", &self.endpoint)
            .field(
                "access_key",
                if self.access_key.is_some() {
                    &"[REDACTED]" as &dyn std::fmt::Debug
                } else {
                    &"[NOT SET]" as &dyn std::fmt::Debug
                },
            )
            .field(
                "secret_key",
                if self.secret_key.is_some() {
                    &"[REDACTED]" as &dyn std::fmt::Debug
                } else {
                    &"[NOT SET]" as &dyn std::fmt::Debug
                },
            )
            .field(
                "authorization_header",
                if self.authorization_header.is_some() {
                    &"[REDACTED]" as &dyn std::fmt::Debug
                } else {
                    &"[NOT SET]" as &dyn std::fmt::Debug
                },
            )
            .field("s3_compat", &self.s3_compat)
            .finish()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct PathsConfig {
    /// Base directory for all RuntimeDB data (catalog.db, cache/).
    /// Defaults to ~/.hotdata/runtimedb
    pub base_dir: Option<String>,
    /// Cache directory for Parquet files. Defaults to {base_dir}/cache
    pub cache_dir: Option<String>,
}

#[derive(Clone, Deserialize, Serialize, Default)]
pub struct SecretsConfig {
    /// Encryption key for secrets (base64-encoded 32-byte key).
    /// Can also be set via RUNTIMEDB_SECRET_KEY environment variable.
    pub encryption_key: Option<String>,
}

impl std::fmt::Debug for SecretsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecretsConfig")
            .field(
                "encryption_key",
                if self.encryption_key.is_some() {
                    &"[REDACTED]" as &dyn std::fmt::Debug
                } else {
                    &"[NOT SET]" as &dyn std::fmt::Debug
                },
            )
            .finish()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct LiquidCacheConfig {
    /// Whether liquid cache is enabled
    #[serde(default)]
    pub enabled: bool,
    /// Liquid cache server address (e.g., "http://localhost:15214")
    pub server_address: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EngineConfig {
    /// Maximum number of concurrent result persistence tasks.
    /// When queries are executed, results are persisted to parquet in background tasks.
    /// This limits how many can run concurrently. Default: 32.
    #[serde(default = "default_max_concurrent_persistence")]
    pub max_concurrent_persistence: usize,

    /// Interval in seconds for cleaning up stale results stuck in pending/processing.
    /// Results older than `stale_result_timeout_secs` are marked as failed.
    /// Set to 0 to disable. Default: 60 (1 minute).
    #[serde(default = "default_stale_result_cleanup_interval_secs")]
    pub stale_result_cleanup_interval_secs: u64,

    /// Timeout in seconds after which pending/processing results are considered stale.
    /// Results stuck longer than this are marked as failed during cleanup.
    /// Default: 300 (5 minutes).
    #[serde(default = "default_stale_result_timeout_secs")]
    pub stale_result_timeout_secs: u64,

    /// Number of days to retain query results before automatic cleanup.
    /// Results older than this (in ready/failed status) are deleted along with their parquet files.
    /// Set to 0 to disable automatic cleanup (keep results indefinitely).
    /// Default: 7.
    #[serde(default = "default_result_retention_days")]
    pub result_retention_days: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            max_concurrent_persistence: default_max_concurrent_persistence(),
            stale_result_cleanup_interval_secs: default_stale_result_cleanup_interval_secs(),
            stale_result_timeout_secs: default_stale_result_timeout_secs(),
            result_retention_days: default_result_retention_days(),
        }
    }
}

fn default_max_concurrent_persistence() -> usize {
    32
}

fn default_stale_result_cleanup_interval_secs() -> u64 {
    60
}

fn default_stale_result_timeout_secs() -> u64 {
    300
}

pub fn default_result_retention_days() -> u64 {
    7
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct CacheConfig {
    /// Redis connection URL. None = caching disabled.
    pub redis_url: Option<String>,
    /// TTL in seconds. How long items should remain cached before removal. Default: 1800 (30 minutes).
    #[serde(default = "default_cache_ttl")]
    pub ttl_secs: u64,
    /// Background refresh interval in seconds. 0 = disabled. Default: 0.
    /// When enabled, periodically refreshes cached metadata to keep it warm.
    /// Can be used with ttl_secs to eliminate cache misses.
    #[serde(default)]
    pub refresh_interval_secs: u64,
    /// Key prefix. Default: "rdb:"
    #[serde(default = "default_cache_key_prefix")]
    pub key_prefix: String,
}

impl CacheConfig {
    /// Compute the distributed lock TTL for cache refresh.
    /// Clamped to [60, 300] seconds (1-5 minutes).
    pub fn refresh_lock_ttl_secs(&self) -> u64 {
        (self.refresh_interval_secs / 2).clamp(60, 300)
    }
}

fn default_cache_ttl() -> u64 {
    1800
}

fn default_cache_key_prefix() -> String {
    "rdb:".to_string()
}

impl AppConfig {
    /// Load configuration from file and environment variables
    pub fn load(config_path: &str) -> Result<Self> {
        let mut builder = config::Config::builder();

        // Load from config file if provided

        builder = builder.add_source(config::File::with_name(config_path));

        // Add environment variables with prefix RUNTIMEDB_
        // Example: RUNTIMEDB_SERVER__PORT=8080
        builder = builder.add_source(
            config::Environment::with_prefix("RUNTIMEDB")
                .prefix_separator("_")
                .separator("__")
                .try_parsing(true),
        );

        let config = builder.build().context("Failed to build configuration")?;

        config
            .try_deserialize()
            .context("Failed to deserialize configuration")
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<()> {
        // Validate catalog config
        match self.catalog.catalog_type.as_str() {
            "postgres" => {
                if self.catalog.host.is_none() {
                    anyhow::bail!("Postgres catalog requires 'host'");
                }
                if self.catalog.database.is_none() {
                    anyhow::bail!("Postgres catalog requires 'database'");
                }
                if self.catalog.user.is_none() {
                    anyhow::bail!("Postgres catalog requires 'user'");
                }
                if self.catalog.password.is_none() {
                    anyhow::bail!("Postgres catalog requires 'password'");
                }
            }
            "sqlite" => {
                // SQLite uses paths config, no additional validation needed
            }
            _ => anyhow::bail!("Invalid catalog type: {}", self.catalog.catalog_type),
        }

        // Validate storage config
        match self.storage.storage_type.as_str() {
            "s3" => {
                if self.storage.bucket.is_none() {
                    anyhow::bail!("S3 storage requires 'bucket'");
                }
            }
            "filesystem" => {
                // Filesystem storage uses paths config, no additional validation needed
            }
            _ => anyhow::bail!("Invalid storage type: {}", self.storage.storage_type),
        }

        // Validate cache config
        if self.cache.refresh_interval_secs > 0
            && self.cache.refresh_interval_secs >= self.cache.ttl_secs
        {
            anyhow::bail!(
                "Cache refresh_interval_secs ({}) must be less than hard_ttl_secs ({})",
                self.cache.refresh_interval_secs,
                self.cache.ttl_secs
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    #[test]
    fn test_load_supports_double_underscore_env_separator() {
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        let temp_file = tempfile::Builder::new().suffix(".toml").tempfile().unwrap();
        std::fs::write(
            temp_file.path(),
            r#"
[server]
host = "127.0.0.1"
port = 3000

[catalog]
type = "sqlite"

[storage]
type = "filesystem"
"#,
        )
        .unwrap();

        std::env::set_var(
            "RUNTIMEDB_STORAGE__AUTHORIZATION_HEADER",
            "Bearer test-token",
        );
        std::env::set_var("RUNTIMEDB_SERVER__PORT", "3111");

        let config = AppConfig::load(temp_file.path().to_str().unwrap()).unwrap();
        assert_eq!(
            config.storage.authorization_header.as_deref(),
            Some("Bearer test-token")
        );
        assert_eq!(config.server.port, 3111);

        std::env::remove_var("RUNTIMEDB_STORAGE__AUTHORIZATION_HEADER");
        std::env::remove_var("RUNTIMEDB_SERVER__PORT");
    }

    #[test]
    fn test_engine_config_result_retention_days_default() {
        let config = EngineConfig::default();
        assert_eq!(config.result_retention_days, 7);
    }

    #[test]
    fn test_engine_config_result_retention_days_from_env() {
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        let temp_file = tempfile::Builder::new().suffix(".toml").tempfile().unwrap();
        std::fs::write(
            temp_file.path(),
            r#"
[server]
host = "127.0.0.1"
port = 3000

[catalog]
type = "sqlite"

[storage]
type = "filesystem"
"#,
        )
        .unwrap();

        std::env::set_var("RUNTIMEDB_ENGINE__RESULT_RETENTION_DAYS", "30");

        let config = AppConfig::load(temp_file.path().to_str().unwrap()).unwrap();
        assert_eq!(config.engine.result_retention_days, 30);

        std::env::remove_var("RUNTIMEDB_ENGINE__RESULT_RETENTION_DAYS");
    }
}
