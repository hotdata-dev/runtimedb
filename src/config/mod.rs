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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CatalogConfig {
    #[serde(rename = "type")]
    pub catalog_type: String,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub database: Option<String>,
    pub user: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StorageConfig {
    #[serde(rename = "type")]
    pub storage_type: String,
    pub bucket: Option<String>,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    /// Enable S3 compatibility layer for non-standard S3 backends (e.g. NVIDIA AIStore).
    /// Wraps the object store to handle quirks like non-standard HTTP error codes.
    #[serde(default)]
    pub s3_compat: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct PathsConfig {
    /// Base directory for all RuntimeDB data (catalog.db, cache/).
    /// Defaults to ~/.hotdata/runtimedb
    pub base_dir: Option<String>,
    /// Cache directory for Parquet files. Defaults to {base_dir}/cache
    pub cache_dir: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct SecretsConfig {
    /// Encryption key for secrets (base64-encoded 32-byte key).
    /// Can also be set via RUNTIMEDB_SECRET_KEY environment variable.
    pub encryption_key: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct LiquidCacheConfig {
    /// Whether liquid cache is enabled
    #[serde(default)]
    pub enabled: bool,
    /// Liquid cache server address (e.g., "http://localhost:15214")
    pub server_address: Option<String>,
    /// Run liquid cache in local (in-process) mode instead of connecting to an external server
    #[serde(default)]
    pub local: bool,
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
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            max_concurrent_persistence: default_max_concurrent_persistence(),
            stale_result_cleanup_interval_secs: default_stale_result_cleanup_interval_secs(),
            stale_result_timeout_secs: default_stale_result_timeout_secs(),
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
        // Example: RUNTIMEDB_SERVER_PORT=8080
        builder = builder.add_source(
            config::Environment::with_prefix("RUNTIMEDB")
                .separator("_")
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
