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

        Ok(())
    }
}
