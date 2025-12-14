use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub catalog: CatalogConfig,
    pub storage: StorageConfig,
    #[serde(default)]
    pub paths: PathsConfig,
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
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct PathsConfig {
    pub cache_dir: Option<String>,
}

impl AppConfig {
    /// Load configuration from file and environment variables
    pub fn load(config_path: &str) -> Result<Self> {
        let mut builder = config::Config::builder();

        // Load from config file if provided

        builder = builder.add_source(config::File::with_name(config_path));

        // Add environment variables with prefix RIVETDB_
        // Example: RIVETDB_SERVER_PORT=8080
        builder = builder.add_source(
            config::Environment::with_prefix("RIVETDB")
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
