use crate::secrets::SecretManager;
use serde::{Deserialize, Serialize};

/// Credential storage - either no credential or a reference to a stored secret.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Credential {
    #[default]
    None,
    SecretRef {
        /// Resource ID of the secret (secr_...)
        id: String,
    },
}

impl Credential {
    /// Create a SecretRef credential with the given ID.
    pub fn secret_ref(id: impl Into<String>) -> Self {
        Credential::SecretRef { id: id.into() }
    }

    /// Resolve the credential to a plaintext string.
    /// Returns an error if the credential is None or the secret cannot be found/decoded.
    pub async fn resolve(&self, secrets: &SecretManager) -> anyhow::Result<String> {
        match self {
            Credential::None => Err(anyhow::anyhow!("no credential configured")),
            Credential::SecretRef { id } => secrets
                .get_string_by_id(id)
                .await
                .map_err(|e| anyhow::anyhow!("failed to resolve secret '{}': {}", id, e)),
        }
    }

    /// Get the secret ID if this is a SecretRef.
    pub fn secret_id(&self) -> Option<&str> {
        match self {
            Credential::None => None,
            Credential::SecretRef { id } => Some(id),
        }
    }
}

/// Iceberg catalog type configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum IcebergCatalogType {
    Rest {
        uri: String,
        #[serde(default)]
        credential: Credential,
    },
    Glue {
        region: String,
        // No credential field - uses IAM/environment credentials
    },
}

/// Represents a data source connection with typed configuration.
/// The `type` field is used as the JSON discriminator via serde's tag attribute.
///
/// Credentials are stored as secrets and referenced via the `credential` field.
/// Use `credential().resolve(secrets)` to obtain the plaintext value.
/// The connection's `secret_id` column mirrors the credential ID for DB queryability.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Source {
    Postgres {
        host: String,
        port: u16,
        user: String,
        database: String,
        #[serde(default)]
        credential: Credential,
    },
    Snowflake {
        account: String,
        user: String,
        warehouse: String,
        database: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        schema: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        role: Option<String>,
        #[serde(default)]
        credential: Credential,
    },
    Motherduck {
        database: String,
        #[serde(default)]
        credential: Credential,
    },
    Duckdb {
        path: String,
        // No credential - local file access doesn't require authentication
    },
    Iceberg {
        catalog_type: IcebergCatalogType,
        /// Warehouse location (e.g., s3://bucket/path)
        warehouse: String,
        /// Optional namespace filter (e.g., "db.schema")
        #[serde(skip_serializing_if = "Option::is_none")]
        namespace: Option<String>,
    },
    Mysql {
        host: String,
        port: u16,
        user: String,
        database: String,
        #[serde(default)]
        credential: Credential,
    },
}

impl Source {
    /// Returns the source type as a string (e.g., "postgres", "snowflake", "motherduck", "duckdb", "iceberg", "mysql")
    pub fn source_type(&self) -> &'static str {
        match self {
            Source::Postgres { .. } => "postgres",
            Source::Snowflake { .. } => "snowflake",
            Source::Motherduck { .. } => "motherduck",
            Source::Duckdb { .. } => "duckdb",
            Source::Iceberg { .. } => "iceberg",
            Source::Mysql { .. } => "mysql",
        }
    }

    /// Returns the catalog name for this source, if applicable.
    /// For Motherduck, this is the database name used to filter table discovery.
    pub fn catalog(&self) -> Option<&str> {
        match self {
            Source::Motherduck { database, .. } => Some(database.as_str()),
            _ => None,
        }
    }

    /// Returns the credential for this source, if any.
    /// Returns None for sources that don't support credentials (DuckDB, Iceberg Glue).
    pub fn credential(&self) -> Option<&Credential> {
        match self {
            Source::Postgres { credential, .. } => Some(credential),
            Source::Snowflake { credential, .. } => Some(credential),
            Source::Motherduck { credential, .. } => Some(credential),
            Source::Duckdb { .. } => None,
            Source::Iceberg { catalog_type, .. } => match catalog_type {
                IcebergCatalogType::Rest { credential, .. } => Some(credential),
                IcebergCatalogType::Glue { .. } => None,
            },
            Source::Mysql { credential, .. } => Some(credential),
        }
    }

    /// Get the secret ID from this source's credential, if any.
    /// This is used to store the secret_id in the connections table for queryability.
    pub fn secret_id(&self) -> Option<&str> {
        self.credential().and_then(|c| c.secret_id())
    }

    /// Return a new Source with the given credential set.
    /// For sources that don't support credentials (DuckDB, Iceberg Glue), returns self unchanged.
    pub fn with_credential(self, credential: Credential) -> Self {
        match self {
            Source::Postgres {
                host,
                port,
                user,
                database,
                ..
            } => Source::Postgres {
                host,
                port,
                user,
                database,
                credential,
            },
            Source::Snowflake {
                account,
                user,
                warehouse,
                database,
                schema,
                role,
                ..
            } => Source::Snowflake {
                account,
                user,
                warehouse,
                database,
                schema,
                role,
                credential,
            },
            Source::Motherduck { database, .. } => Source::Motherduck {
                database,
                credential,
            },
            Source::Duckdb { path } => Source::Duckdb { path }, // No credential support
            Source::Iceberg {
                catalog_type,
                warehouse,
                namespace,
            } => {
                let catalog_type = match catalog_type {
                    IcebergCatalogType::Rest { uri, .. } => {
                        IcebergCatalogType::Rest { uri, credential }
                    }
                    glue @ IcebergCatalogType::Glue { .. } => glue, // No credential support - uses IAM
                };
                Source::Iceberg {
                    catalog_type,
                    warehouse,
                    namespace,
                }
            }
            Source::Mysql {
                host,
                port,
                user,
                database,
                ..
            } => Source::Mysql {
                host,
                port,
                user,
                database,
                credential,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_postgres_serialization() {
        let source = Source::Postgres {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            database: "mydb".to_string(),
            credential: Credential::SecretRef {
                id: "secr_abc123".to_string(),
            },
        };

        let json = serde_json::to_string(&source).unwrap();
        assert!(json.contains(r#""type":"postgres""#));
        assert!(json.contains(r#""host":"localhost""#));
        assert!(json.contains(r#""type":"secret_ref""#));
        assert!(json.contains(r#""id":"secr_abc123""#));

        let parsed: Source = serde_json::from_str(&json).unwrap();
        assert_eq!(source, parsed);
    }

    #[test]
    fn test_postgres_no_credential() {
        let source = Source::Postgres {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            database: "mydb".to_string(),
            credential: Credential::None,
        };

        let json = serde_json::to_string(&source).unwrap();
        assert!(json.contains(r#""type":"none""#));

        let parsed: Source = serde_json::from_str(&json).unwrap();
        assert_eq!(source, parsed);
    }

    #[test]
    fn test_snowflake_serialization() {
        let source = Source::Snowflake {
            account: "xyz123".to_string(),
            user: "bob".to_string(),
            warehouse: "COMPUTE_WH".to_string(),
            database: "PROD".to_string(),
            schema: Some("PUBLIC".to_string()),
            role: Some("ANALYST".to_string()),
            credential: Credential::SecretRef {
                id: "secr_xyz".to_string(),
            },
        };

        let json = serde_json::to_string(&source).unwrap();
        assert!(json.contains(r#""type":"snowflake""#));
        assert!(json.contains(r#""account":"xyz123""#));
        assert!(json.contains(r#""schema":"PUBLIC""#));

        let parsed: Source = serde_json::from_str(&json).unwrap();
        assert_eq!(source, parsed);
    }

    #[test]
    fn test_snowflake_without_role() {
        let source = Source::Snowflake {
            account: "xyz123".to_string(),
            user: "bob".to_string(),
            warehouse: "COMPUTE_WH".to_string(),
            database: "PROD".to_string(),
            schema: None,
            role: None,
            credential: Credential::None,
        };

        let json = serde_json::to_string(&source).unwrap();
        assert!(!json.contains(r#""role""#));
        assert!(!json.contains(r#""schema""#));
    }

    #[test]
    fn test_motherduck_serialization() {
        let source = Source::Motherduck {
            database: "my_db".to_string(),
            credential: Credential::SecretRef {
                id: "secr_token".to_string(),
            },
        };

        let json = serde_json::to_string(&source).unwrap();
        assert!(json.contains(r#""type":"motherduck""#));

        let parsed: Source = serde_json::from_str(&json).unwrap();
        assert_eq!(source, parsed);
    }

    #[test]
    fn test_duckdb_serialization() {
        let source = Source::Duckdb {
            path: "/path/to/db.duckdb".to_string(),
        };

        let json = serde_json::to_string(&source).unwrap();
        assert!(json.contains(r#""type":"duckdb""#));
        assert!(json.contains(r#""path":"/path/to/db.duckdb""#));
        assert!(!json.contains(r#""credential""#));

        let parsed: Source = serde_json::from_str(&json).unwrap();
        assert_eq!(source, parsed);
    }

    #[test]
    fn test_catalog_method() {
        let motherduck = Source::Motherduck {
            database: "my_database".to_string(),
            credential: Credential::None,
        };
        assert_eq!(motherduck.catalog(), Some("my_database"));

        let duckdb = Source::Duckdb {
            path: "/path/to/db".to_string(),
        };
        assert_eq!(duckdb.catalog(), None);

        let postgres = Source::Postgres {
            host: "localhost".to_string(),
            port: 5432,
            user: "u".to_string(),
            database: "d".to_string(),
            credential: Credential::None,
        };
        assert_eq!(postgres.catalog(), None);

        let snowflake = Source::Snowflake {
            account: "a".to_string(),
            user: "u".to_string(),
            warehouse: "w".to_string(),
            database: "d".to_string(),
            schema: None,
            role: None,
            credential: Credential::None,
        };
        assert_eq!(snowflake.catalog(), None);
    }

    #[test]
    fn test_source_type_method() {
        let postgres = Source::Postgres {
            host: "localhost".to_string(),
            port: 5432,
            user: "u".to_string(),
            database: "d".to_string(),
            credential: Credential::None,
        };
        assert_eq!(postgres.source_type(), "postgres");

        let snowflake = Source::Snowflake {
            account: "a".to_string(),
            user: "u".to_string(),
            warehouse: "w".to_string(),
            database: "d".to_string(),
            schema: None,
            role: None,
            credential: Credential::None,
        };
        assert_eq!(snowflake.source_type(), "snowflake");

        let motherduck = Source::Motherduck {
            database: "d".to_string(),
            credential: Credential::None,
        };
        assert_eq!(motherduck.source_type(), "motherduck");
    }

    #[test]
    fn test_credential_accessor() {
        let postgres = Source::Postgres {
            host: "h".to_string(),
            port: 5432,
            user: "u".to_string(),
            database: "d".to_string(),
            credential: Credential::SecretRef {
                id: "secr_test".to_string(),
            },
        };
        assert_eq!(
            postgres.credential(),
            Some(&Credential::SecretRef {
                id: "secr_test".to_string()
            })
        );
        assert_eq!(postgres.secret_id(), Some("secr_test"));

        let duckdb = Source::Duckdb {
            path: "/p".to_string(),
        };
        assert_eq!(duckdb.credential(), None);
        assert_eq!(duckdb.secret_id(), None);
    }

    #[test]
    fn test_iceberg_rest_serialization() {
        let source = Source::Iceberg {
            catalog_type: IcebergCatalogType::Rest {
                uri: "https://catalog.example.com".to_string(),
                credential: Credential::SecretRef {
                    id: "secr_bearer".to_string(),
                },
            },
            warehouse: "s3://my-bucket/warehouse".to_string(),
            namespace: Some("my_database".to_string()),
        };

        let json = serde_json::to_string(&source).unwrap();
        assert!(json.contains(r#""type":"iceberg""#));
        assert!(json.contains(r#""type":"rest""#));
        assert!(json.contains(r#""uri":"https://catalog.example.com""#));
        assert!(json.contains(r#""warehouse":"s3://my-bucket/warehouse""#));
        assert!(json.contains(r#""namespace":"my_database""#));

        let parsed: Source = serde_json::from_str(&json).unwrap();
        assert_eq!(source, parsed);
    }

    #[test]
    fn test_iceberg_glue_serialization() {
        let source = Source::Iceberg {
            catalog_type: IcebergCatalogType::Glue {
                region: "us-east-1".to_string(),
            },
            warehouse: "s3://data-lake/iceberg".to_string(),
            namespace: Some("analytics.events".to_string()),
        };

        let json = serde_json::to_string(&source).unwrap();
        assert!(json.contains(r#""type":"iceberg""#));
        assert!(json.contains(r#""type":"glue""#));
        assert!(json.contains(r#""region":"us-east-1""#));
        assert!(json.contains(r#""warehouse":"s3://data-lake/iceberg""#));
        assert!(json.contains(r#""namespace":"analytics.events""#));
        assert!(!json.contains(r#""credential""#)); // Glue has no credential field

        let parsed: Source = serde_json::from_str(&json).unwrap();
        assert_eq!(source, parsed);
    }

    #[test]
    fn test_iceberg_without_namespace() {
        let source = Source::Iceberg {
            catalog_type: IcebergCatalogType::Rest {
                uri: "http://localhost:8181".to_string(),
                credential: Credential::None,
            },
            warehouse: "s3://bucket/path".to_string(),
            namespace: None,
        };

        let json = serde_json::to_string(&source).unwrap();
        assert!(!json.contains(r#""namespace""#));

        let parsed: Source = serde_json::from_str(&json).unwrap();
        assert_eq!(source, parsed);
    }

    #[test]
    fn test_iceberg_source_type() {
        let iceberg = Source::Iceberg {
            catalog_type: IcebergCatalogType::Rest {
                uri: "http://localhost:8181".to_string(),
                credential: Credential::None,
            },
            warehouse: "s3://bucket/path".to_string(),
            namespace: None,
        };
        assert_eq!(iceberg.source_type(), "iceberg");
    }

    #[test]
    fn test_iceberg_catalog_returns_none() {
        let iceberg = Source::Iceberg {
            catalog_type: IcebergCatalogType::Glue {
                region: "us-west-2".to_string(),
            },
            warehouse: "s3://bucket/path".to_string(),
            namespace: Some("my_ns".to_string()),
        };
        // Iceberg doesn't use the catalog() method like Motherduck does
        assert_eq!(iceberg.catalog(), None);
    }

    #[test]
    fn test_iceberg_credential_accessor() {
        // REST catalog has credential
        let rest = Source::Iceberg {
            catalog_type: IcebergCatalogType::Rest {
                uri: "http://localhost".to_string(),
                credential: Credential::SecretRef {
                    id: "secr_rest".to_string(),
                },
            },
            warehouse: "s3://b/p".to_string(),
            namespace: None,
        };
        assert_eq!(rest.secret_id(), Some("secr_rest"));

        // Glue catalog has no credential (uses IAM)
        let glue = Source::Iceberg {
            catalog_type: IcebergCatalogType::Glue {
                region: "us-east-1".to_string(),
            },
            warehouse: "s3://b/p".to_string(),
            namespace: None,
        };
        assert_eq!(glue.credential(), None);
        assert_eq!(glue.secret_id(), None);
    }

    #[test]
    fn test_mysql_serialization() {
        let source = Source::Mysql {
            host: "localhost".to_string(),
            port: 3306,
            user: "myuser".to_string(),
            database: "mydb".to_string(),
            credential: Credential::SecretRef {
                id: "secr_mysql".to_string(),
            },
        };

        let json = serde_json::to_string(&source).unwrap();
        assert!(json.contains(r#""type":"mysql""#));
        assert!(json.contains(r#""host":"localhost""#));
        assert!(json.contains(r#""port":3306"#));
        assert!(json.contains(r#""user":"myuser""#));
        assert!(json.contains(r#""database":"mydb""#));

        let parsed: Source = serde_json::from_str(&json).unwrap();
        assert_eq!(source, parsed);
    }

    #[test]
    fn test_mysql_source_type() {
        let mysql = Source::Mysql {
            host: "localhost".to_string(),
            port: 3306,
            user: "u".to_string(),
            database: "d".to_string(),
            credential: Credential::None,
        };
        assert_eq!(mysql.source_type(), "mysql");
    }

    #[test]
    fn test_mysql_credential_accessor() {
        let mysql = Source::Mysql {
            host: "h".to_string(),
            port: 3306,
            user: "u".to_string(),
            database: "d".to_string(),
            credential: Credential::SecretRef {
                id: "secr_mysql".to_string(),
            },
        };
        assert_eq!(mysql.secret_id(), Some("secr_mysql"));
    }

    #[test]
    fn test_credential_secret_ref_helper() {
        let cred = Credential::secret_ref("secr_test123");
        assert_eq!(
            cred,
            Credential::SecretRef {
                id: "secr_test123".to_string()
            }
        );
        assert_eq!(cred.secret_id(), Some("secr_test123"));
    }

    #[test]
    fn test_credential_none_has_no_id() {
        let cred = Credential::None;
        assert_eq!(cred.secret_id(), None);
    }
}
