use crate::secrets::SecretManager;
use serde::{Deserialize, Serialize};

/// AWS credentials for services like Glue, S3, etc.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AwsCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_token: Option<String>,
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
        #[serde(default)]
        credential: Credential,
    },
}

/// Credential storage - either no credential or a reference to a stored secret.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Credential {
    #[default]
    None,
    SecretRef {
        name: String,
    },
}

impl Credential {
    /// Resolve the credential to a plaintext string.
    /// Returns an error if the credential is None or the secret cannot be found/decoded.
    pub async fn resolve(&self, secrets: &SecretManager) -> anyhow::Result<String> {
        match self {
            Credential::None => Err(anyhow::anyhow!("no credential configured")),
            Credential::SecretRef { name } => {
                let bytes = secrets
                    .get(name)
                    .await
                    .map_err(|e| anyhow::anyhow!("failed to resolve secret '{}': {}", name, e))?;
                String::from_utf8(bytes)
                    .map_err(|_| anyhow::anyhow!("secret '{}' is not valid UTF-8", name))
            }
        }
    }
}

/// Represents a data source connection with typed configuration.
/// The `type` field is used as the JSON discriminator via serde's tag attribute.
///
/// Credentials are stored as secrets and referenced via the `credential` field.
/// Use `credential().resolve(secrets)` to obtain the plaintext value.
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
    },
    Iceberg {
        catalog_type: IcebergCatalogType,
        /// Warehouse location (e.g., s3://bucket/path)
        warehouse: String,
        /// Optional namespace filter (e.g., "db.schema")
        #[serde(skip_serializing_if = "Option::is_none")]
        namespace: Option<String>,
    },
}

impl Source {
    /// Returns the source type as a string (e.g., "postgres", "snowflake", "motherduck", "duckdb", "iceberg")
    pub fn source_type(&self) -> &'static str {
        match self {
            Source::Postgres { .. } => "postgres",
            Source::Snowflake { .. } => "snowflake",
            Source::Motherduck { .. } => "motherduck",
            Source::Duckdb { .. } => "duckdb",
            Source::Iceberg { .. } => "iceberg",
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

    /// Access the credential field.
    pub fn credential(&self) -> &Credential {
        match self {
            Source::Postgres { credential, .. } => credential,
            Source::Snowflake { credential, .. } => credential,
            Source::Motherduck { credential, .. } => credential,
            Source::Duckdb { .. } => &Credential::None,
            Source::Iceberg { catalog_type, .. } => match catalog_type {
                IcebergCatalogType::Rest { credential, .. } => credential,
                IcebergCatalogType::Glue { credential, .. } => credential,
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
                name: "my-pg-secret".to_string(),
            },
        };

        let json = serde_json::to_string(&source).unwrap();
        assert!(json.contains(r#""type":"postgres""#));
        assert!(json.contains(r#""host":"localhost""#));
        assert!(json.contains(r#""my-pg-secret""#));

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
            role: Some("ANALYST".to_string()),
            credential: Credential::SecretRef {
                name: "snowflake-secret".to_string(),
            },
        };

        let json = serde_json::to_string(&source).unwrap();
        assert!(json.contains(r#""type":"snowflake""#));
        assert!(json.contains(r#""account":"xyz123""#));

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
            role: None,
            credential: Credential::SecretRef {
                name: "secret".to_string(),
            },
        };

        let json = serde_json::to_string(&source).unwrap();
        assert!(!json.contains(r#""role""#));
    }

    #[test]
    fn test_motherduck_serialization() {
        let source = Source::Motherduck {
            database: "my_db".to_string(),
            credential: Credential::SecretRef {
                name: "md-token".to_string(),
            },
        };

        let json = serde_json::to_string(&source).unwrap();
        assert!(json.contains(r#""type":"motherduck""#));
        assert!(json.contains(r#""md-token""#));

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
        let with_secret = Source::Postgres {
            host: "h".to_string(),
            port: 5432,
            user: "u".to_string(),
            database: "d".to_string(),
            credential: Credential::SecretRef {
                name: "my-secret".to_string(),
            },
        };
        assert!(matches!(
            with_secret.credential(),
            Credential::SecretRef { name } if name == "my-secret"
        ));

        let duckdb = Source::Duckdb {
            path: "/p".to_string(),
        };
        assert!(matches!(duckdb.credential(), Credential::None));
    }

    #[test]
    fn test_iceberg_rest_serialization() {
        let source = Source::Iceberg {
            catalog_type: IcebergCatalogType::Rest {
                uri: "https://catalog.example.com".to_string(),
                credential: Credential::SecretRef {
                    name: "iceberg-token".to_string(),
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
        assert!(json.contains(r#""iceberg-token""#));

        let parsed: Source = serde_json::from_str(&json).unwrap();
        assert_eq!(source, parsed);
    }

    #[test]
    fn test_iceberg_glue_serialization() {
        let source = Source::Iceberg {
            catalog_type: IcebergCatalogType::Glue {
                region: "us-east-1".to_string(),
                credential: Credential::SecretRef {
                    name: "aws-creds".to_string(),
                },
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
                credential: Credential::None,
            },
            warehouse: "s3://bucket/path".to_string(),
            namespace: Some("my_ns".to_string()),
        };
        // Iceberg doesn't use the catalog() method like Motherduck does
        assert_eq!(iceberg.catalog(), None);
    }

    #[test]
    fn test_iceberg_credential_accessor() {
        // REST catalog with credential
        let rest_with_cred = Source::Iceberg {
            catalog_type: IcebergCatalogType::Rest {
                uri: "http://localhost".to_string(),
                credential: Credential::SecretRef {
                    name: "rest-token".to_string(),
                },
            },
            warehouse: "s3://b/p".to_string(),
            namespace: None,
        };
        assert!(matches!(
            rest_with_cred.credential(),
            Credential::SecretRef { name } if name == "rest-token"
        ));

        // Glue catalog with credential
        let glue_with_cred = Source::Iceberg {
            catalog_type: IcebergCatalogType::Glue {
                region: "us-east-1".to_string(),
                credential: Credential::SecretRef {
                    name: "aws-secret".to_string(),
                },
            },
            warehouse: "s3://b/p".to_string(),
            namespace: None,
        };
        assert!(matches!(
            glue_with_cred.credential(),
            Credential::SecretRef { name } if name == "aws-secret"
        ));

        // REST catalog without credential
        let rest_no_cred = Source::Iceberg {
            catalog_type: IcebergCatalogType::Rest {
                uri: "http://localhost".to_string(),
                credential: Credential::None,
            },
            warehouse: "s3://b/p".to_string(),
            namespace: None,
        };
        assert!(matches!(rest_no_cred.credential(), Credential::None));
    }

    #[test]
    fn test_aws_credentials_serialization() {
        let creds = AwsCredentials {
            access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            session_token: Some("FwoGZXIvYXdzEB...".to_string()),
        };

        let json = serde_json::to_string(&creds).unwrap();
        assert!(json.contains(r#""access_key_id":"AKIAIOSFODNN7EXAMPLE""#));
        assert!(json.contains(r#""secret_access_key""#));
        assert!(json.contains(r#""session_token""#));

        let parsed: AwsCredentials = serde_json::from_str(&json).unwrap();
        assert_eq!(creds, parsed);
    }

    #[test]
    fn test_aws_credentials_without_session_token() {
        let creds = AwsCredentials {
            access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            session_token: None,
        };

        let json = serde_json::to_string(&creds).unwrap();
        assert!(!json.contains(r#""session_token""#));

        let parsed: AwsCredentials = serde_json::from_str(&json).unwrap();
        assert_eq!(creds, parsed);
    }
}
