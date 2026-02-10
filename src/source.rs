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
    #[tracing::instrument(
        name = "resolve_credential",
        skip(self, secrets),
        fields(
            runtimedb.has_credential = !matches!(self, Credential::None),
        )
    )]
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
    Bigquery {
        project_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        dataset: Option<String>,
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
            Source::Bigquery { .. } => "bigquery",
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
            Source::Mysql { credential, .. } => credential,
            Source::Bigquery { credential, .. } => credential,
        }
    }

    /// Get the secret ID from this source's credential, if any.
    /// This is used to store the secret_id in the connections table for queryability.
    pub fn secret_id(&self) -> Option<&str> {
        self.credential().secret_id()
    }

    /// Return a new Source with the given credential set.
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
            Source::Duckdb { path } => Source::Duckdb { path },
            Source::Iceberg {
                catalog_type,
                warehouse,
                namespace,
            } => {
                let catalog_type = match catalog_type {
                    IcebergCatalogType::Rest { uri, .. } => {
                        IcebergCatalogType::Rest { uri, credential }
                    }
                    IcebergCatalogType::Glue { region, .. } => {
                        IcebergCatalogType::Glue { region, credential }
                    }
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
            Source::Bigquery {
                project_id,
                dataset,
                ..
            } => Source::Bigquery {
                project_id,
                dataset,
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
        let with_secret = Source::Postgres {
            host: "h".to_string(),
            port: 5432,
            user: "u".to_string(),
            database: "d".to_string(),
            credential: Credential::SecretRef {
                id: "secr_test".to_string(),
            },
        };
        assert!(matches!(
            with_secret.credential(),
            Credential::SecretRef { id } if id == "secr_test"
        ));
        assert_eq!(with_secret.secret_id(), Some("secr_test"));

        let duckdb = Source::Duckdb {
            path: "/p".to_string(),
        };
        assert!(matches!(duckdb.credential(), Credential::None));
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
                credential: Credential::SecretRef {
                    id: "secr_aws".to_string(),
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
                    id: "secr_rest".to_string(),
                },
            },
            warehouse: "s3://b/p".to_string(),
            namespace: None,
        };
        assert!(matches!(
            rest_with_cred.credential(),
            Credential::SecretRef { id } if id == "secr_rest"
        ));
        assert_eq!(rest_with_cred.secret_id(), Some("secr_rest"));

        // Glue catalog with credential
        let glue_with_cred = Source::Iceberg {
            catalog_type: IcebergCatalogType::Glue {
                region: "us-east-1".to_string(),
                credential: Credential::SecretRef {
                    id: "secr_aws".to_string(),
                },
            },
            warehouse: "s3://b/p".to_string(),
            namespace: None,
        };
        assert!(matches!(
            glue_with_cred.credential(),
            Credential::SecretRef { id } if id == "secr_aws"
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
        let with_secret = Source::Mysql {
            host: "h".to_string(),
            port: 3306,
            user: "u".to_string(),
            database: "d".to_string(),
            credential: Credential::SecretRef {
                id: "secr_mysql".to_string(),
            },
        };
        assert!(matches!(
            with_secret.credential(),
            Credential::SecretRef { id } if id == "secr_mysql"
        ));
        assert_eq!(with_secret.secret_id(), Some("secr_mysql"));

        let without_cred = Source::Mysql {
            host: "h".to_string(),
            port: 3306,
            user: "u".to_string(),
            database: "d".to_string(),
            credential: Credential::None,
        };
        assert!(matches!(without_cred.credential(), Credential::None));
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

    #[test]
    fn test_bigquery_serialization() {
        let source = Source::Bigquery {
            project_id: "my-gcp-project".to_string(),
            dataset: Some("my_dataset".to_string()),
            credential: Credential::SecretRef {
                id: "secr_bq".to_string(),
            },
        };

        let json = serde_json::to_string(&source).unwrap();
        assert!(json.contains(r#""type":"bigquery""#));
        assert!(json.contains(r#""project_id":"my-gcp-project""#));
        assert!(json.contains(r#""dataset":"my_dataset""#));

        let parsed: Source = serde_json::from_str(&json).unwrap();
        assert_eq!(source, parsed);
    }

    #[test]
    fn test_bigquery_without_dataset() {
        let source = Source::Bigquery {
            project_id: "my-gcp-project".to_string(),
            dataset: None,
            credential: Credential::None,
        };

        let json = serde_json::to_string(&source).unwrap();
        assert!(!json.contains(r#""dataset""#));

        let parsed: Source = serde_json::from_str(&json).unwrap();
        assert_eq!(source, parsed);
    }

    #[test]
    fn test_bigquery_source_type() {
        let bq = Source::Bigquery {
            project_id: "proj".to_string(),
            dataset: None,
            credential: Credential::None,
        };
        assert_eq!(bq.source_type(), "bigquery");
    }

    #[test]
    fn test_bigquery_credential_accessor() {
        let with_secret = Source::Bigquery {
            project_id: "proj".to_string(),
            dataset: None,
            credential: Credential::SecretRef {
                id: "secr_bq".to_string(),
            },
        };
        assert!(matches!(
            with_secret.credential(),
            Credential::SecretRef { id } if id == "secr_bq"
        ));
        assert_eq!(with_secret.secret_id(), Some("secr_bq"));

        let without_cred = Source::Bigquery {
            project_id: "proj".to_string(),
            dataset: None,
            credential: Credential::None,
        };
        assert!(matches!(without_cred.credential(), Credential::None));
    }
}
