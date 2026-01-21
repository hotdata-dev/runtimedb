use serde::{Deserialize, Serialize};

/// Authentication type - describes how the secret is used.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AuthType {
    /// Username/password authentication (Postgres, MySQL, Snowflake)
    Password,
    /// Token-based authentication (Motherduck)
    Token,
    /// Bearer token authentication (Iceberg REST)
    BearerToken,
}

/// Iceberg catalog type configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum IcebergCatalogType {
    Rest {
        uri: String,
        auth: AuthType,
    },
    Glue {
        region: String,
        // No auth field - uses IAM/environment credentials
    },
}

/// Represents a data source connection with typed configuration.
/// The `type` field is used as the JSON discriminator via serde's tag attribute.
///
/// Credentials are stored separately via the `secret_id` column in the connections table,
/// which references a secret stored in the secrets table.
/// The `auth` field describes how the secret should be applied.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Source {
    Postgres {
        host: String,
        port: u16,
        user: String,
        database: String,
        auth: AuthType,
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
        auth: AuthType,
    },
    Motherduck {
        database: String,
        auth: AuthType,
    },
    Duckdb {
        path: String,
        // No auth field - local file access doesn't require authentication
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
        auth: AuthType,
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

    /// Returns the auth type for this source, if any.
    /// Returns None for sources that don't require authentication (DuckDB, Iceberg Glue).
    pub fn auth_type(&self) -> Option<&AuthType> {
        match self {
            Source::Postgres { auth, .. } => Some(auth),
            Source::Snowflake { auth, .. } => Some(auth),
            Source::Motherduck { auth, .. } => Some(auth),
            Source::Duckdb { .. } => None,
            Source::Iceberg { catalog_type, .. } => match catalog_type {
                IcebergCatalogType::Rest { auth, .. } => Some(auth),
                IcebergCatalogType::Glue { .. } => None,
            },
            Source::Mysql { auth, .. } => Some(auth),
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
            auth: AuthType::Password,
        };

        let json = serde_json::to_string(&source).unwrap();
        assert!(json.contains(r#""type":"postgres""#));
        assert!(json.contains(r#""host":"localhost""#));
        assert!(json.contains(r#""auth":"password""#));

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
            auth: AuthType::Password,
        };

        let json = serde_json::to_string(&source).unwrap();
        assert!(json.contains(r#""type":"snowflake""#));
        assert!(json.contains(r#""account":"xyz123""#));
        assert!(json.contains(r#""schema":"PUBLIC""#));
        assert!(json.contains(r#""auth":"password""#));

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
            auth: AuthType::Password,
        };

        let json = serde_json::to_string(&source).unwrap();
        assert!(!json.contains(r#""role""#));
        assert!(!json.contains(r#""schema""#));
    }

    #[test]
    fn test_motherduck_serialization() {
        let source = Source::Motherduck {
            database: "my_db".to_string(),
            auth: AuthType::Token,
        };

        let json = serde_json::to_string(&source).unwrap();
        assert!(json.contains(r#""type":"motherduck""#));
        assert!(json.contains(r#""auth":"token""#));

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
        assert!(!json.contains(r#""auth""#));

        let parsed: Source = serde_json::from_str(&json).unwrap();
        assert_eq!(source, parsed);
    }

    #[test]
    fn test_catalog_method() {
        let motherduck = Source::Motherduck {
            database: "my_database".to_string(),
            auth: AuthType::Token,
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
            auth: AuthType::Password,
        };
        assert_eq!(postgres.catalog(), None);

        let snowflake = Source::Snowflake {
            account: "a".to_string(),
            user: "u".to_string(),
            warehouse: "w".to_string(),
            database: "d".to_string(),
            schema: None,
            role: None,
            auth: AuthType::Password,
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
            auth: AuthType::Password,
        };
        assert_eq!(postgres.source_type(), "postgres");

        let snowflake = Source::Snowflake {
            account: "a".to_string(),
            user: "u".to_string(),
            warehouse: "w".to_string(),
            database: "d".to_string(),
            schema: None,
            role: None,
            auth: AuthType::Password,
        };
        assert_eq!(snowflake.source_type(), "snowflake");

        let motherduck = Source::Motherduck {
            database: "d".to_string(),
            auth: AuthType::Token,
        };
        assert_eq!(motherduck.source_type(), "motherduck");
    }

    #[test]
    fn test_auth_type_accessor() {
        let postgres = Source::Postgres {
            host: "h".to_string(),
            port: 5432,
            user: "u".to_string(),
            database: "d".to_string(),
            auth: AuthType::Password,
        };
        assert_eq!(postgres.auth_type(), Some(&AuthType::Password));

        let motherduck = Source::Motherduck {
            database: "d".to_string(),
            auth: AuthType::Token,
        };
        assert_eq!(motherduck.auth_type(), Some(&AuthType::Token));

        let duckdb = Source::Duckdb {
            path: "/p".to_string(),
        };
        assert_eq!(duckdb.auth_type(), None);
    }

    #[test]
    fn test_iceberg_rest_serialization() {
        let source = Source::Iceberg {
            catalog_type: IcebergCatalogType::Rest {
                uri: "https://catalog.example.com".to_string(),
                auth: AuthType::BearerToken,
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
        assert!(json.contains(r#""auth":"bearer_token""#));

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
        assert!(!json.contains(r#""auth""#)); // Glue has no auth field

        let parsed: Source = serde_json::from_str(&json).unwrap();
        assert_eq!(source, parsed);
    }

    #[test]
    fn test_iceberg_without_namespace() {
        let source = Source::Iceberg {
            catalog_type: IcebergCatalogType::Rest {
                uri: "http://localhost:8181".to_string(),
                auth: AuthType::BearerToken,
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
                auth: AuthType::BearerToken,
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
    fn test_iceberg_auth_type_accessor() {
        // REST catalog has auth
        let rest = Source::Iceberg {
            catalog_type: IcebergCatalogType::Rest {
                uri: "http://localhost".to_string(),
                auth: AuthType::BearerToken,
            },
            warehouse: "s3://b/p".to_string(),
            namespace: None,
        };
        assert_eq!(rest.auth_type(), Some(&AuthType::BearerToken));

        // Glue catalog has no auth (uses IAM)
        let glue = Source::Iceberg {
            catalog_type: IcebergCatalogType::Glue {
                region: "us-east-1".to_string(),
            },
            warehouse: "s3://b/p".to_string(),
            namespace: None,
        };
        assert_eq!(glue.auth_type(), None);
    }

    #[test]
    fn test_mysql_serialization() {
        let source = Source::Mysql {
            host: "localhost".to_string(),
            port: 3306,
            user: "myuser".to_string(),
            database: "mydb".to_string(),
            auth: AuthType::Password,
        };

        let json = serde_json::to_string(&source).unwrap();
        assert!(json.contains(r#""type":"mysql""#));
        assert!(json.contains(r#""host":"localhost""#));
        assert!(json.contains(r#""port":3306"#));
        assert!(json.contains(r#""user":"myuser""#));
        assert!(json.contains(r#""database":"mydb""#));
        assert!(json.contains(r#""auth":"password""#));

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
            auth: AuthType::Password,
        };
        assert_eq!(mysql.source_type(), "mysql");
    }

    #[test]
    fn test_mysql_auth_type_accessor() {
        let mysql = Source::Mysql {
            host: "h".to_string(),
            port: 3306,
            user: "u".to_string(),
            database: "d".to_string(),
            auth: AuthType::Password,
        };
        assert_eq!(mysql.auth_type(), Some(&AuthType::Password));
    }

    #[test]
    fn test_auth_type_serialization() {
        assert_eq!(
            serde_json::to_string(&AuthType::Password).unwrap(),
            r#""password""#
        );
        assert_eq!(
            serde_json::to_string(&AuthType::Token).unwrap(),
            r#""token""#
        );
        assert_eq!(
            serde_json::to_string(&AuthType::BearerToken).unwrap(),
            r#""bearer_token""#
        );
    }
}
