use crate::secrets::SecretMetadata;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Request body for POST /query
#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    pub sql: String,
}

/// Response body for POST /query and GET /results/{id}
#[derive(Debug, Serialize)]
pub struct QueryResponse {
    /// Unique identifier for retrieving this result via GET /results/{id}.
    /// Null if persistence failed (see `warning` field for details).
    pub result_id: Option<String>,
    pub columns: Vec<String>,
    /// Nullable flags for each column (parallel to columns vec).
    /// True if the column allows NULL values, false if NOT NULL.
    pub nullable: Vec<bool>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub row_count: usize,
    pub execution_time_ms: u64,
    /// Warning message if result persistence failed.
    /// When present, `result_id` will be null and the result cannot be retrieved later.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warning: Option<String>,
}

/// Response body for GET /results/{id}
/// Returns status and optionally the result data
#[derive(Debug, Serialize)]
pub struct GetResultResponse {
    pub result_id: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nullable: Option<Vec<bool>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows: Option<Vec<Vec<serde_json::Value>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_time_ms: Option<u64>,
}

/// Summary of a persisted query result for listing
#[derive(Debug, Serialize)]
pub struct ResultInfo {
    pub id: String,
    pub created_at: DateTime<Utc>,
}

/// Response body for GET /results
#[derive(Debug, Serialize)]
pub struct ListResultsResponse {
    pub results: Vec<ResultInfo>,
    /// Number of results returned in this response
    pub count: usize,
    /// Pagination offset used for this request
    pub offset: usize,
    /// Limit used for this request
    pub limit: usize,
    /// Whether there are more results available after this page
    pub has_more: bool,
}

/// Column metadata for API responses
#[derive(Debug, Serialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

/// Single table metadata
#[derive(Debug, Serialize)]
pub struct TableInfo {
    pub connection: String,
    pub schema: String,
    pub table: String,
    pub synced: bool,
    pub last_sync: Option<String>,
    pub columns: Vec<ColumnInfo>,
}

/// Response body for GET /information_schema
#[derive(Debug, Serialize)]
pub struct InformationSchemaResponse {
    pub tables: Vec<TableInfo>,
}

/// Request body for POST /connections
#[derive(Debug, Deserialize)]
pub struct CreateConnectionRequest {
    pub name: String,
    pub source_type: String,
    pub config: serde_json::Value,
    /// Optional reference to a secret by name.
    /// If provided, this secret will be used for authentication.
    /// Mutually exclusive with `secret_id`.
    #[serde(default)]
    pub secret_name: Option<String>,
    /// Optional reference to a secret by ID (e.g., "secr_abc123").
    /// If provided, this secret will be used for authentication.
    /// Mutually exclusive with `secret_name`.
    #[serde(default)]
    pub secret_id: Option<String>,
}

/// Discovery status for connection creation
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryStatus {
    /// Discovery succeeded
    Success,
    /// Discovery was skipped (e.g., skip_discovery=true)
    Skipped,
    /// Discovery failed (connection still registered)
    Failed,
}

/// Response body for POST /connections
#[derive(Debug, Serialize)]
pub struct CreateConnectionResponse {
    pub id: String,
    pub name: String,
    pub source_type: String,
    pub tables_discovered: usize,
    pub discovery_status: DiscoveryStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub discovery_error: Option<String>,
}

/// Single connection metadata for API responses
#[derive(Debug, Serialize)]
pub struct ConnectionInfo {
    pub id: String,
    pub name: String,
    pub source_type: String,
}

/// Response body for GET /connections
#[derive(Debug, Serialize)]
pub struct ListConnectionsResponse {
    pub connections: Vec<ConnectionInfo>,
}

/// Response body for GET /connections/{connection_id}
#[derive(Debug, Serialize)]
pub struct GetConnectionResponse {
    pub id: String,
    pub name: String,
    pub source_type: String,
    pub table_count: usize,
    pub synced_table_count: usize,
}

// Secret management models

/// Request body for POST /secrets
#[derive(Debug, Deserialize)]
pub struct CreateSecretRequest {
    pub name: String,
    pub value: String,
}

/// Request body for PUT /secrets/{name}
#[derive(Debug, Deserialize)]
pub struct UpdateSecretRequest {
    pub value: String,
}

/// Response body for POST /secrets
#[derive(Debug, Serialize)]
pub struct CreateSecretResponse {
    pub id: String,
    pub name: String,
    pub created_at: DateTime<Utc>,
}

/// Response body for PUT /secrets/{name}
#[derive(Debug, Serialize)]
pub struct UpdateSecretResponse {
    pub name: String,
    pub updated_at: DateTime<Utc>,
}

/// Response body for GET /secrets
#[derive(Debug, Serialize)]
pub struct ListSecretsResponse {
    pub secrets: Vec<SecretMetadataResponse>,
}

/// Single secret metadata for API responses
#[derive(Debug, Serialize)]
pub struct SecretMetadataResponse {
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<SecretMetadata> for SecretMetadataResponse {
    fn from(m: SecretMetadata) -> Self {
        Self {
            name: m.name,
            created_at: m.created_at,
            updated_at: m.updated_at,
        }
    }
}

/// Response body for GET /secrets/{name}
#[derive(Debug, Serialize)]
pub struct GetSecretResponse {
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// Refresh endpoint models

/// Request body for POST /refresh
#[derive(Debug, Deserialize)]
pub struct RefreshRequest {
    pub connection_id: Option<String>,
    pub schema_name: Option<String>,
    pub table_name: Option<String>,
    #[serde(default)]
    pub data: bool,
    /// Controls whether uncached tables are included in connection-wide data refresh.
    ///
    /// - `false` (default): Only refresh tables that already have cached parquet files.
    ///   This is the common case for keeping existing data up-to-date.
    /// - `true`: Also sync tables that haven't been cached yet, essentially performing
    ///   an initial sync for any new tables discovered since the connection was created.
    ///
    /// This field only applies to connection-wide data refresh (when `data=true` and
    /// `table_name` is not specified). It has no effect on single-table refresh or
    /// schema refresh operations.
    #[serde(default)]
    pub include_uncached: bool,
}

/// Error details for a failed connection schema refresh
#[derive(Debug, Serialize)]
pub struct ConnectionSchemaError {
    pub connection_id: String,
    pub error: String,
}

/// Response for schema refresh operations
#[derive(Debug, Serialize)]
pub struct SchemaRefreshResult {
    pub connections_refreshed: usize,
    pub connections_failed: usize,
    pub tables_discovered: usize,
    pub tables_added: usize,
    pub tables_modified: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<ConnectionSchemaError>,
}

/// Non-fatal warning that occurred during a refresh operation.
/// Used to report issues like failed deletion scheduling that don't
/// prevent the refresh from succeeding.
#[derive(Debug, Clone, Serialize)]
pub struct RefreshWarning {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_name: Option<String>,
    pub message: String,
}

/// Response for single table data refresh
#[derive(Debug, Serialize)]
pub struct TableRefreshResult {
    pub connection_id: String,
    pub schema_name: String,
    pub table_name: String,
    pub rows_synced: usize,
    pub duration_ms: u64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<RefreshWarning>,
}

/// Error details for a failed table refresh
#[derive(Debug, Serialize)]
pub struct TableRefreshError {
    pub schema_name: String,
    pub table_name: String,
    pub error: String,
}

/// Response for connection-wide data refresh
#[derive(Debug, Serialize)]
pub struct ConnectionRefreshResult {
    pub connection_id: String,
    pub tables_refreshed: usize,
    pub tables_failed: usize,
    pub total_rows: usize,
    pub duration_ms: u64,
    pub errors: Vec<TableRefreshError>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<RefreshWarning>,
}

/// Unified response type for refresh operations
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum RefreshResponse {
    Schema(SchemaRefreshResult),
    Table(TableRefreshResult),
    Connection(ConnectionRefreshResult),
}

// Upload models

/// Response body for POST /v1/files
#[derive(Debug, Serialize)]
pub struct UploadResponse {
    pub id: String,
    pub status: String,
    pub size_bytes: i64,
    pub content_type: Option<String>,
    pub created_at: DateTime<Utc>,
}

/// Response body for GET /v1/files
#[derive(Debug, Serialize)]
pub struct ListUploadsResponse {
    pub uploads: Vec<UploadInfo>,
}

/// Single upload info for listing
#[derive(Debug, Serialize)]
pub struct UploadInfo {
    pub id: String,
    pub status: String,
    pub size_bytes: i64,
    pub content_type: Option<String>,
    pub created_at: DateTime<Utc>,
}

// Dataset models

/// Column type specification - either a simple type string or detailed spec with properties.
///
/// Simple form: `"VARCHAR"`, `"INT"`, `"DATE"`
/// Detailed form: `{ "type": "DECIMAL", "precision": 10, "scale": 2 }`
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum ColumnDefinition {
    /// Simple type name (e.g., "VARCHAR", "INT", "DATE")
    Simple(String),
    /// Detailed specification with type and optional properties
    Detailed(ColumnTypeSpec),
}

/// Detailed column type specification with optional properties.
#[derive(Debug, Clone, Deserialize)]
pub struct ColumnTypeSpec {
    /// The data type name (e.g., "DECIMAL", "TIMESTAMP")
    #[serde(rename = "type")]
    pub data_type: String,
    /// Precision for DECIMAL type (1-38)
    #[serde(default)]
    pub precision: Option<u8>,
    /// Scale for DECIMAL type
    #[serde(default)]
    pub scale: Option<i8>,
    // Future: format, srid, geometry_type, timezone, etc.
}

/// Request body for POST /v1/datasets
#[derive(Debug, Deserialize)]
pub struct CreateDatasetRequest {
    pub label: String,
    /// Optional table_name - if not provided, derived from label
    #[serde(default)]
    pub table_name: Option<String>,
    pub source: DatasetSource,
}

/// Dataset source specification
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum DatasetSource {
    /// Create from a previously uploaded file
    Upload {
        upload_id: String,
        #[serde(default)]
        format: Option<String>,
        /// Optional explicit column definitions. Keys are column names, values are type specs.
        /// When provided, the schema is built from these definitions instead of being inferred.
        #[serde(default)]
        columns: Option<std::collections::HashMap<String, ColumnDefinition>>,
    },
    /// Create from inline data (small payloads)
    Inline { inline: InlineData },
}

/// Inline data specification
#[derive(Debug, Deserialize)]
pub struct InlineData {
    pub format: String,
    pub content: String,
    /// Optional explicit column definitions. Keys are column names, values are type specs.
    /// When provided, the schema is built from these definitions instead of being inferred.
    #[serde(default)]
    pub columns: Option<std::collections::HashMap<String, ColumnDefinition>>,
}

/// Response body for POST /v1/datasets
#[derive(Debug, Serialize)]
pub struct CreateDatasetResponse {
    pub id: String,
    pub label: String,
    pub table_name: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
}

/// Response body for GET /v1/datasets
#[derive(Debug, Serialize)]
pub struct ListDatasetsResponse {
    pub datasets: Vec<DatasetSummary>,
    /// Number of datasets returned in this response
    pub count: usize,
    /// Pagination offset used for this request
    pub offset: usize,
    /// Limit used for this request
    pub limit: usize,
    /// Whether there are more datasets available after this page
    pub has_more: bool,
}

/// Dataset summary for listing
#[derive(Debug, Serialize)]
pub struct DatasetSummary {
    pub id: String,
    pub label: String,
    pub table_name: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Response body for GET /v1/datasets/{id}
#[derive(Debug, Serialize)]
pub struct GetDatasetResponse {
    pub id: String,
    pub label: String,
    pub schema_name: String,
    pub table_name: String,
    pub source_type: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub columns: Vec<ColumnInfo>,
}

/// Request body for PUT /v1/datasets/{id}
#[derive(Debug, Deserialize)]
pub struct UpdateDatasetRequest {
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub table_name: Option<String>,
}

/// Response body for PUT /v1/datasets/{id}
#[derive(Debug, Serialize)]
pub struct UpdateDatasetResponse {
    pub id: String,
    pub label: String,
    pub table_name: String,
    pub updated_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_refresh_result_omits_empty_warnings() {
        let result = TableRefreshResult {
            connection_id: "test".to_string(),
            schema_name: "public".to_string(),
            table_name: "users".to_string(),
            rows_synced: 100,
            duration_ms: 50,
            warnings: vec![],
        };

        let json = serde_json::to_value(&result).unwrap();

        assert!(
            json.get("warnings").is_none(),
            "empty warnings should be omitted"
        );
        assert_eq!(json["rows_synced"], 100);
    }

    #[test]
    fn test_table_refresh_result_includes_warnings_when_present() {
        let result = TableRefreshResult {
            connection_id: "test".to_string(),
            schema_name: "public".to_string(),
            table_name: "users".to_string(),
            rows_synced: 100,
            duration_ms: 50,
            warnings: vec![RefreshWarning {
                schema_name: Some("public".to_string()),
                table_name: Some("users".to_string()),
                message: "Failed to schedule deletion".to_string(),
            }],
        };

        let json = serde_json::to_value(&result).unwrap();

        assert!(json.get("warnings").is_some(), "warnings should be present");
        let warnings = json["warnings"].as_array().unwrap();
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0]["message"], "Failed to schedule deletion");
        assert_eq!(warnings[0]["schema_name"], "public");
        assert_eq!(warnings[0]["table_name"], "users");
    }

    #[test]
    fn test_connection_refresh_result_omits_empty_warnings() {
        let result = ConnectionRefreshResult {
            connection_id: "test".to_string(),
            tables_refreshed: 5,
            tables_failed: 0,
            total_rows: 500,
            duration_ms: 100,
            errors: vec![],
            warnings: vec![],
        };

        let json = serde_json::to_value(&result).unwrap();

        assert!(
            json.get("warnings").is_none(),
            "empty warnings should be omitted"
        );
        assert_eq!(json["tables_refreshed"], 5);
    }

    #[test]
    fn test_connection_refresh_result_includes_warnings_when_present() {
        let result = ConnectionRefreshResult {
            connection_id: "test".to_string(),
            tables_refreshed: 5,
            tables_failed: 0,
            total_rows: 500,
            duration_ms: 100,
            errors: vec![],
            warnings: vec![
                RefreshWarning {
                    schema_name: Some("public".to_string()),
                    table_name: Some("orders".to_string()),
                    message: "Failed to schedule deletion of old cache".to_string(),
                },
                RefreshWarning {
                    schema_name: None,
                    table_name: None,
                    message: "General warning".to_string(),
                },
            ],
        };

        let json = serde_json::to_value(&result).unwrap();

        assert!(json.get("warnings").is_some(), "warnings should be present");
        let warnings = json["warnings"].as_array().unwrap();
        assert_eq!(warnings.len(), 2);

        // First warning has schema and table
        assert_eq!(warnings[0]["schema_name"], "public");
        assert_eq!(warnings[0]["table_name"], "orders");

        // Second warning omits null schema/table fields
        assert!(warnings[1].get("schema_name").is_none());
        assert!(warnings[1].get("table_name").is_none());
        assert_eq!(warnings[1]["message"], "General warning");
    }

    #[test]
    fn test_refresh_warning_omits_none_fields() {
        let warning = RefreshWarning {
            schema_name: None,
            table_name: None,
            message: "Test warning".to_string(),
        };

        let json = serde_json::to_value(&warning).unwrap();

        assert!(
            json.get("schema_name").is_none(),
            "None schema_name should be omitted"
        );
        assert!(
            json.get("table_name").is_none(),
            "None table_name should be omitted"
        );
        assert_eq!(json["message"], "Test warning");
    }
}
