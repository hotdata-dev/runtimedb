use crate::secrets::SecretMetadata;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Request body for POST /query
#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    pub sql: String,
}

/// Response body for POST /query
#[derive(Debug, Serialize)]
pub struct QueryResponse {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub row_count: usize,
    pub execution_time_ms: u64,
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

/// Response body for POST /connections/{connection_id}/discover
#[derive(Debug, Serialize)]
pub struct DiscoverConnectionResponse {
    pub id: String,
    pub name: String,
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
}

/// Response for schema refresh operations
#[derive(Debug, Serialize)]
pub struct SchemaRefreshResult {
    pub connections_refreshed: usize,
    pub tables_discovered: usize,
    pub tables_added: usize,
    pub tables_removed: usize,
    pub tables_modified: usize,
}

/// Response for single table data refresh
#[derive(Debug, Serialize)]
pub struct TableRefreshResult {
    pub connection_id: i32,
    pub schema_name: String,
    pub table_name: String,
    pub rows_synced: usize,
    pub duration_ms: u64,
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
    pub connection_id: i32,
    pub tables_refreshed: usize,
    pub tables_failed: usize,
    pub total_rows: usize,
    pub duration_ms: u64,
    pub errors: Vec<TableRefreshError>,
}

/// Unified response type for refresh operations
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum RefreshResponse {
    Schema(SchemaRefreshResult),
    Table(TableRefreshResult),
    Connection(ConnectionRefreshResult),
}
