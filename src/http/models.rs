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

/// Single table metadata
#[derive(Debug, Serialize)]
pub struct TableInfo {
    pub connection: String,
    pub schema: String,
    pub table: String,
    pub synced: bool,
    pub last_sync: Option<String>,
}

/// Response body for GET /tables
#[derive(Debug, Serialize)]
pub struct TablesResponse {
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
    pub name: String,
    pub source_type: String,
    pub tables_discovered: usize,
    pub discovery_status: DiscoveryStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub discovery_error: Option<String>,
}

/// Response body for POST /connections/{name}/discover
#[derive(Debug, Serialize)]
pub struct DiscoverConnectionResponse {
    pub name: String,
    pub tables_discovered: usize,
    pub discovery_status: DiscoveryStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub discovery_error: Option<String>,
}

/// Single connection metadata for API responses
#[derive(Debug, Serialize)]
pub struct ConnectionInfo {
    pub id: i32,
    pub name: String,
    pub source_type: String,
}

/// Response body for GET /connections
#[derive(Debug, Serialize)]
pub struct ListConnectionsResponse {
    pub connections: Vec<ConnectionInfo>,
}

/// Response body for GET /connections/{name}
#[derive(Debug, Serialize)]
pub struct GetConnectionResponse {
    pub id: i32,
    pub name: String,
    pub source_type: String,
    pub table_count: usize,
    pub synced_table_count: usize,
}
