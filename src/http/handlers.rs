use crate::datafusion::HotDataEngine;
use crate::http::error::ApiError;
use crate::http::models::{
    ConnectionInfo, CreateConnectionRequest, CreateConnectionResponse, GetConnectionResponse,
    ListConnectionsResponse, QueryRequest, QueryResponse, TableInfo, TablesResponse,
};
use crate::http::serialization::{encode_value_at, make_array_encoder};
use crate::source::Source;
use axum::{
    extract::{Path, Query as QueryParams, State},
    http::StatusCode,
    Json,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::error;

/// Handler for POST /query
pub async fn query_handler(
    State(engine): State<Arc<HotDataEngine>>,
    Json(request): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, ApiError> {
    // Validate SQL is not empty
    if request.sql.trim().is_empty() {
        return Err(ApiError::bad_request("SQL query cannot be empty"));
    }

    // Execute query
    let start = Instant::now();
    let result = engine.execute_query(&request.sql).await?;
    let execution_time_ms = start.elapsed().as_millis() as u64;

    // Convert arrow batches to JSON
    let batches = &result.results;

    // Get column names from schema
    let columns: Vec<String> = if let Some(batch) = batches.first() {
        batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    } else {
        Vec::new()
    };

    // Convert rows to JSON values
    let mut rows: Vec<Vec<serde_json::Value>> = Vec::new();

    for batch in batches {
        // Create encoders once per batch (one per column)
        let schema = batch.schema();
        let mut encoders: Vec<_> = batch
            .columns()
            .iter()
            .zip(schema.fields())
            .map(|(col, field)| make_array_encoder(col.as_ref(), field))
            .collect::<Result<_, _>>()
            .map_err(|e| ApiError::internal_error(format!("Failed to create encoder: {}", e)))?;

        // Process all rows using the pre-created encoders
        for row_idx in 0..batch.num_rows() {
            let row_values: Vec<serde_json::Value> = encoders
                .iter_mut()
                .map(|encoder| encode_value_at(encoder, row_idx))
                .collect();
            rows.push(row_values);
        }
    }

    let row_count = rows.len();

    Ok(Json(QueryResponse {
        columns,
        rows,
        row_count,
        execution_time_ms,
    }))
}

/// Handler for GET /tables
pub async fn tables_handler(
    State(engine): State<Arc<HotDataEngine>>,
    QueryParams(params): QueryParams<HashMap<String, String>>,
) -> Result<Json<TablesResponse>, ApiError> {
    // Get optional connection filter
    let connection_filter = params.get("connection").map(|s| s.as_str());

    // Validate connection exists if filter provided
    // todo: add exists method to engine/catalog;
    // todo: consider accepting connection_id instead?
    if let Some(conn_name) = connection_filter {
        let connections = engine.list_connections()?;
        if !connections.iter().any(|c| c.name == conn_name) {
            return Err(ApiError::not_found(format!(
                "Connection '{}' not found",
                conn_name
            )));
        }
    }

    // Get tables from engine
    let tables = engine.list_tables(connection_filter)?;

    // Get all connections to map connection_id to connection name
    let connections = engine.list_connections()?;
    let connection_map: HashMap<i32, String> =
        connections.into_iter().map(|c| (c.id, c.name)).collect();

    // Convert to API response format
    let table_infos: Vec<TableInfo> = tables
        .into_iter()
        .filter_map(|t| {
            // Look up connection name from connection_id
            connection_map
                .get(&t.connection_id)
                .map(|conn_name| TableInfo {
                    connection: conn_name.clone(),
                    schema: t.schema_name,
                    table: t.table_name,
                    synced: t.parquet_path.is_some(),
                    last_sync: t.last_sync,
                })
        })
        .collect();

    Ok(Json(TablesResponse {
        tables: table_infos,
    }))
}

/// Handler for GET /health
pub async fn health_handler() -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "ok",
            "service": "rivet"
        })),
    )
}

/// Handler for POST /connections
pub async fn create_connection_handler(
    State(engine): State<Arc<HotDataEngine>>,
    Json(request): Json<CreateConnectionRequest>,
) -> Result<(StatusCode, Json<CreateConnectionResponse>), ApiError> {
    // Validate name is not empty
    if request.name.trim().is_empty() {
        return Err(ApiError::bad_request("Connection name cannot be empty"));
    }

    // Check if connection already exists
    // todo: add "exists" method
    if engine
        .list_connections()?
        .iter()
        .any(|c| c.name == request.name)
    {
        return Err(ApiError::conflict(format!(
            "Connection '{}' already exists",
            request.name
        )));
    }

    // Merge source_type into config as "type" for Source enum deserialization
    let config_with_type = {
        let mut obj = match request.config {
            serde_json::Value::Object(m) => m,
            _ => return Err(ApiError::bad_request("Configuration must be a JSON object")),
        };
        obj.insert(
            "type".to_string(),
            serde_json::Value::String(request.source_type.clone()),
        );
        serde_json::Value::Object(obj)
    };

    // Deserialize to Source enum
    let source: Source = serde_json::from_value(config_with_type)
        .map_err(|e| ApiError::bad_request(format!("Invalid source configuration: {}", e)))?;

    let source_type = source.source_type().to_string();

    // Attempt to connect (discovers tables and registers catalog)
    engine.connect(&request.name, source).await.map_err(|e| {
        error!("Failed to connect to database: {}", e);
        // Extract root cause message only - don't expose full stack trace to clients
        let root_cause = e.root_cause().to_string();
        let msg = root_cause.lines().next().unwrap_or("Unknown error");

        if msg.contains("Failed to connect") || msg.contains("connection refused") {
            ApiError::bad_gateway(format!("Failed to connect to database: {}", msg))
        } else if msg.contains("Unsupported source type") || msg.contains("Invalid configuration") {
            ApiError::bad_request(msg.to_string())
        } else {
            ApiError::bad_gateway(format!("Failed to connect to database: {}", msg))
        }
    })?;

    // Count discovered tables
    let tables_discovered = engine
        .list_tables(Some(&request.name))
        .map(|t| t.len())
        .unwrap_or(0);

    Ok((
        StatusCode::CREATED,
        Json(CreateConnectionResponse {
            name: request.name,
            source_type,
            tables_discovered,
        }),
    ))
}

/// Handler for GET /connections
pub async fn list_connections_handler(
    State(engine): State<Arc<HotDataEngine>>,
) -> Result<Json<ListConnectionsResponse>, ApiError> {
    let connections = engine.list_connections()?;

    let connection_infos: Vec<ConnectionInfo> = connections
        .into_iter()
        .map(|c| ConnectionInfo {
            id: c.id,
            name: c.name,
            source_type: c.source_type,
        })
        .collect();

    Ok(Json(ListConnectionsResponse {
        connections: connection_infos,
    }))
}

/// Handler for GET /connections/{name}
pub async fn get_connection_handler(
    State(engine): State<Arc<HotDataEngine>>,
    Path(name): Path<String>,
) -> Result<Json<GetConnectionResponse>, ApiError> {
    // Get connection info
    let conn = engine
        .catalog()
        .get_connection(&name)?
        .ok_or_else(|| ApiError::not_found(format!("Connection '{}' not found", name)))?;

    // Get table counts
    let tables = engine.list_tables(Some(&name))?;
    let table_count = tables.len();
    let synced_table_count = tables.iter().filter(|t| t.parquet_path.is_some()).count();

    Ok(Json(GetConnectionResponse {
        id: conn.id,
        name: conn.name,
        source_type: conn.source_type,
        table_count,
        synced_table_count,
    }))
}
