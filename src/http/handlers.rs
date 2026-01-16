use crate::datafetch::deserialize_arrow_schema;
use crate::http::error::ApiError;
use crate::http::models::{
    ColumnInfo, ConnectionInfo, CreateConnectionRequest, CreateConnectionResponse,
    CreateSecretRequest, CreateSecretResponse, DiscoveryStatus, GetConnectionResponse,
    GetSecretResponse, InformationSchemaResponse, ListConnectionsResponse, ListSecretsResponse,
    QueryRequest, QueryResponse, RefreshRequest, RefreshResponse, SchemaRefreshResult,
    SecretMetadataResponse, TableInfo, UpdateSecretRequest, UpdateSecretResponse,
};
use crate::http::serialization::{encode_value_at, make_array_encoder};
use crate::source::Source;
use crate::RuntimeEngine;
use axum::{
    extract::{Path, Query as QueryParams, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::error;

/// Handler for POST /query
pub async fn query_handler(
    State(engine): State<Arc<RuntimeEngine>>,
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

/// Handler for GET /information_schema
pub async fn information_schema_handler(
    State(engine): State<Arc<RuntimeEngine>>,
    QueryParams(params): QueryParams<HashMap<String, String>>,
) -> Result<Json<InformationSchemaResponse>, ApiError> {
    // Get optional connection_id filter and resolve to name
    let connection_filter = if let Some(conn_id) = params.get("connection_id") {
        let conn = engine
            .catalog()
            .get_connection_by_external_id(conn_id)
            .await?
            .ok_or_else(|| ApiError::not_found(format!("Connection '{}' not found", conn_id)))?;
        Some(conn.name)
    } else {
        None
    };

    // Get tables from engine (uses connection name internally)
    let tables = engine.list_tables(connection_filter.as_deref()).await?;

    // Get all connections to map connection_id to connection name
    let connections = engine.list_connections().await?;
    let connection_map: HashMap<i32, String> =
        connections.into_iter().map(|c| (c.id, c.name)).collect();

    // Convert to API response format
    let table_infos: Vec<TableInfo> = tables
        .into_iter()
        .filter_map(|t| {
            // Look up connection name from connection_id
            connection_map.get(&t.connection_id).map(|conn_name| {
                // Parse arrow schema to extract column information
                let columns = t
                    .arrow_schema_json
                    .as_ref()
                    .and_then(|json| {
                        deserialize_arrow_schema(json)
                            .map_err(|e| {
                                tracing::warn!(
                                    table = %t.table_name,
                                    schema = %t.schema_name,
                                    error = %e,
                                    "Failed to parse arrow schema JSON"
                                );
                                e
                            })
                            .ok()
                    })
                    .map(|schema| {
                        schema
                            .fields()
                            .iter()
                            .map(|field| ColumnInfo {
                                name: field.name().clone(),
                                data_type: format!("{}", field.data_type()),
                                nullable: field.is_nullable(),
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                TableInfo {
                    connection: conn_name.clone(),
                    schema: t.schema_name,
                    table: t.table_name,
                    synced: t.parquet_path.is_some(),
                    last_sync: t.last_sync,
                    columns,
                }
            })
        })
        .collect();

    Ok(Json(InformationSchemaResponse {
        tables: table_infos,
    }))
}

/// Handler for GET /health
pub async fn health_handler() -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "ok",
            "service": "runtimedb"
        })),
    )
}

/// Handler for POST /connections
pub async fn create_connection_handler(
    State(engine): State<Arc<RuntimeEngine>>,
    Json(request): Json<CreateConnectionRequest>,
) -> Result<(StatusCode, Json<CreateConnectionResponse>), ApiError> {
    // Validate name is not empty
    if request.name.trim().is_empty() {
        return Err(ApiError::bad_request("Connection name cannot be empty"));
    }

    // Check if connection already exists
    // todo: add "exists" method
    if engine
        .list_connections()
        .await?
        .iter()
        .any(|c| c.name == request.name)
    {
        return Err(ApiError::conflict(format!(
            "Connection '{}' already exists",
            request.name
        )));
    }

    // Build config object, handling password-to-secret conversion
    let config_with_type = {
        let mut obj = match request.config {
            serde_json::Value::Object(m) => m,
            _ => return Err(ApiError::bad_request("Configuration must be a JSON object")),
        };

        // If "password" is provided, auto-create a secret and replace with credential ref
        if let Some(password_value) = obj.remove("password") {
            if let Some(password) = password_value.as_str() {
                if !password.is_empty() {
                    // Generate secret name from connection name
                    let secret_name = format!("conn-{}-password", request.name);

                    // Create the secret
                    engine
                        .secret_manager()
                        .create(&secret_name, password.as_bytes())
                        .await
                        .map_err(|e| {
                            ApiError::internal_error(format!("Failed to store password: {}", e))
                        })?;

                    // Replace password with credential reference
                    obj.insert(
                        "credential".to_string(),
                        serde_json::json!({
                            "type": "secret_ref",
                            "name": secret_name
                        }),
                    );
                }
            }
        }

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

    // Step 1: Register the connection
    let conn_id = engine
        .register_connection(&request.name, source)
        .await
        .map_err(|e| {
            error!("Failed to register connection: {}", e);
            ApiError::internal_error(format!("Failed to register connection: {}", e))
        })?;

    // Step 2: Attempt discovery - catch errors and return partial success
    let (tables_discovered, discovery_status, discovery_error) =
        match engine.refresh_schema(conn_id).await {
            Ok((added, _, _)) => (added, DiscoveryStatus::Success, None),
            Err(e) => {
                let root_cause = e.root_cause().to_string();
                let msg = root_cause
                    .lines()
                    .next()
                    .unwrap_or("Unknown error")
                    .to_string();
                error!(
                    "Discovery failed for connection '{}': {}",
                    request.name, msg
                );
                (0, DiscoveryStatus::Failed, Some(msg))
            }
        };

    // Fetch the created connection to get external_id
    let conn = engine
        .catalog()
        .get_connection(&request.name)
        .await?
        .ok_or_else(|| ApiError::internal_error("Failed to retrieve created connection"))?;

    Ok((
        StatusCode::CREATED,
        Json(CreateConnectionResponse {
            id: conn.external_id,
            name: request.name,
            source_type,
            tables_discovered,
            discovery_status,
            discovery_error,
        }),
    ))
}

/// Handler for GET /connections
pub async fn list_connections_handler(
    State(engine): State<Arc<RuntimeEngine>>,
) -> Result<Json<ListConnectionsResponse>, ApiError> {
    let connections = engine.list_connections().await?;

    let connection_infos: Vec<ConnectionInfo> = connections
        .into_iter()
        .map(|c| ConnectionInfo {
            id: c.external_id,
            name: c.name,
            source_type: c.source_type,
        })
        .collect();

    Ok(Json(ListConnectionsResponse {
        connections: connection_infos,
    }))
}

/// Handler for GET /connections/{connection_id}
pub async fn get_connection_handler(
    State(engine): State<Arc<RuntimeEngine>>,
    Path(connection_id): Path<String>,
) -> Result<Json<GetConnectionResponse>, ApiError> {
    // Look up connection by external_id
    let conn = engine
        .catalog()
        .get_connection_by_external_id(&connection_id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("Connection '{}' not found", connection_id)))?;

    // Get table counts using connection name
    let tables = engine.list_tables(Some(&conn.name)).await?;
    let table_count = tables.len();
    let synced_table_count = tables.iter().filter(|t| t.parquet_path.is_some()).count();

    Ok(Json(GetConnectionResponse {
        id: conn.external_id,
        name: conn.name,
        source_type: conn.source_type,
        table_count,
        synced_table_count,
    }))
}

/// Handler for DELETE /connections/{connection_id}
pub async fn delete_connection_handler(
    State(engine): State<Arc<RuntimeEngine>>,
    Path(connection_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    // Look up connection by external_id to get name for deletion
    let conn = engine
        .catalog()
        .get_connection_by_external_id(&connection_id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("Connection '{}' not found", connection_id)))?;

    engine.remove_connection(&conn.name).await.map_err(|e| {
        let msg = e.to_string();
        if msg.contains("not found") {
            ApiError::not_found(msg)
        } else {
            ApiError::internal_error(msg)
        }
    })?;

    Ok(StatusCode::NO_CONTENT)
}

/// Handler for DELETE /connections/{connection_id}/cache
pub async fn purge_connection_cache_handler(
    State(engine): State<Arc<RuntimeEngine>>,
    Path(connection_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    // Look up connection by external_id
    let conn = engine
        .catalog()
        .get_connection_by_external_id(&connection_id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("Connection '{}' not found", connection_id)))?;

    engine.purge_connection(&conn.name).await.map_err(|e| {
        let msg = e.to_string();
        if msg.contains("not found") {
            ApiError::not_found(msg)
        } else {
            ApiError::internal_error(msg)
        }
    })?;

    Ok(StatusCode::NO_CONTENT)
}

/// Path parameters for table cache operations
#[derive(Deserialize)]
pub struct TableCachePath {
    connection_id: String,
    schema: String,
    table: String,
}

/// Handler for DELETE /connections/{connection_id}/tables/{schema}/{table}/cache
pub async fn purge_table_cache_handler(
    State(engine): State<Arc<RuntimeEngine>>,
    Path(params): Path<TableCachePath>,
) -> Result<StatusCode, ApiError> {
    // Look up connection by external_id
    let conn = engine
        .catalog()
        .get_connection_by_external_id(&params.connection_id)
        .await?
        .ok_or_else(|| {
            ApiError::not_found(format!("Connection '{}' not found", params.connection_id))
        })?;

    engine
        .purge_table(&conn.name, &params.schema, &params.table)
        .await
        .map_err(|e| {
            let msg = e.to_string();
            if msg.contains("not found") {
                ApiError::not_found(msg)
            } else {
                ApiError::internal_error(msg)
            }
        })?;

    Ok(StatusCode::NO_CONTENT)
}

// Secret management handlers

/// Handler for POST /secrets
pub async fn create_secret_handler(
    State(engine): State<Arc<RuntimeEngine>>,
    Json(request): Json<CreateSecretRequest>,
) -> Result<(StatusCode, Json<CreateSecretResponse>), ApiError> {
    let secret_manager = engine.secret_manager();

    secret_manager
        .create(&request.name, request.value.as_bytes())
        .await?;

    let metadata = secret_manager.get_metadata(&request.name).await?;

    Ok((
        StatusCode::CREATED,
        Json(CreateSecretResponse {
            name: metadata.name,
            created_at: metadata.created_at,
        }),
    ))
}

/// Handler for PUT /secrets/{name}
pub async fn update_secret_handler(
    State(engine): State<Arc<RuntimeEngine>>,
    Path(name): Path<String>,
    Json(request): Json<UpdateSecretRequest>,
) -> Result<Json<UpdateSecretResponse>, ApiError> {
    let secret_manager = engine.secret_manager();

    secret_manager
        .update(&name, request.value.as_bytes())
        .await?;

    let metadata = secret_manager.get_metadata(&name).await?;

    Ok(Json(UpdateSecretResponse {
        name: metadata.name,
        updated_at: metadata.updated_at,
    }))
}

/// Handler for GET /secrets
pub async fn list_secrets_handler(
    State(engine): State<Arc<RuntimeEngine>>,
) -> Result<Json<ListSecretsResponse>, ApiError> {
    let secret_manager = engine.secret_manager();

    let secrets = secret_manager.list().await?;

    Ok(Json(ListSecretsResponse {
        secrets: secrets
            .into_iter()
            .map(SecretMetadataResponse::from)
            .collect(),
    }))
}

/// Handler for GET /secrets/{name}
pub async fn get_secret_handler(
    State(engine): State<Arc<RuntimeEngine>>,
    Path(name): Path<String>,
) -> Result<Json<GetSecretResponse>, ApiError> {
    let secret_manager = engine.secret_manager();

    let metadata = secret_manager.get_metadata(&name).await?;

    Ok(Json(GetSecretResponse {
        name: metadata.name,
        created_at: metadata.created_at,
        updated_at: metadata.updated_at,
    }))
}

/// Handler for DELETE /secrets/{name}
pub async fn delete_secret_handler(
    State(engine): State<Arc<RuntimeEngine>>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    let secret_manager = engine.secret_manager();

    secret_manager.delete(&name).await?;

    Ok(StatusCode::NO_CONTENT)
}

/// Handler for POST /refresh
pub async fn refresh_handler(
    State(engine): State<Arc<RuntimeEngine>>,
    Json(request): Json<RefreshRequest>,
) -> Result<Json<RefreshResponse>, ApiError> {
    // Validate request
    if request.schema_name.is_some() && request.connection_id.is_none() {
        return Err(ApiError::bad_request("schema_name requires connection_id"));
    }
    if request.table_name.is_some() && request.schema_name.is_none() {
        return Err(ApiError::bad_request("table_name requires schema_name"));
    }
    if request.data && request.connection_id.is_none() {
        return Err(ApiError::bad_request("data refresh requires connection_id"));
    }

    // Resolve connection_id from external ID to internal ID if provided
    let conn_info = if let Some(ref external_id) = request.connection_id {
        let conn = engine
            .catalog()
            .get_connection_by_external_id(external_id)
            .await?
            .ok_or_else(|| {
                ApiError::not_found(format!("Connection '{}' not found", external_id))
            })?;
        Some((conn.id, external_id.clone()))
    } else {
        None
    };

    let response = match (
        conn_info,
        request.schema_name,
        request.table_name,
        request.data,
    ) {
        // Schema refresh: all connections
        (None, None, None, false) => {
            let result = engine.refresh_all_schemas().await?;
            RefreshResponse::Schema(result)
        }

        // Schema refresh: single connection
        (Some((conn_id, _)), None, None, false) => {
            let (added, removed, modified) = engine.refresh_schema(conn_id).await?;
            let tables = engine.catalog().list_tables(Some(conn_id)).await?;
            RefreshResponse::Schema(SchemaRefreshResult {
                connections_refreshed: 1,
                tables_discovered: tables.len(),
                tables_added: added,
                tables_removed: removed,
                tables_modified: modified,
            })
        }

        // Data refresh: single table
        (Some((conn_id, external_id)), Some(schema), Some(table), true) => {
            let result = engine
                .refresh_table_data(conn_id, &external_id, &schema, &table)
                .await?;
            RefreshResponse::Table(result)
        }

        // Data refresh: all tables in connection (or only cached tables by default)
        (Some((conn_id, external_id)), None, None, true) => {
            let result = engine
                .refresh_connection_data(conn_id, &external_id, request.include_uncached)
                .await?;
            RefreshResponse::Connection(result)
        }

        // Invalid: schema-level refresh not supported
        (Some(_), Some(_), None, false) => {
            return Err(ApiError::bad_request(
                "schema-level refresh not supported; omit schema_name to refresh entire connection",
            ));
        }

        // Invalid: data refresh with schema but no table
        (Some(_), Some(_), None, true) => {
            return Err(ApiError::bad_request(
                "data refresh with schema_name requires table_name",
            ));
        }

        // Invalid: schema refresh cannot target specific table
        (Some(_), Some(_), Some(_), false) => {
            return Err(ApiError::bad_request(
                "schema refresh cannot target specific table; use data: true for table data refresh",
            ));
        }

        _ => unreachable!(),
    };

    Ok(Json(response))
}
