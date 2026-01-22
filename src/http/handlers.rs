use crate::datafetch::deserialize_arrow_schema;
use crate::http::error::ApiError;
use crate::http::models::{
    ColumnInfo, ConnectionInfo, CreateConnectionRequest, CreateConnectionResponse,
    CreateSecretRequest, CreateSecretResponse, DiscoveryStatus, GetConnectionResponse,
    GetSecretResponse, InformationSchemaResponse, ListConnectionsResponse, ListResultsResponse,
    ListSecretsResponse, QueryRequest, QueryResponse, RefreshRequest, RefreshResponse, ResultInfo,
    SchemaRefreshResult, SecretMetadataResponse, TableInfo, UpdateSecretRequest,
    UpdateSecretResponse,
};
use crate::http::serialization::{encode_value_at, make_array_encoder};
use crate::source::{Credential, Source};
use crate::RuntimeEngine;
use axum::{
    extract::{Path, Query as QueryParams, State},
    http::StatusCode,
    Json,
};
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::{error, warn};

/// Result type for serialized batch data: (columns, nullable flags, rows)
type SerializedBatchData = (Vec<String>, Vec<bool>, Vec<Vec<serde_json::Value>>);

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

    let batches = &result.results;
    let schema = &result.schema;

    // Persist result and get ID (best-effort - don't fail query on persistence error)
    // todo: this likely should be entirely rolled into the engine (e.g. execute_query_and_persist(..)
    let (result_id, warning) = match engine.persist_result(schema, batches).await {
        Ok(id) => (Some(id), None),
        Err(e) => {
            warn!("Failed to persist query result: {}", e);
            (None, Some(format!("Result not persisted: {}", e)))
        }
    };

    // Serialize results for HTTP response
    let (columns, nullable, rows) = serialize_batches(schema, batches)?;
    let row_count = rows.len();

    Ok(Json(QueryResponse {
        result_id,
        columns,
        nullable,
        rows,
        row_count,
        execution_time_ms,
        warning,
    }))
}

/// Serialize record batches to columns, nullable flags, and rows for JSON response.
fn serialize_batches(
    schema: &Arc<Schema>,
    batches: &[RecordBatch],
) -> Result<SerializedBatchData, ApiError> {
    // Get column names and nullable flags from schema
    let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let nullable: Vec<bool> = schema.fields().iter().map(|f| f.is_nullable()).collect();

    // Convert rows to JSON values
    let mut rows: Vec<Vec<serde_json::Value>> = Vec::new();

    for batch in batches {
        let schema = batch.schema();
        let mut encoders: Vec<_> = batch
            .columns()
            .iter()
            .zip(schema.fields())
            .map(|(col, field)| make_array_encoder(col.as_ref(), field))
            .collect::<Result<_, _>>()
            .map_err(|e| ApiError::internal_error(format!("Failed to create encoder: {}", e)))?;

        for row_idx in 0..batch.num_rows() {
            let row_values: Vec<serde_json::Value> = encoders
                .iter_mut()
                .map(|encoder| encode_value_at(encoder, row_idx))
                .collect();
            rows.push(row_values);
        }
    }

    Ok((columns, nullable, rows))
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

    // Build config object, handling inline credential-to-secret conversion
    let (config_with_type, auto_secret) = {
        let mut obj = match request.config {
            serde_json::Value::Object(m) => m,
            _ => return Err(ApiError::bad_request("Configuration must be a JSON object")),
        };

        // Track if we auto-create a secret (stores the name and generated ID for cleanup on error)
        let mut auto_secret: Option<(String, String)> = None;

        // Helper to normalize connection name for use in secret name
        // Replaces spaces with dashes, removes invalid chars, lowercases
        let normalize_for_secret = |name: &str| -> String {
            name.chars()
                .map(|c| {
                    if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                        c.to_ascii_lowercase()
                    } else if c.is_whitespace() {
                        '-'
                    } else {
                        '_'
                    }
                })
                .collect()
        };

        // Check for inline credential fields: password, token, bearer_token
        // Only process if no explicit secret_name is provided (to avoid creating unused secrets)
        // Only one should be present; first match wins
        let credential_fields = [
            ("password", "password"),
            ("token", "token"),
            ("bearer_token", "bearer_token"),
        ];

        // Skip inline credential processing if user already provided a secret_name
        if request.secret_name.is_none() {
            for (field_name, auth_type) in credential_fields {
                if let Some(value) = obj.remove(field_name) {
                    if let Some(secret_value) = value.as_str() {
                        if !secret_value.is_empty() {
                            // For Iceberg, check if it's a REST catalog (not Glue)
                            // Glue uses IAM credentials, not bearer tokens
                            if request.source_type == "iceberg" {
                                let is_rest_catalog = obj
                                    .get("catalog_type")
                                    .and_then(|ct| ct.as_object())
                                    .map(|ct_obj| {
                                        ct_obj.get("type").and_then(|v| v.as_str()) == Some("rest")
                                            || ct_obj.contains_key("uri")
                                    })
                                    .unwrap_or(false);

                                if !is_rest_catalog {
                                    // Reject inline credentials for Iceberg Glue
                                    return Err(ApiError::bad_request(
                                        "Iceberg Glue catalogs use IAM credentials, not inline tokens. \
                                         Remove the bearer_token field.",
                                    ));
                                }
                            }

                            // Generate normalized secret name from connection name
                            // Format: conn-{normalized_conn}-{field_name} or conn-{normalized_conn}-{field_name}-{n}
                            // Max secret name length is 128 chars
                            // Reserve space for suffix "-999" (4 chars) for collision handling
                            // "conn-" = 5 chars, "-" = 1 char, field_name max = 12 chars (bearer_token), "-999" = 4 chars
                            // So normalized_conn can be at most 128 - 5 - 1 - 12 - 4 = 106 chars
                            let mut normalized_conn = normalize_for_secret(&request.name);
                            normalized_conn.truncate(106);
                            let base_secret_name =
                                format!("conn-{}-{}", normalized_conn, field_name);

                            // Try to create the secret, retrying with numeric suffix on collision
                            let (secret_name, secret_id) = {
                                use crate::secrets::SecretError;

                                // First try without suffix
                                match engine
                                    .secret_manager()
                                    .create(&base_secret_name, secret_value.as_bytes())
                                    .await
                                {
                                    Ok(id) => (base_secret_name.clone(), id),
                                    Err(SecretError::AlreadyExists(_)) => {
                                        // Retry with numeric suffixes
                                        let mut created = None;
                                        for i in 1..=999 {
                                            let suffixed_name =
                                                format!("{}-{}", base_secret_name, i);
                                            match engine
                                                .secret_manager()
                                                .create(&suffixed_name, secret_value.as_bytes())
                                                .await
                                            {
                                                Ok(id) => {
                                                    created = Some((suffixed_name, id));
                                                    break;
                                                }
                                                Err(SecretError::AlreadyExists(_)) => continue,
                                                Err(e) => {
                                                    return Err(ApiError::internal_error(format!(
                                                        "Failed to store {}: {}",
                                                        field_name, e
                                                    )));
                                                }
                                            }
                                        }
                                        created.ok_or_else(|| {
                                            ApiError::internal_error(format!(
                                                "Failed to generate unique secret name after 1000 attempts for connection '{}'",
                                                request.name
                                            ))
                                        })?
                                    }
                                    Err(e) => {
                                        return Err(ApiError::internal_error(format!(
                                            "Failed to store {}: {}",
                                            field_name, e
                                        )));
                                    }
                                }
                            };

                            // Set auth type for the source config
                            // For Iceberg REST, auth goes inside catalog_type, not at the top level
                            if request.source_type == "iceberg" {
                                // For Iceberg, inject auth into catalog_type (we know it's REST at this point)
                                if let Some(catalog_type) = obj.get_mut("catalog_type") {
                                    if let Some(ct_obj) = catalog_type.as_object_mut() {
                                        ct_obj.insert(
                                            "auth".to_string(),
                                            serde_json::Value::String(auth_type.to_string()),
                                        );
                                    }
                                }
                            } else {
                                // For all other sources, auth goes at the top level
                                obj.insert(
                                    "auth".to_string(),
                                    serde_json::Value::String(auth_type.to_string()),
                                );
                            }

                            auto_secret = Some((secret_name, secret_id));
                            break; // Only process first matching credential field
                        }
                    }
                }
            }
        } else {
            // User provided secret_name, just remove any inline credential fields
            // without processing them (they shouldn't be there, but clean up if present)
            for (field_name, _) in credential_fields {
                obj.remove(field_name);
            }
        }

        obj.insert(
            "type".to_string(),
            serde_json::Value::String(request.source_type.clone()),
        );
        (serde_json::Value::Object(obj), auto_secret)
    };

    // Helper macro to cleanup auto-created secret on error
    // This prevents orphaned secrets if subsequent operations fail
    macro_rules! cleanup_and_return {
        ($err:expr) => {{
            if let Some((ref secret_name, _)) = auto_secret {
                if let Err(e) = engine.secret_manager().delete(secret_name).await {
                    tracing::warn!(
                        secret = %secret_name,
                        error = %e,
                        "Failed to cleanup auto-created secret after connection creation failure"
                    );
                }
            }
            return Err($err);
        }};
    }

    // Deserialize to Source enum
    let source: Source = match serde_json::from_value(config_with_type) {
        Ok(s) => s,
        Err(e) => {
            cleanup_and_return!(ApiError::bad_request(format!(
                "Invalid source configuration: {}",
                e
            )));
        }
    };

    let source_type = source.source_type().to_string();

    // Validate mutual exclusivity of secret_name and secret_id
    if request.secret_name.is_some() && request.secret_id.is_some() {
        cleanup_and_return!(ApiError::bad_request(
            "Cannot specify both secret_name and secret_id; use one or the other"
        ));
    }

    // Determine which secret_id to use: explicit secret_id > secret_name > auto-created
    let effective_secret_id = if let Some(ref secret_id) = request.secret_id {
        // User provided a secret ID directly, verify it exists
        match engine.secret_manager().get_metadata_by_id(secret_id).await {
            Ok(_) => Some(secret_id.clone()),
            Err(e) => {
                cleanup_and_return!(ApiError::bad_request(format!(
                    "Secret with ID '{}' not found: {}",
                    secret_id, e
                )));
            }
        }
    } else if let Some(ref secret_name) = request.secret_name {
        // User provided a secret name, resolve it to its ID
        match engine.secret_manager().get_metadata(secret_name).await {
            Ok(metadata) => Some(metadata.id),
            Err(e) => {
                cleanup_and_return!(ApiError::bad_request(format!(
                    "Secret '{}' not found: {}",
                    secret_name, e
                )));
            }
        }
    } else {
        // Use auto-created secret ID if any
        auto_secret.as_ref().map(|(_, id)| id.clone())
    };

    // Build Source with credential embedded
    // The credential field is stored inside the Source, which will be serialized to config_json
    let source = if let Some(secret_id) = effective_secret_id {
        source.with_credential(Credential::secret_ref(secret_id))
    } else {
        source
    };

    // Step 1: Register the connection
    // Source contains the credential internally; engine extracts secret_id for DB queryability
    let conn_id = match engine.register_connection(&request.name, source).await {
        Ok(id) => id,
        Err(e) => {
            error!("Failed to register connection: {}", e);
            cleanup_and_return!(ApiError::internal_error(format!(
                "Failed to register connection: {}",
                e
            )));
        }
    };

    // Step 2: Attempt discovery - catch errors and return partial success
    let (tables_discovered, discovery_status, discovery_error) =
        match engine.refresh_schema(conn_id).await {
            Ok((added, _)) => (added, DiscoveryStatus::Success, None),
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

    let secret_id = secret_manager
        .create(&request.name, request.value.as_bytes())
        .await?;

    let metadata = secret_manager.get_metadata(&request.name).await?;

    Ok((
        StatusCode::CREATED,
        Json(CreateSecretResponse {
            id: secret_id,
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
            let (added, modified) = engine.refresh_schema(conn_id).await?;
            let tables = engine.catalog().list_tables(Some(conn_id)).await?;
            RefreshResponse::Schema(SchemaRefreshResult {
                connections_refreshed: 1,
                connections_failed: 0,
                tables_discovered: tables.len(),
                tables_added: added,
                tables_modified: modified,
                errors: Vec::new(),
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

/// Default limit for listing results
const DEFAULT_RESULTS_LIMIT: usize = 100;

/// Maximum limit for listing results
const MAX_RESULTS_LIMIT: usize = 1000;

/// Query parameters for listing results
#[derive(Debug, Deserialize)]
pub struct ListResultsParams {
    /// Maximum number of results to return (default: 100, max: 1000)
    pub limit: Option<usize>,
    /// Offset for pagination (default: 0)
    pub offset: Option<usize>,
}

/// Handler for GET /results
pub async fn list_results_handler(
    State(engine): State<Arc<RuntimeEngine>>,
    QueryParams(params): QueryParams<ListResultsParams>,
) -> Result<Json<ListResultsResponse>, ApiError> {
    let limit = params
        .limit
        .unwrap_or(DEFAULT_RESULTS_LIMIT)
        .min(MAX_RESULTS_LIMIT);
    let offset = params.offset.unwrap_or(0);

    let (results, has_more) = engine
        .list_results(limit, offset)
        .await
        .map_err(|e| ApiError::internal_error(format!("Failed to list results: {}", e)))?;

    let count = results.len();
    let results = results
        .into_iter()
        .map(|r| ResultInfo {
            id: r.id,
            created_at: r.created_at,
        })
        .collect();

    Ok(Json(ListResultsResponse {
        results,
        count,
        offset,
        limit,
        has_more,
    }))
}

/// Handler for GET /results/{id}
pub async fn get_result_handler(
    State(engine): State<Arc<RuntimeEngine>>,
    Path(id): Path<String>,
) -> Result<Json<QueryResponse>, ApiError> {
    let start = Instant::now();

    // Load result from engine (handles both catalog lookup and parquet loading)
    let (schema, batches) = engine
        .get_result(&id)
        .await
        .map_err(|e| ApiError::internal_error(format!("Failed to lookup result: {}", e)))?
        .ok_or_else(|| ApiError::not_found(format!("Result '{}' not found", id)))?;

    let (columns, nullable, rows) = serialize_batches(&schema, &batches)?;
    let row_count = rows.len();
    let execution_time_ms = start.elapsed().as_millis() as u64;

    Ok(Json(QueryResponse {
        result_id: Some(id),
        columns,
        nullable,
        rows,
        row_count,
        execution_time_ms,
        warning: None,
    }))
}
