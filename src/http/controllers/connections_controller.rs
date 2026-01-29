use crate::http::error::ApiError;
use crate::http::models::{
    ConnectionInfo, CreateConnectionRequest, CreateConnectionResponse, DiscoveryStatus,
    GetConnectionResponse, ListConnectionsResponse,
};
use crate::source::{Credential, Source};
use crate::RuntimeEngine;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use std::sync::Arc;
use tracing::error;

/// Handler for POST /connections
#[tracing::instrument(
    name = "handler_create_connection",
    skip(engine, request),
    fields(
        runtimedb.connection_name = %request.name,
        runtimedb.source_type = %request.source_type,
        runtimedb.connection_id = tracing::field::Empty,
    )
)]
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
        match engine.refresh_schema(&conn_id).await {
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

    tracing::Span::current().record("runtimedb.connection_id", &conn_id);

    Ok((
        StatusCode::CREATED,
        Json(CreateConnectionResponse {
            id: conn_id,
            name: request.name,
            source_type,
            tables_discovered,
            discovery_status,
            discovery_error,
        }),
    ))
}

/// Handler for GET /connections
#[tracing::instrument(name = "handler_list_connections", skip(engine))]
pub async fn list_connections_handler(
    State(engine): State<Arc<RuntimeEngine>>,
) -> Result<Json<ListConnectionsResponse>, ApiError> {
    let connections = engine.list_connections().await?;

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

/// Handler for GET /connections/{connection_id}
#[tracing::instrument(
    name = "handler_get_connection",
    skip(engine),
    fields(runtimedb.connection_id = %connection_id)
)]
pub async fn get_connection_handler(
    State(engine): State<Arc<RuntimeEngine>>,
    Path(connection_id): Path<String>,
) -> Result<Json<GetConnectionResponse>, ApiError> {
    // Look up connection by id
    let conn = engine
        .catalog()
        .get_connection(&connection_id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("Connection '{}' not found", connection_id)))?;

    // Get table counts using connection id
    let tables = engine.list_tables(Some(&connection_id)).await?;
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

/// Handler for DELETE /connections/{connection_id}
#[tracing::instrument(
    name = "handler_delete_connection",
    skip(engine),
    fields(runtimedb.connection_id = %connection_id)
)]
pub async fn delete_connection_handler(
    State(engine): State<Arc<RuntimeEngine>>,
    Path(connection_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    engine
        .remove_connection(&connection_id)
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

/// Handler for DELETE /connections/{connection_id}/cache
#[tracing::instrument(
    name = "handler_purge_connection_cache",
    skip(engine),
    fields(runtimedb.connection_id = %connection_id)
)]
pub async fn purge_connection_cache_handler(
    State(engine): State<Arc<RuntimeEngine>>,
    Path(connection_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    engine.purge_connection(&connection_id).await.map_err(|e| {
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
#[derive(Debug, Deserialize)]
pub struct TableCachePath {
    connection_id: String,
    schema: String,
    table: String,
}

/// Handler for DELETE /connections/{connection_id}/tables/{schema}/{table}/cache
#[tracing::instrument(
    name = "handler_purge_table_cache",
    skip(engine),
    fields(
        runtimedb.connection_id = %params.connection_id,
        runtimedb.schema = %params.schema,
        runtimedb.table = %params.table,
    )
)]
pub async fn purge_table_cache_handler(
    State(engine): State<Arc<RuntimeEngine>>,
    Path(params): Path<TableCachePath>,
) -> Result<StatusCode, ApiError> {
    engine
        .purge_table(&params.connection_id, &params.schema, &params.table)
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
