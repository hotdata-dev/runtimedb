use crate::http::error::ApiError;
use crate::http::models::{
    CreateSecretRequest, CreateSecretResponse, GetSecretResponse, ListSecretsResponse,
    SecretMetadataResponse, UpdateSecretRequest, UpdateSecretResponse,
};
use crate::RuntimeEngine;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use std::sync::Arc;

/// Handler for POST /secrets
#[tracing::instrument(
    name = "handler_create_secret",
    skip(engine, request),
    fields(runtimedb.secret_name = %request.name)
)]
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
#[tracing::instrument(
    name = "handler_update_secret",
    skip(engine, request),
    fields(runtimedb.secret_name = %name)
)]
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
#[tracing::instrument(name = "handler_list_secrets", skip(engine))]
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
#[tracing::instrument(
    name = "handler_get_secret",
    skip(engine),
    fields(runtimedb.secret_name = %name)
)]
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
#[tracing::instrument(
    name = "handler_delete_secret",
    skip(engine),
    fields(runtimedb.secret_name = %name)
)]
pub async fn delete_secret_handler(
    State(engine): State<Arc<RuntimeEngine>>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    let secret_manager = engine.secret_manager();

    secret_manager.delete(&name).await?;

    Ok(StatusCode::NO_CONTENT)
}
