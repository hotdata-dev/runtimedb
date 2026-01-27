use crate::http::error::ApiError;
use crate::http::models::{ListUploadsResponse, UploadInfo, UploadResponse};
use crate::RuntimeEngine;
use axum::{
    body::Bytes,
    extract::{Query as QueryParams, State},
    http::{header, HeaderMap, StatusCode},
    Json,
};
use serde::Deserialize;
use std::sync::Arc;

/// Query parameters for listing uploads
#[derive(Debug, Deserialize)]
pub struct ListUploadsParams {
    pub status: Option<String>,
}

/// Maximum upload size: 2GB
pub const MAX_UPLOAD_SIZE: usize = 2 * 1024 * 1024 * 1024;

/// Handler for POST /v1/files - Upload a file
#[tracing::instrument(
    name = "handler_upload_file",
    skip(engine, headers, body),
    fields(
        runtimedb.upload_id = tracing::field::Empty,
        runtimedb.size_bytes = tracing::field::Empty,
        runtimedb.content_type = tracing::field::Empty,
    )
)]
pub async fn upload_file(
    State(engine): State<Arc<RuntimeEngine>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, Json<UploadResponse>), ApiError> {
    // Validate upload is not empty
    if body.is_empty() {
        return Err(ApiError::bad_request("Upload cannot be empty"));
    }

    // Validate upload size
    if body.len() > MAX_UPLOAD_SIZE {
        return Err(ApiError::bad_request(format!(
            "Upload exceeds maximum size of {} bytes",
            MAX_UPLOAD_SIZE
        )));
    }

    // Get content type from headers
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // Store the upload
    let upload = engine
        .store_upload(body.to_vec(), content_type)
        .await
        .map_err(|e| ApiError::internal_error(format!("Failed to store upload: {}", e)))?;

    tracing::Span::current()
        .record("runtimedb.upload_id", &upload.id)
        .record("runtimedb.size_bytes", upload.size_bytes)
        .record(
            "runtimedb.content_type",
            upload.content_type.as_deref().unwrap_or(""),
        );

    Ok((
        StatusCode::CREATED,
        Json(UploadResponse {
            id: upload.id,
            status: upload.status,
            size_bytes: upload.size_bytes,
            content_type: upload.content_type,
            created_at: upload.created_at,
        }),
    ))
}

/// Handler for GET /v1/files - List uploads
#[tracing::instrument(
    name = "handler_list_uploads",
    skip(engine),
    fields(runtimedb.upload_count = tracing::field::Empty)
)]
pub async fn list_uploads(
    State(engine): State<Arc<RuntimeEngine>>,
    QueryParams(params): QueryParams<ListUploadsParams>,
) -> Result<Json<ListUploadsResponse>, ApiError> {
    let status = params.status.as_deref();
    let uploads = engine
        .catalog()
        .list_uploads(status)
        .await
        .map_err(|e| ApiError::internal_error(format!("Failed to list uploads: {}", e)))?;

    tracing::Span::current().record("runtimedb.upload_count", uploads.len());

    Ok(Json(ListUploadsResponse {
        uploads: uploads
            .into_iter()
            .map(|u| UploadInfo {
                id: u.id,
                status: u.status,
                size_bytes: u.size_bytes,
                content_type: u.content_type,
                created_at: u.created_at,
            })
            .collect(),
    }))
}
