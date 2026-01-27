use crate::datafetch::deserialize_arrow_schema;
use crate::http::error::ApiError;
use crate::http::models::{
    ColumnInfo, CreateDatasetRequest, CreateDatasetResponse, DatasetSource, DatasetSummary,
    GetDatasetResponse, ListDatasetsResponse, UpdateDatasetRequest, UpdateDatasetResponse,
};
use crate::RuntimeEngine;
use axum::{
    extract::{Path, Query as QueryParams, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use std::sync::Arc;

/// Maximum inline data size: 1MB
const MAX_INLINE_DATA_SIZE: usize = 1_048_576;

/// Default limit for listing datasets
const DEFAULT_DATASETS_LIMIT: usize = 100;

/// Maximum limit for listing datasets
const MAX_DATASETS_LIMIT: usize = 1000;

/// Query parameters for listing datasets
#[derive(Debug, Deserialize)]
pub struct ListDatasetsParams {
    /// Maximum number of datasets to return (default: 100, max: 1000)
    pub limit: Option<usize>,
    /// Offset for pagination (default: 0)
    pub offset: Option<usize>,
}

/// Handler for POST /v1/datasets - Create a dataset
#[tracing::instrument(
    name = "handler_create_dataset",
    skip(engine, request),
    fields(
        runtimedb.dataset_id = tracing::field::Empty,
        runtimedb.dataset_label = tracing::field::Empty,
        runtimedb.table_name = tracing::field::Empty,
    )
)]
pub async fn create_dataset(
    State(engine): State<Arc<RuntimeEngine>>,
    Json(request): Json<CreateDatasetRequest>,
) -> Result<(StatusCode, Json<CreateDatasetResponse>), ApiError> {
    // Validate inline data size if present
    if let DatasetSource::Inline { ref inline } = request.source {
        if inline.content.len() > MAX_INLINE_DATA_SIZE {
            return Err(ApiError::bad_request(format!(
                "Inline data exceeds maximum size of 1MB ({} bytes)",
                MAX_INLINE_DATA_SIZE
            )));
        }
    }

    // Create the dataset through the engine
    let dataset = engine
        .create_dataset(
            &request.label,
            request.table_name.as_deref(),
            request.source,
        )
        .await
        .map_err(|e| {
            let msg = e.to_string();
            if e.is_conflict() {
                ApiError::conflict(msg)
            } else if e.is_client_error() {
                ApiError::bad_request(msg)
            } else {
                ApiError::internal_error(msg)
            }
        })?;

    tracing::Span::current()
        .record("runtimedb.dataset_id", &dataset.id)
        .record("runtimedb.dataset_label", &dataset.label)
        .record("runtimedb.table_name", &dataset.table_name);

    Ok((
        StatusCode::CREATED,
        Json(CreateDatasetResponse {
            id: dataset.id,
            label: dataset.label,
            table_name: dataset.table_name,
            status: "ready".to_string(),
            created_at: dataset.created_at,
        }),
    ))
}

/// Handler for GET /v1/datasets - List all datasets
#[tracing::instrument(
    name = "handler_list_datasets",
    skip(engine),
    fields(runtimedb.dataset_count = tracing::field::Empty)
)]
pub async fn list_datasets(
    State(engine): State<Arc<RuntimeEngine>>,
    QueryParams(params): QueryParams<ListDatasetsParams>,
) -> Result<Json<ListDatasetsResponse>, ApiError> {
    let limit = params
        .limit
        .unwrap_or(DEFAULT_DATASETS_LIMIT)
        .min(MAX_DATASETS_LIMIT);
    let offset = params.offset.unwrap_or(0);

    let (datasets, has_more) = engine
        .catalog()
        .list_datasets(limit, offset)
        .await
        .map_err(|e| ApiError::internal_error(format!("Failed to list datasets: {}", e)))?;

    let count = datasets.len();
    tracing::Span::current().record("runtimedb.dataset_count", count);

    let datasets = datasets
        .into_iter()
        .map(|d| DatasetSummary {
            id: d.id,
            label: d.label,
            table_name: d.table_name,
            created_at: d.created_at,
            updated_at: d.updated_at,
        })
        .collect();

    Ok(Json(ListDatasetsResponse {
        datasets,
        count,
        offset,
        limit,
        has_more,
    }))
}

/// Handler for GET /v1/datasets/{id} - Get a specific dataset
#[tracing::instrument(
    name = "handler_get_dataset",
    skip(engine),
    fields(runtimedb.dataset_id = %id)
)]
pub async fn get_dataset(
    State(engine): State<Arc<RuntimeEngine>>,
    Path(id): Path<String>,
) -> Result<Json<GetDatasetResponse>, ApiError> {
    let dataset = engine.get_dataset(&id).await.map_err(|e| {
        let msg = e.to_string();
        if e.is_not_found() {
            ApiError::not_found(msg)
        } else {
            ApiError::internal_error(msg)
        }
    })?;

    // Parse the arrow schema to extract column information
    let columns = deserialize_arrow_schema(&dataset.arrow_schema_json)
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

    Ok(Json(GetDatasetResponse {
        id: dataset.id,
        label: dataset.label,
        schema_name: dataset.schema_name,
        table_name: dataset.table_name,
        source_type: dataset.source_type,
        created_at: dataset.created_at,
        updated_at: dataset.updated_at,
        columns,
    }))
}

/// Handler for PUT /v1/datasets/{id} - Update a dataset
#[tracing::instrument(
    name = "handler_update_dataset",
    skip(engine, request),
    fields(runtimedb.dataset_id = %id)
)]
pub async fn update_dataset(
    State(engine): State<Arc<RuntimeEngine>>,
    Path(id): Path<String>,
    Json(request): Json<UpdateDatasetRequest>,
) -> Result<Json<UpdateDatasetResponse>, ApiError> {
    let dataset = engine
        .update_dataset(&id, request.label.as_deref(), request.table_name.as_deref())
        .await
        .map_err(|e| {
            let msg = e.to_string();
            if e.is_not_found() {
                ApiError::not_found(msg)
            } else if e.is_conflict() {
                ApiError::conflict(msg)
            } else if e.is_client_error() {
                ApiError::bad_request(msg)
            } else {
                ApiError::internal_error(msg)
            }
        })?;

    Ok(Json(UpdateDatasetResponse {
        id: dataset.id,
        label: dataset.label,
        table_name: dataset.table_name,
        updated_at: dataset.updated_at,
    }))
}

/// Handler for DELETE /v1/datasets/{id} - Delete a dataset
#[tracing::instrument(
    name = "handler_delete_dataset",
    skip(engine),
    fields(runtimedb.dataset_id = %id)
)]
pub async fn delete_dataset(
    State(engine): State<Arc<RuntimeEngine>>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    engine.delete_dataset(&id).await.map_err(|e| {
        let msg = e.to_string();
        if e.is_not_found() {
            ApiError::not_found(msg)
        } else {
            ApiError::internal_error(msg)
        }
    })?;

    Ok(StatusCode::NO_CONTENT)
}
