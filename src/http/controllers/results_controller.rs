use crate::catalog::ResultStatus;
use crate::http::controllers::query_controller::serialize_batches;
use crate::http::error::ApiError;
use crate::http::models::{GetResultResponse, ListResultsResponse, ResultInfo};
use crate::RuntimeEngine;
use axum::{
    extract::{Path, Query as QueryParams, State},
    Json,
};
use serde::Deserialize;
use std::sync::Arc;

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
#[tracing::instrument(name = "handler_list_results", skip(engine, params))]
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
            status: r.status.to_string(),
            error_message: r.error_message,
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
#[tracing::instrument(
    name = "handler_get_result",
    skip(engine),
    fields(runtimedb.result_id = %id)
)]
pub async fn get_result_handler(
    State(engine): State<Arc<RuntimeEngine>>,
    Path(id): Path<String>,
) -> Result<Json<GetResultResponse>, ApiError> {
    // First check the result status in catalog
    let result_meta = engine
        .get_result_metadata(&id)
        .await
        .map_err(|e| ApiError::internal_error(format!("Failed to lookup result: {}", e)))?
        .ok_or_else(|| ApiError::not_found(format!("Result '{}' not found", id)))?;

    // If not ready, return status only (with error message if failed)
    if result_meta.status != ResultStatus::Ready {
        return Ok(Json(GetResultResponse {
            result_id: id,
            status: result_meta.status.to_string(),
            error_message: result_meta.error_message,
            columns: None,
            nullable: None,
            rows: None,
            row_count: None,
        }));
    }

    // Status is ready, load the data
    let (schema, batches) = match engine.get_result(&id).await {
        Ok(Some(data)) => data,
        Ok(None) => {
            // Parquet file is missing (deleted concurrently or partial failure).
            // Mark the result as failed and return the failed status.
            let error_message = "Result data not found (file may have been deleted)";
            if let Err(e) = engine.mark_result_failed(&id, error_message).await {
                tracing::warn!(
                    result_id = %id,
                    error = %e,
                    "Failed to mark result as failed after data not found"
                );
            }
            return Ok(Json(GetResultResponse {
                result_id: id,
                status: ResultStatus::Failed.to_string(),
                error_message: Some(error_message.to_string()),
                columns: None,
                nullable: None,
                rows: None,
                row_count: None,
            }));
        }
        Err(e) => {
            return Err(ApiError::internal_error(format!(
                "Failed to load result: {}",
                e
            )));
        }
    };

    let (columns, nullable, rows) = serialize_batches(&schema, &batches)?;
    let row_count = rows.len();

    Ok(Json(GetResultResponse {
        result_id: id,
        status: ResultStatus::Ready.to_string(),
        error_message: None,
        columns: Some(columns),
        nullable: Some(nullable),
        rows: Some(rows),
        row_count: Some(row_count),
    }))
}
