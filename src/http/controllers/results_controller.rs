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
use std::time::Instant;

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

    // If not ready, return status only
    if result_meta.status != "ready" {
        return Ok(Json(GetResultResponse {
            result_id: id,
            status: result_meta.status,
            columns: None,
            nullable: None,
            rows: None,
            row_count: None,
            execution_time_ms: None,
        }));
    }

    // Status is ready, load the data
    let start = Instant::now();

    let (schema, batches) = engine
        .get_result(&id)
        .await
        .map_err(|e| ApiError::internal_error(format!("Failed to load result: {}", e)))?
        .ok_or_else(|| ApiError::internal_error("Result metadata exists but data not found"))?;

    let (columns, nullable, rows) = serialize_batches(&schema, &batches)?;
    let row_count = rows.len();
    let execution_time_ms = start.elapsed().as_millis() as u64;

    Ok(Json(GetResultResponse {
        result_id: id,
        status: "ready".to_string(),
        columns: Some(columns),
        nullable: Some(nullable),
        rows: Some(rows),
        row_count: Some(row_count),
        execution_time_ms: Some(execution_time_ms),
    }))
}
