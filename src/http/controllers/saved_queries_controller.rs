use crate::http::controllers::query_controller::serialize_batches;
use crate::http::error::ApiError;
use crate::http::models::{
    CreateSavedQueryRequest, ExecuteSavedQueryRequest, ListSavedQueriesParams,
    ListSavedQueriesResponse, ListSavedQueryVersionsParams, ListSavedQueryVersionsResponse,
    QueryResponse, SavedQueryDetail, SavedQuerySummary, SavedQueryVersionInfo,
    UpdateSavedQueryRequest,
};
use crate::RuntimeEngine;
use axum::{
    extract::{Path, Query as QueryParams, State},
    http::StatusCode,
    Json,
};
use std::sync::Arc;

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1000;
const MAX_NAME_LENGTH: usize = 255;
const MAX_SQL_LENGTH: usize = 1_000_000;
/// Upper bound on raw (pre-trim) input length to avoid spending CPU trimming
/// extremely large whitespace payloads. Any input exceeding this cannot be
/// valid after trimming since the post-trim limits are well below this.
const MAX_RAW_INPUT_LENGTH: usize = 2_000_000;

/// Handler for POST /v1/queries
pub async fn create_saved_query(
    State(engine): State<Arc<RuntimeEngine>>,
    Json(request): Json<CreateSavedQueryRequest>,
) -> Result<(StatusCode, Json<SavedQueryDetail>), ApiError> {
    if request.name.len() > MAX_RAW_INPUT_LENGTH || request.sql.len() > MAX_RAW_INPUT_LENGTH {
        return Err(ApiError::bad_request(
            "Request field exceeds maximum length",
        ));
    }

    let name = request.name.trim().to_string();
    let sql = request.sql.trim().to_string();

    if name.is_empty() {
        return Err(ApiError::bad_request("Saved query name cannot be empty"));
    }
    if name.len() > MAX_NAME_LENGTH {
        return Err(ApiError::bad_request(format!(
            "Saved query name exceeds maximum length of {} characters",
            MAX_NAME_LENGTH
        )));
    }
    if sql.is_empty() {
        return Err(ApiError::bad_request("SQL cannot be empty"));
    }
    if sql.len() > MAX_SQL_LENGTH {
        return Err(ApiError::bad_request(format!(
            "SQL exceeds maximum length of {} characters",
            MAX_SQL_LENGTH
        )));
    }

    let saved_query = engine
        .create_saved_query(&name, &sql)
        .await
        .map_err(|e| -> ApiError { e.into() })?;

    let version = engine
        .get_saved_query_version(&saved_query.id, saved_query.latest_version)
        .await
        .map_err(|e| -> ApiError { e.into() })?
        .ok_or_else(|| {
            ApiError::internal_error(format!(
                "Version {} missing for saved query '{}'",
                saved_query.latest_version, saved_query.id
            ))
        })?;

    Ok((
        StatusCode::CREATED,
        Json(SavedQueryDetail {
            id: saved_query.id,
            name: saved_query.name,
            latest_version: saved_query.latest_version,
            sql: version.sql_text,
            sql_hash: version.sql_hash,
            created_at: saved_query.created_at,
            updated_at: saved_query.updated_at,
        }),
    ))
}

/// Handler for GET /v1/queries
pub async fn list_saved_queries(
    State(engine): State<Arc<RuntimeEngine>>,
    QueryParams(params): QueryParams<ListSavedQueriesParams>,
) -> Result<Json<ListSavedQueriesResponse>, ApiError> {
    let limit = params.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let offset = params.offset.unwrap_or(0);

    let (queries, has_more) = engine
        .list_saved_queries(limit, offset)
        .await
        .map_err(|e| -> ApiError { e.into() })?;

    let count = queries.len();
    let summaries: Vec<SavedQuerySummary> = queries
        .into_iter()
        .map(|q| SavedQuerySummary {
            id: q.id,
            name: q.name,
            latest_version: q.latest_version,
            created_at: q.created_at,
            updated_at: q.updated_at,
        })
        .collect();

    Ok(Json(ListSavedQueriesResponse {
        queries: summaries,
        count,
        offset,
        limit,
        has_more,
    }))
}

/// Handler for GET /v1/queries/{id}
pub async fn get_saved_query(
    State(engine): State<Arc<RuntimeEngine>>,
    Path(id): Path<String>,
) -> Result<Json<SavedQueryDetail>, ApiError> {
    let saved_query = engine
        .get_saved_query(&id)
        .await
        .map_err(|e| -> ApiError { e.into() })?
        .ok_or_else(|| ApiError::not_found(format!("Saved query '{}' not found", id)))?;

    let version = engine
        .get_saved_query_version(&saved_query.id, saved_query.latest_version)
        .await
        .map_err(|e| -> ApiError { e.into() })?
        .ok_or_else(|| {
            ApiError::internal_error(format!(
                "Latest version {} missing for saved query '{}'",
                saved_query.latest_version, id
            ))
        })?;

    Ok(Json(SavedQueryDetail {
        id: saved_query.id,
        name: saved_query.name,
        latest_version: saved_query.latest_version,
        sql: version.sql_text,
        sql_hash: version.sql_hash,
        created_at: saved_query.created_at,
        updated_at: saved_query.updated_at,
    }))
}

/// Handler for PUT /v1/queries/{id} — creates a new version, optionally renames
pub async fn update_saved_query(
    State(engine): State<Arc<RuntimeEngine>>,
    Path(id): Path<String>,
    Json(request): Json<UpdateSavedQueryRequest>,
) -> Result<Json<SavedQueryDetail>, ApiError> {
    let raw_too_large = request
        .sql
        .as_ref()
        .is_some_and(|s| s.len() > MAX_RAW_INPUT_LENGTH)
        || request
            .name
            .as_ref()
            .is_some_and(|n| n.len() > MAX_RAW_INPUT_LENGTH);
    if raw_too_large {
        return Err(ApiError::bad_request(
            "Request field exceeds maximum length",
        ));
    }

    let trimmed_sql = request.sql.as_deref().map(str::trim);
    if let Some(sql) = trimmed_sql {
        if sql.is_empty() {
            return Err(ApiError::bad_request("SQL cannot be empty"));
        }
        if sql.len() > MAX_SQL_LENGTH {
            return Err(ApiError::bad_request(format!(
                "SQL exceeds maximum length of {} characters",
                MAX_SQL_LENGTH
            )));
        }
    }

    let trimmed_name = request.name.as_deref().map(str::trim);
    if let Some(name) = trimmed_name {
        if name.is_empty() {
            return Err(ApiError::bad_request("Saved query name cannot be empty"));
        }
        if name.len() > MAX_NAME_LENGTH {
            return Err(ApiError::bad_request(format!(
                "Saved query name exceeds maximum length of {} characters",
                MAX_NAME_LENGTH
            )));
        }
    }

    if trimmed_name.is_none() && trimmed_sql.is_none() {
        return Err(ApiError::bad_request(
            "At least one of 'name' or 'sql' must be provided",
        ));
    }

    let saved_query = engine
        .update_saved_query(&id, trimmed_name, trimmed_sql)
        .await
        .map_err(|e| -> ApiError { e.into() })?
        .ok_or_else(|| ApiError::not_found(format!("Saved query '{}' not found", id)))?;

    let version = engine
        .get_saved_query_version(&saved_query.id, saved_query.latest_version)
        .await
        .map_err(|e| -> ApiError { e.into() })?
        .ok_or_else(|| {
            ApiError::internal_error(format!(
                "Version {} missing for saved query '{}'",
                saved_query.latest_version, saved_query.id
            ))
        })?;

    Ok(Json(SavedQueryDetail {
        id: saved_query.id,
        name: saved_query.name,
        latest_version: saved_query.latest_version,
        sql: version.sql_text,
        sql_hash: version.sql_hash,
        created_at: saved_query.created_at,
        updated_at: saved_query.updated_at,
    }))
}

/// Handler for DELETE /v1/queries/{id}
pub async fn delete_saved_query(
    State(engine): State<Arc<RuntimeEngine>>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let deleted = engine
        .delete_saved_query(&id)
        .await
        .map_err(|e| -> ApiError { e.into() })?;

    if deleted {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::not_found(format!(
            "Saved query '{}' not found",
            id
        )))
    }
}

/// Handler for GET /v1/queries/{id}/versions
pub async fn list_saved_query_versions(
    State(engine): State<Arc<RuntimeEngine>>,
    Path(id): Path<String>,
    QueryParams(params): QueryParams<ListSavedQueryVersionsParams>,
) -> Result<Json<ListSavedQueryVersionsResponse>, ApiError> {
    let limit = params.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let offset = params.offset.unwrap_or(0);

    // Verify saved query exists
    engine
        .get_saved_query(&id)
        .await
        .map_err(|e| -> ApiError { e.into() })?
        .ok_or_else(|| ApiError::not_found(format!("Saved query '{}' not found", id)))?;

    let (versions, has_more) = engine
        .list_saved_query_versions(&id, limit, offset)
        .await
        .map_err(|e| -> ApiError { e.into() })?;

    let count = versions.len();
    let version_infos: Vec<SavedQueryVersionInfo> = versions
        .into_iter()
        .map(|v| SavedQueryVersionInfo {
            version: v.version,
            sql: v.sql_text,
            sql_hash: v.sql_hash,
            created_at: v.created_at,
        })
        .collect();

    Ok(Json(ListSavedQueryVersionsResponse {
        saved_query_id: id,
        versions: version_infos,
        count,
        offset,
        limit,
        has_more,
    }))
}

/// Handler for POST /v1/queries/{id}/execute
#[tracing::instrument(name = "http_execute_saved_query", skip(engine, body))]
pub async fn execute_saved_query(
    State(engine): State<Arc<RuntimeEngine>>,
    Path(id): Path<String>,
    body: axum::body::Bytes,
) -> Result<Json<QueryResponse>, ApiError> {
    let version = if body.is_empty() {
        None
    } else {
        let req: ExecuteSavedQueryRequest = serde_json::from_slice(&body)
            .map_err(|e| ApiError::bad_request(format!("Invalid JSON: {}", e)))?;
        req.version
    };

    // engine.execute_saved_query returns NotFoundError (→ 404) if the saved
    // query or requested version does not exist, so no pre-check is needed.
    let result = engine
        .execute_saved_query(&id, version)
        .await
        .map_err(|e| -> ApiError { e.into() })?;

    let (columns, nullable, rows) = serialize_batches(&result.schema, &result.results)?;

    Ok(Json(QueryResponse {
        query_run_id: result.query_run_id,
        result_id: result.result_id,
        columns,
        nullable,
        rows,
        row_count: result.row_count,
        execution_time_ms: result.execution_time_ms,
        warning: result.warning,
    }))
}
