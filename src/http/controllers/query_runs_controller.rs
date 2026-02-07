use crate::http::error::ApiError;
use crate::http::models::{ListQueryRunsParams, ListQueryRunsResponse, QueryRunInfo};
use crate::RuntimeEngine;
use axum::extract::{Query, State};
use axum::Json;
use std::sync::Arc;

/// Handler for GET /query-runs
#[tracing::instrument(
    name = "handler_list_query_runs",
    skip(engine),
    fields(
        runtimedb.limit = tracing::field::Empty,
        runtimedb.has_cursor = tracing::field::Empty,
    )
)]
pub async fn list_query_runs_handler(
    State(engine): State<Arc<RuntimeEngine>>,
    Query(params): Query<ListQueryRunsParams>,
) -> Result<Json<ListQueryRunsResponse>, ApiError> {
    tracing::Span::current().record("runtimedb.has_cursor", params.cursor.is_some());

    let page = engine
        .list_query_runs_page(params.limit, params.cursor.as_deref())
        .await
        .map_err(|e| -> ApiError { e.into() })?;

    tracing::Span::current().record("runtimedb.limit", page.limit);

    let query_runs: Vec<QueryRunInfo> = page
        .runs
        .into_iter()
        .map(|r| QueryRunInfo {
            id: r.id,
            status: r.status.to_string(),
            sql_text: r.sql_text,
            sql_hash: r.sql_hash,
            trace_id: r.trace_id,
            result_id: r.result_id,
            error_message: r.error_message,
            warning_message: r.warning_message,
            row_count: r.row_count,
            execution_time_ms: r.execution_time_ms,
            created_at: r.created_at,
            completed_at: r.completed_at,
        })
        .collect();

    Ok(Json(ListQueryRunsResponse {
        query_runs,
        count: page.count,
        limit: page.limit,
        has_more: page.has_more,
        next_cursor: page.next_cursor,
    }))
}
