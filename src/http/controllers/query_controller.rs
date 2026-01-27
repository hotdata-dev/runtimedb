use crate::http::error::ApiError;
use crate::http::models::{QueryRequest, QueryResponse};
use crate::http::serialization::{encode_value_at, make_array_encoder};
use crate::RuntimeEngine;
use axum::{extract::State, Json};
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::time::Instant;
use tracing::warn;

/// Result type for serialized batch data: (columns, nullable flags, rows)
pub(crate) type SerializedBatchData = (Vec<String>, Vec<bool>, Vec<Vec<serde_json::Value>>);

/// Handler for POST /query
#[tracing::instrument(
    name = "handler_query",
    skip(engine, request),
    fields(
        runtimedb.row_count = tracing::field::Empty,
        runtimedb.result_id = tracing::field::Empty,
    )
)]
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

    tracing::Span::current()
        .record("runtimedb.row_count", row_count)
        .record("runtimedb.result_id", result_id.as_deref().unwrap_or(""));

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
pub(crate) fn serialize_batches(
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
