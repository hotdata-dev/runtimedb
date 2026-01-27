use crate::http::error::ApiError;
use crate::http::models::{RefreshRequest, RefreshResponse, SchemaRefreshResult};
use crate::RuntimeEngine;
use axum::{extract::State, Json};
use std::sync::Arc;

/// Handler for POST /refresh
#[tracing::instrument(
    name = "handler_refresh",
    skip(engine, request),
    fields(
        runtimedb.connection_id = request.connection_id.as_deref().unwrap_or("all"),
        runtimedb.data_refresh = request.data,
    )
)]
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

    // Validate connection_id exists if provided
    let connection_id = if let Some(ref ext_id) = request.connection_id {
        // Verify connection exists
        engine
            .catalog()
            .get_connection_by_external_id(ext_id)
            .await?
            .ok_or_else(|| ApiError::not_found(format!("Connection '{}' not found", ext_id)))?;
        Some(ext_id.clone())
    } else {
        None
    };

    let response = match (
        connection_id,
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
        (Some(conn_id), None, None, false) => {
            let (added, modified) = engine.refresh_schema(&conn_id).await?;
            let tables = engine.list_tables(Some(&conn_id)).await?;
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
        (Some(conn_id), Some(schema), Some(table), true) => {
            let result = engine.refresh_table_data(&conn_id, &schema, &table).await?;
            RefreshResponse::Table(result)
        }

        // Data refresh: all tables in connection (or only cached tables by default)
        (Some(conn_id), None, None, true) => {
            let result = engine
                .refresh_connection_data(&conn_id, request.include_uncached)
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
