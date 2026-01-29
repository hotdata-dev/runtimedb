use crate::datafetch::deserialize_arrow_schema;
use crate::http::error::ApiError;
use crate::http::models::{ColumnInfo, InformationSchemaResponse, TableInfo};
use crate::RuntimeEngine;
use axum::{
    extract::{Query as QueryParams, State},
    Json,
};
use std::collections::HashMap;
use std::sync::Arc;

/// Handler for GET /information_schema
#[tracing::instrument(
    name = "handler_information_schema",
    skip(engine, params),
    fields(runtimedb.table_count = tracing::field::Empty)
)]
pub async fn information_schema_handler(
    State(engine): State<Arc<RuntimeEngine>>,
    QueryParams(params): QueryParams<HashMap<String, String>>,
) -> Result<Json<InformationSchemaResponse>, ApiError> {
    // Get optional connection_id filter
    let connection_id = params.get("connection_id").map(|s| s.as_str());

    // Get tables from engine
    let tables = engine.list_tables(connection_id).await.map_err(|e| {
        // Check if the error is a "connection not found" error
        let msg = e.to_string();
        if msg.contains("not found") {
            ApiError::not_found(msg)
        } else {
            ApiError::from(e)
        }
    })?;

    // Get all connections to map connection_id to connection name
    let connections = engine.list_connections().await?;
    let connection_map: HashMap<String, String> =
        connections.into_iter().map(|c| (c.id, c.name)).collect();

    // Convert to API response format
    let table_infos: Vec<TableInfo> = tables
        .into_iter()
        .filter_map(|t| {
            // Look up connection name from connection_id
            connection_map.get(&t.connection_id).map(|conn_name| {
                // Parse arrow schema to extract column information
                let columns = t
                    .arrow_schema_json
                    .as_ref()
                    .and_then(|json| {
                        deserialize_arrow_schema(json)
                            .map_err(|e| {
                                tracing::warn!(
                                    table = %t.table_name,
                                    schema = %t.schema_name,
                                    error = %e,
                                    "Failed to parse arrow schema JSON"
                                );
                                e
                            })
                            .ok()
                    })
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

                TableInfo {
                    connection: conn_name.clone(),
                    schema: t.schema_name,
                    table: t.table_name,
                    synced: t.parquet_path.is_some(),
                    last_sync: t.last_sync,
                    columns,
                }
            })
        })
        .collect();

    tracing::Span::current().record("runtimedb.table_count", table_infos.len());

    Ok(Json(InformationSchemaResponse {
        tables: table_infos,
    }))
}
