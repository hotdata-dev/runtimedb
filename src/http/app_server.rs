use crate::http::handlers::{
    create_connection_handler, create_dataset, create_secret_handler, delete_connection_handler,
    delete_dataset, delete_secret_handler, get_connection_handler, get_dataset, get_result_handler,
    get_secret_handler, health_handler, information_schema_handler, list_connections_handler,
    list_datasets, list_results_handler, list_secrets_handler, list_uploads,
    purge_connection_cache_handler, purge_table_cache_handler, query_handler, refresh_handler,
    update_dataset, update_secret_handler, upload_file, MAX_UPLOAD_SIZE,
};
use crate::RuntimeEngine;
use axum::extract::DefaultBodyLimit;
use axum::routing::{delete, get, post};
use axum::Router;
use std::sync::Arc;

pub struct AppServer {
    pub router: Router,
    pub engine: Arc<RuntimeEngine>,
}

pub const PATH_QUERY: &str = "/query";
pub const PATH_INFORMATION_SCHEMA: &str = "/information_schema";
pub const PATH_HEALTH: &str = "/health";
pub const PATH_REFRESH: &str = "/refresh";
pub const PATH_CONNECTIONS: &str = "/connections";
pub const PATH_CONNECTION: &str = "/connections/{connection_id}";
pub const PATH_CONNECTION_CACHE: &str = "/connections/{connection_id}/cache";
pub const PATH_TABLE_CACHE: &str = "/connections/{connection_id}/tables/{schema}/{table}/cache";
pub const PATH_SECRETS: &str = "/secrets";
pub const PATH_SECRET: &str = "/secrets/{name}";
pub const PATH_RESULTS: &str = "/results";
pub const PATH_RESULT: &str = "/results/{id}";
pub const PATH_FILES: &str = "/v1/files";
pub const PATH_DATASETS: &str = "/v1/datasets";
pub const PATH_DATASET: &str = "/v1/datasets/{id}";

impl AppServer {
    pub fn new(engine: RuntimeEngine) -> Self {
        let engine = Arc::new(engine);
        AppServer {
            router: Router::new()
                .route(PATH_QUERY, post(query_handler))
                .route(PATH_INFORMATION_SCHEMA, get(information_schema_handler))
                .route(PATH_HEALTH, get(health_handler))
                .route(PATH_REFRESH, post(refresh_handler))
                .route(
                    PATH_CONNECTIONS,
                    post(create_connection_handler).get(list_connections_handler),
                )
                .route(
                    PATH_CONNECTION,
                    get(get_connection_handler).delete(delete_connection_handler),
                )
                .route(
                    PATH_CONNECTION_CACHE,
                    delete(purge_connection_cache_handler),
                )
                .route(PATH_TABLE_CACHE, delete(purge_table_cache_handler))
                .route(
                    PATH_SECRETS,
                    post(create_secret_handler).get(list_secrets_handler),
                )
                .route(
                    PATH_SECRET,
                    get(get_secret_handler)
                        .put(update_secret_handler)
                        .delete(delete_secret_handler),
                )
                .route(PATH_RESULTS, get(list_results_handler))
                .route(PATH_RESULT, get(get_result_handler))
                // Upload route with body limit to reject oversized requests before buffering
                .route(
                    PATH_FILES,
                    post(upload_file)
                        .layer(DefaultBodyLimit::max(MAX_UPLOAD_SIZE))
                        .get(list_uploads),
                )
                .route(PATH_DATASETS, post(create_dataset).get(list_datasets))
                .route(
                    PATH_DATASET,
                    get(get_dataset).put(update_dataset).delete(delete_dataset),
                )
                .with_state(engine.clone()),
            engine,
        }
    }
}
