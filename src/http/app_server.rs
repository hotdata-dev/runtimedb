use crate::http::handlers::{
    create_connection_handler, create_secret_handler, delete_connection_handler,
    delete_secret_handler, get_connection_handler, get_result_handler, get_secret_handler,
    health_handler, information_schema_handler, list_connections_handler, list_results_handler,
    list_secrets_handler, purge_connection_cache_handler, purge_table_cache_handler, query_handler,
    refresh_handler, update_secret_handler,
};
use crate::RuntimeEngine;
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
                .with_state(engine.clone()),
            engine,
        }
    }
}
