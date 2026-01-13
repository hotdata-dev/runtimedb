use crate::http::handlers::{
    create_connection_handler, create_secret_handler, delete_connection_handler,
    delete_secret_handler, discover_connection_handler, get_connection_handler, get_secret_handler,
    health_handler, information_schema_handler, list_connections_handler, list_secrets_handler,
    purge_connection_cache_handler, purge_table_cache_handler, query_handler,
    update_secret_handler,
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
pub const PATH_CONNECTIONS: &str = "/connections";
pub const PATH_CONNECTION: &str = "/connections/{name}";
pub const PATH_CONNECTION_DISCOVER: &str = "/connections/{name}/discover";
pub const PATH_CONNECTION_CACHE: &str = "/connections/{name}/cache";
pub const PATH_TABLE_CACHE: &str = "/connections/{name}/tables/{schema}/{table}/cache";
pub const PATH_SECRETS: &str = "/secrets";
pub const PATH_SECRET: &str = "/secrets/{name}";

impl AppServer {
    pub fn new(engine: RuntimeEngine) -> Self {
        let engine = Arc::new(engine);
        AppServer {
            router: Router::new()
                .route(PATH_QUERY, post(query_handler))
                .route(PATH_INFORMATION_SCHEMA, get(information_schema_handler))
                .route(PATH_HEALTH, get(health_handler))
                .route(
                    PATH_CONNECTIONS,
                    post(create_connection_handler).get(list_connections_handler),
                )
                .route(
                    PATH_CONNECTION,
                    get(get_connection_handler).delete(delete_connection_handler),
                )
                .route(PATH_CONNECTION_DISCOVER, post(discover_connection_handler))
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
                .with_state(engine.clone()),
            engine,
        }
    }
}
