use crate::datafusion::HotDataEngine;
use crate::http::handlers::{
    create_connection_handler, get_connection_handler, health_handler, list_connections_handler,
    query_handler, tables_handler,
};
use axum::routing::{get, post};
use axum::Router;
use std::sync::Arc;

pub struct AppServer {
    pub router: Router,
    pub engine: Arc<HotDataEngine>,
}

pub const PATH_QUERY: &str = "/query";
pub const PATH_TABLES: &str = "/tables";
pub const PATH_HEALTH: &str = "/health";
pub const PATH_CONNECTIONS: &str = "/connections";
pub const PATH_CONNECTION: &str = "/connections/:name";

impl AppServer {
    pub fn new(engine: HotDataEngine) -> Self {
        let engine = Arc::new(engine);
        AppServer {
            router: Router::new()
                .route(PATH_QUERY, post(query_handler))
                .route(PATH_TABLES, get(tables_handler))
                .route(PATH_HEALTH, get(health_handler))
                .route(
                    PATH_CONNECTIONS,
                    post(create_connection_handler).get(list_connections_handler),
                )
                .route(PATH_CONNECTION, get(get_connection_handler))
                .with_state(engine.clone()),
            engine,
        }
    }
}
