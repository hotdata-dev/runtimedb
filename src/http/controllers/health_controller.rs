use axum::{http::StatusCode, Json};

/// Handler for GET /health
pub async fn health_handler() -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "ok",
            "service": "runtimedb"
        })),
    )
}
