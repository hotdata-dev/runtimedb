use crate::metrics::MetricsEvent;
use crate::RuntimeEngine;
use axum::body::Body;
use axum::extract::{MatchedPath, Request, State};
use axum::middleware::Next;
use axum::response::Response;
use chrono::Utc;
use std::sync::Arc;
use std::time::Instant;

/// Axum middleware that captures per-request HTTP metrics.
///
/// Records start time before the handler runs, then after the response is ready
/// sends an [`MetricsEvent::HttpRequest`] to the metrics worker via a non-blocking
/// `try_send`. Events are silently dropped when the channel is full, so this
/// middleware adds no latency to the hot path.
pub async fn metrics_middleware(
    State(engine): State<Arc<RuntimeEngine>>,
    matched_path: Option<MatchedPath>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let start = Instant::now();
    let method = request.method().to_string();
    let path = matched_path
        .map(|p| p.as_str().to_owned())
        .unwrap_or_else(|| "/unknown".to_owned());

    let response = next.run(request).await;

    let _ = engine.metrics_sender.try_send(MetricsEvent::HttpRequest {
        timestamp: Utc::now(),
        path,
        method,
        status_code: response.status().as_u16(),
        latency_ms: start.elapsed().as_millis() as u64,
    });

    response
}
