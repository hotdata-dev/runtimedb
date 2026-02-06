use anyhow::Result;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use base64::{engine::general_purpose::STANDARD, Engine};
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
use rand::RngCore;
use runtimedb::http::app_server::{AppServer, PATH_HEALTH, PATH_QUERY};
use runtimedb::RuntimeEngine;
use serde_json::json;
use tower::util::ServiceExt;
use tracing_subscriber::layer::SubscriberExt;

fn generate_test_secret_key() -> String {
    let mut key = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut key);
    STANDARD.encode(key)
}

/// Build a tracing subscriber with a real OTel tracer so spans get valid trace IDs.
fn otel_subscriber() -> (impl tracing::Subscriber, SdkTracerProvider) {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter)
        .build();
    let tracer = provider.tracer("test");
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    let subscriber = tracing_subscriber::registry().with(otel_layer);
    (subscriber, provider)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_trace_headers_present_on_query() -> Result<()> {
    let (subscriber, _provider) = otel_subscriber();
    let _guard = tracing::subscriber::set_default(subscriber);

    let temp_dir = tempfile::tempdir()?;
    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .secret_key(generate_test_secret_key())
        .build()
        .await?;
    let app = AppServer::new(engine);

    let response = app
        .router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PATH_QUERY)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&json!({
                    "sql": "SELECT 1"
                }))?))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    // Verify traceparent header exists and follows W3C format
    let traceparent = response
        .headers()
        .get("traceparent")
        .expect("traceparent header should be present")
        .to_str()?;

    let parts: Vec<&str> = traceparent.split('-').collect();
    assert_eq!(parts.len(), 4, "traceparent must have 4 parts");
    assert_eq!(parts[0], "00", "version must be 00");
    assert_eq!(parts[1].len(), 32, "trace-id must be 32 hex chars");
    assert_eq!(parts[2].len(), 16, "parent-id must be 16 hex chars");
    assert_eq!(parts[3].len(), 2, "trace-flags must be 2 hex chars");

    // Verify trace-id is not all zeros (valid trace)
    assert_ne!(parts[1], "00000000000000000000000000000000");

    // Verify X-Trace-Id header matches the trace-id from traceparent
    let x_trace_id = response
        .headers()
        .get("x-trace-id")
        .expect("x-trace-id header should be present")
        .to_str()?;

    assert_eq!(
        x_trace_id, parts[1],
        "X-Trace-Id must match traceparent trace-id"
    );
    assert_eq!(x_trace_id.len(), 32);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_trace_headers_present_on_health() -> Result<()> {
    let (subscriber, _provider) = otel_subscriber();
    let _guard = tracing::subscriber::set_default(subscriber);

    let temp_dir = tempfile::tempdir()?;
    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .secret_key(generate_test_secret_key())
        .build()
        .await?;
    let app = AppServer::new(engine);

    let response = app
        .router
        .oneshot(Request::builder().uri(PATH_HEALTH).body(Body::empty())?)
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let traceparent = response
        .headers()
        .get("traceparent")
        .expect("traceparent header should be present on health endpoint")
        .to_str()?;

    let parts: Vec<&str> = traceparent.split('-').collect();
    assert_eq!(parts.len(), 4);
    assert_ne!(parts[1], "00000000000000000000000000000000");

    assert!(
        response.headers().get("x-trace-id").is_some(),
        "x-trace-id should be present on health endpoint"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_trace_headers_absent_without_otel() -> Result<()> {
    // No OTel subscriber — spans won't have valid trace context
    let temp_dir = tempfile::tempdir()?;
    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .secret_key(generate_test_secret_key())
        .build()
        .await?;
    let app = AppServer::new(engine);

    let response = app
        .router
        .oneshot(Request::builder().uri(PATH_HEALTH).body(Body::empty())?)
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    assert!(
        response.headers().get("traceparent").is_none(),
        "traceparent should be absent when OTel is not configured"
    );
    assert!(
        response.headers().get("x-trace-id").is_none(),
        "x-trace-id should be absent when OTel is not configured"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_different_requests_get_different_trace_ids() -> Result<()> {
    let (subscriber, _provider) = otel_subscriber();
    let _guard = tracing::subscriber::set_default(subscriber);

    let temp_dir = tempfile::tempdir()?;
    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .secret_key(generate_test_secret_key())
        .build()
        .await?;
    let app = AppServer::new(engine);

    let response1 = app
        .router
        .clone()
        .oneshot(Request::builder().uri(PATH_HEALTH).body(Body::empty())?)
        .await?;

    let response2 = app
        .router
        .oneshot(Request::builder().uri(PATH_HEALTH).body(Body::empty())?)
        .await?;

    let trace_id1 = response1.headers().get("x-trace-id").unwrap().to_str()?;
    let trace_id2 = response2.headers().get("x-trace-id").unwrap().to_str()?;

    assert_ne!(
        trace_id1, trace_id2,
        "each request should get a unique trace ID"
    );

    Ok(())
}
