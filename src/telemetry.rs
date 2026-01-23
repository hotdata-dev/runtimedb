//! OpenTelemetry telemetry initialization.
//!
//! Provides `init_telemetry()` for tracing setup and `shutdown_telemetry()` for cleanup.
//! When `OTEL_EXPORTER_OTLP_ENDPOINT` is set, traces are exported via OTLP.
//! Otherwise, only console logging is enabled.

use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use std::sync::OnceLock;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

static TRACER_PROVIDER: OnceLock<SdkTracerProvider> = OnceLock::new();

/// Initialize telemetry with optional OTLP export.
///
/// Checks `OTEL_EXPORTER_OTLP_ENDPOINT` to determine if OTLP export is enabled.
/// Sets up tracing-subscriber with both console output and OpenTelemetry layer.
///
/// # Environment Variables
/// - `OTEL_EXPORTER_OTLP_ENDPOINT`: OTLP endpoint URL (enables export when set)
/// - `OTEL_SERVICE_NAME`: Service name in traces (default: "runtimedb")
/// - `OTEL_TRACES_SAMPLER`: Sampler type (default: "traceidratio")
/// - `OTEL_TRACES_SAMPLER_ARG`: Sampling ratio 0.0-1.0 (default: "1.0")
/// - `OTEL_RUNTIMEDB_INCLUDE_SQL`: Include SQL in spans (default: "true")
pub fn init_telemetry() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Set global propagator for W3C Trace Context
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    let env_filter = EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into());

    let fmt_layer = tracing_subscriber::fmt::layer();

    if let Ok(endpoint) = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
        // OTLP export enabled
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(&endpoint)
            .build()?;

        let tracer_provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .build();

        // Store provider for shutdown
        let _ = TRACER_PROVIDER.set(tracer_provider.clone());

        let tracer = tracer_provider.tracer("runtimedb");
        let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .with(otel_layer)
            .init();

        tracing::info!(endpoint = %endpoint, "OpenTelemetry OTLP export enabled");
    } else {
        // Console-only logging
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .init();
    }

    Ok(())
}

/// Shutdown telemetry and flush pending spans.
///
/// Should be called during graceful shutdown to ensure all spans are exported.
pub fn shutdown_telemetry() {
    if let Some(provider) = TRACER_PROVIDER.get() {
        if let Err(e) = provider.shutdown() {
            eprintln!("Error shutting down tracer provider: {:?}", e);
        }
    }
}

/// Check if SQL should be included in trace attributes.
///
/// Controlled by `OTEL_RUNTIMEDB_INCLUDE_SQL` env var (default: true).
pub fn include_sql_in_traces() -> bool {
    std::env::var("OTEL_RUNTIMEDB_INCLUDE_SQL")
        .map(|v| v != "false" && v != "0")
        .unwrap_or(true)
}
