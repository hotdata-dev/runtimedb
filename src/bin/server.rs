use anyhow::Result;
use clap::Parser;
use runtimedb::config::AppConfig;
use runtimedb::http::app_server::AppServer;
use runtimedb::RuntimeEngine;
use std::time::Instant;

#[derive(Parser)]
#[command(name = "runtime-server", about = "RuntimeDB HTTP Server")]
struct Cli {
    /// Path to config file
    config: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let now = Instant::now();
    // Initialize telemetry (tracing + optional OpenTelemetry)
    runtimedb::telemetry::init_telemetry().expect("Failed to initialize telemetry");

    let cli = Cli::parse();

    tracing::info!("Starting RuntimeDB HTTP Server");

    // Load configuration
    let config = AppConfig::load(&cli.config)?;
    config.validate()?;

    tracing::info!("Configuration '{}' loaded successfully", &cli.config);

    // Initialize engine from config
    let engine = RuntimeEngine::from_config(&config).await?;

    tracing::info!("Engine initialized");

    // Create router
    let app = AppServer::new(engine);
    let engine = app.engine.clone();

    // Create server address
    let addr = format!("{}:{}", config.server.host, config.server.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;

    tracing::info!("Server started in {}ms", now.elapsed().as_millis());
    tracing::info!("Server listening on {}", addr);

    // Start server
    let server = axum::serve(listener, app.router).with_graceful_shutdown(shutdown());

    server.await?;

    // Explicitly shutdown engine to close catalog connection
    if let Err(e) = engine.shutdown().await {
        tracing::error!("Error during engine shutdown: {}", e);
    }

    // Flush pending telemetry spans
    runtimedb::telemetry::shutdown_telemetry();

    tracing::info!("Server shutdown complete");

    Ok(())
}

async fn shutdown() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!("Shutdown signal received, stopping server...");
}
