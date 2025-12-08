use anyhow::Result;
use clap::Parser;
use std::time::Instant;
use rivetdb::config::AppConfig;
use rivetdb::datafusion::HotDataEngine;
use rivetdb::http::app_server::AppServer;

#[derive(Parser)]
#[command(name = "rivet-server", about = "Rivet HTTP Server")]
struct Cli {
    /// Path to config file
    config: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let now = Instant::now();
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let cli = Cli::parse();

    tracing::info!("Starting Rivet HTTP Server");

    // Load configuration
    let config = AppConfig::load(&cli.config)?;
    config.validate()?;

    tracing::info!("Configuration '{}' loaded successfully", &cli.config);

    // Initialize engine from config
    let engine = HotDataEngine::from_config(&config)?;

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
    let server = axum::serve(listener, app.router)
        .with_graceful_shutdown(shutdown());

    server.await?;

    // Explicitly shutdown engine to close catalog connection
    if let Err(e) = engine.shutdown() {
        tracing::error!("Error during engine shutdown: {}", e);
    }

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
