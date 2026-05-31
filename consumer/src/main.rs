mod config;
mod db;
mod handlers;

use anyhow::Context;
use axum::{routing::{get, post}, Router};
use tracing_subscriber::{fmt, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    fmt()
        .json()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cfg: config::Config = envy::from_env().context("failed to load config from environment")?;

    tracing::info!(port = cfg.port, "ingest service starting");

    let pool = db::connect(&cfg.database_url)
        .await
        .context("failed to connect to postgres")?;

    tracing::info!("postgres connection established");

    let app = Router::new()
        .route("/ingest", post(handlers::ingest))
        .route("/health", get(handlers::health))
        .with_state(pool);

    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], cfg.port));
    tracing::info!(%addr, "listening");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context("failed to bind TCP listener")?;

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c()
                .await
                .expect("failed to install CTRL+C handler");
            tracing::info!("shutdown signal received — exiting");
        })
        .await
        .context("server error")?;

    Ok(())
}
