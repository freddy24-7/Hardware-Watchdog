mod config;
mod consumer;
mod db;

use anyhow::Context;
use tracing_subscriber::{fmt, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    fmt()
        .json()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cfg: config::Config = envy::from_env().context("failed to load config from environment")?;

    tracing::info!(
        brokers = %cfg.kafka_brokers,
        topic = %cfg.kafka_topic,
        group_id = %cfg.kafka_group_id,
        batch_size = cfg.consumer_batch_size,
        "consumer starting"
    );

    let pool = db::connect(&cfg.database_url)
        .await
        .context("failed to connect to postgres")?;

    tracing::info!("postgres connection established");

    let kafka_consumer = consumer::build_consumer(&cfg.kafka_brokers, &cfg.kafka_group_id)
        .context("failed to create Kafka consumer")?;

    let mut shutdown = std::pin::pin!(async {
        tokio::signal::ctrl_c()
            .await
            .context("failed to install CTRL+C handler")
    });

    tokio::select! {
        result = consumer::run_consumer_loop(
            &kafka_consumer,
            &cfg.kafka_topic,
            &pool,
            cfg.consumer_batch_size,
            cfg.consumer_batch_timeout_ms,
        ) => {
            if let Err(e) = result {
                tracing::error!(error = %e, "consumer loop exited with error");
                return Err(e.into());
            }
        }
        result = &mut shutdown => {
            result?;
            tracing::info!("shutdown signal received — exiting");
        }
    }

    Ok(())
}
