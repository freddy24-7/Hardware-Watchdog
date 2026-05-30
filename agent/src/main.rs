mod config;
mod metrics;
mod producer;

use anyhow::Context;
use tracing_subscriber::{fmt, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // JSON logging in production; RUST_LOG controls the filter level
    fmt()
        .json()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cfg: config::Config = envy::from_env().context("failed to load config from environment")?;

    tracing::info!(
        brokers = %cfg.kafka_brokers,
        topic = %cfg.kafka_topic,
        interval_secs = cfg.metrics_interval_secs,
        "agent starting"
    );

    let producer = producer::MetricsProducer::new(&cfg.kafka_brokers, &cfg.kafka_topic)
        .context("failed to create Kafka producer")?;

    let mut collector =
        metrics::Collector::new().context("failed to initialise metrics collector")?;

    // First collect is a warm-up; sysinfo needs two samples to compute CPU delta
    let _ = collector.collect();

    let mut interval =
        tokio::time::interval(std::time::Duration::from_secs(cfg.metrics_interval_secs));

    // Wait for either the next tick or a shutdown signal
    let shutdown = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install CTRL+C handler");
    };
    tokio::pin!(shutdown);

    tracing::info!(
        "agent ready — collecting metrics every {}s",
        cfg.metrics_interval_secs
    );

    loop {
        tokio::select! {
            _ = interval.tick() => {
                match collector.collect() {
                    Ok(snapshot) => {
                        tracing::info!(
                            cpu = %snapshot.cpu_pct,
                            memory = %snapshot.memory_pct,
                            disk_read = snapshot.disk_read_bytes,
                            disk_write = snapshot.disk_write_bytes,
                            "metric collected"
                        );
                        if let Err(e) = producer.send(&snapshot).await {
                            tracing::error!(error = %e, "failed to publish snapshot");
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "failed to collect metrics");
                    }
                }
            }
            _ = &mut shutdown => {
                tracing::info!("shutdown signal received — exiting");
                break;
            }
        }
    }

    Ok(())
}
