mod config;
mod metrics;
mod producer;

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
        ingest_url = %cfg.ingest_url,
        interval_secs = cfg.metrics_interval_secs,
        "agent starting"
    );

    let producer = producer::MetricsProducer::new(cfg.ingest_url.clone());

    let mut collector =
        metrics::Collector::new().context("failed to initialise metrics collector")?;

    tracing::info!(machine_id = %collector.machine_id(), "machine ID assigned");

    // First collect is a warm-up; sysinfo needs two samples to compute CPU delta
    let _ = collector.collect();

    let mut interval =
        tokio::time::interval(std::time::Duration::from_secs(cfg.metrics_interval_secs));

    let mut shutdown = std::pin::pin!(async {
        tokio::signal::ctrl_c()
            .await
            .context("failed to install CTRL+C handler")
    });

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
                            tracing::error!(error = %e, "failed to send snapshot");
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "failed to collect metrics");
                    }
                }
            }
            result = &mut shutdown => {
                result?;
                tracing::info!("shutdown signal received — exiting");
                break;
            }
        }
    }

    Ok(())
}
