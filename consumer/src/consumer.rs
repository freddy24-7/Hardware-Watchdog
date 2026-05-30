use rdkafka::{
    consumer::{CommitMode, Consumer, StreamConsumer},
    ClientConfig, Message,
};
use serde::Deserialize;
use sqlx::PgPool;
use thiserror::Error;
use time::OffsetDateTime;

use crate::db;

#[derive(Debug, Error)]
pub enum ConsumerError {
    #[error("failed to create Kafka consumer: {0}")]
    Creation(rdkafka::error::KafkaError),

    #[error("failed to subscribe to topic: {0}")]
    Subscribe(rdkafka::error::KafkaError),

    #[error("Kafka receive error: {0}")]
    Receive(rdkafka::error::KafkaError),

    #[error("failed to deserialise message: {0}")]
    Deserialise(#[from] serde_json::Error),

    #[error("message has no payload")]
    EmptyPayload,

    #[error("database error: {0}")]
    Db(#[from] db::DbError),

    #[error("failed to commit offsets: {0}")]
    Commit(rdkafka::error::KafkaError),
}

/// Wire format published by the agent
#[derive(Debug, Deserialize)]
struct AgentSnapshot {
    collected_at: String,
    host: String,
    cpu_pct: f64,
    memory_pct: f64,
    disk_read_bytes: u64,
    disk_write_bytes: u64,
}

/// Normalised DB row — uses types that map directly to Postgres columns
#[derive(Debug)]
pub struct MetricsRow {
    pub collected_at: OffsetDateTime,
    pub host: String,
    pub cpu_pct: f64,
    pub memory_pct: f64,
    pub disk_read_bytes: i64,
    pub disk_write_bytes: i64,
}

pub fn build_consumer(brokers: &str, group_id: &str) -> Result<StreamConsumer, ConsumerError> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group_id)
        .set("enable.auto.commit", "false") // manual commit after DB write
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .create()
        .map_err(ConsumerError::Creation)?;

    Ok(consumer)
}

pub async fn run_consumer_loop(
    consumer: &StreamConsumer,
    topic: &str,
    pool: &PgPool,
    batch_size: usize,
    batch_timeout_ms: u64,
) -> Result<(), ConsumerError> {
    consumer
        .subscribe(&[topic])
        .map_err(ConsumerError::Subscribe)?;

    tracing::info!(%topic, %batch_size, %batch_timeout_ms, "consumer subscribed");

    let batch_timeout = std::time::Duration::from_millis(batch_timeout_ms);

    loop {
        let mut batch: Vec<MetricsRow> = Vec::with_capacity(batch_size);
        let deadline = tokio::time::Instant::now() + batch_timeout;

        // Accumulate messages until the batch is full or the timeout fires
        while batch.len() < batch_size {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }

            match tokio::time::timeout(remaining, consumer.recv()).await {
                Err(_) => break, // timeout — flush partial batch
                Ok(Err(e)) => return Err(ConsumerError::Receive(e)),
                Ok(Ok(msg)) => {
                    let payload = msg.payload().ok_or(ConsumerError::EmptyPayload)?;
                    let snapshot: AgentSnapshot = serde_json::from_slice(payload)?;

                    let collected_at = time::OffsetDateTime::parse(
                        &snapshot.collected_at,
                        &time::format_description::well_known::Rfc3339,
                    )
                    .unwrap_or_else(|_| OffsetDateTime::now_utc());

                    batch.push(MetricsRow {
                        collected_at,
                        host: snapshot.host,
                        cpu_pct: snapshot.cpu_pct,
                        memory_pct: snapshot.memory_pct,
                        // Cast u64 → i64: Postgres BIGINT is signed. Values will
                        // never exceed i64::MAX (9.2 EB) in practice.
                        disk_read_bytes: snapshot.disk_read_bytes as i64,
                        disk_write_bytes: snapshot.disk_write_bytes as i64,
                    });
                }
            }
        }

        if batch.is_empty() {
            continue;
        }

        // Write to DB first — only then commit offsets. If we crash between
        // write and commit, Kafka redelivers and the upsert is idempotent.
        db::upsert_batch(pool, &batch).await?;

        tracing::info!(rows = batch.len(), "batch written to postgres");

        consumer
            .commit_consumer_state(CommitMode::Async)
            .map_err(ConsumerError::Commit)?;
    }
}
