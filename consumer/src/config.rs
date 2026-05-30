use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    /// Kafka bootstrap broker list, e.g. "kafka:9092"
    pub kafka_brokers: String,

    /// Topic to consume metric snapshots from
    pub kafka_topic: String,

    /// Consumer group ID — all instances share this to avoid duplicate processing
    pub kafka_group_id: String,

    /// PostgreSQL connection URL
    pub database_url: String,

    /// Maximum rows to buffer before flushing to Postgres
    #[serde(default = "default_batch_size")]
    pub consumer_batch_size: usize,

    /// Maximum milliseconds to wait before flushing a partial batch
    #[serde(default = "default_batch_timeout_ms")]
    pub consumer_batch_timeout_ms: u64,
}

fn default_batch_size() -> usize {
    50
}

fn default_batch_timeout_ms() -> u64 {
    2000
}
