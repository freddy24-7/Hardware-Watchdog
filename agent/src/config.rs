use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    /// Kafka bootstrap broker list, e.g. "kafka:9092"
    pub kafka_brokers: String,

    /// Topic to publish metric snapshots to
    pub kafka_topic: String,

    /// How often to collect and publish a snapshot, in seconds
    #[serde(default = "default_interval")]
    pub metrics_interval_secs: u64,
}

fn default_interval() -> u64 {
    5
}
