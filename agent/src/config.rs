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

    // SASL fields are optional — omit them for plaintext local clusters
    /// SASL username (required when KAFKA_SECURITY_PROTOCOL is SASL_SSL)
    pub kafka_sasl_username: Option<String>,

    /// SASL password (required when KAFKA_SECURITY_PROTOCOL is SASL_SSL)
    pub kafka_sasl_password: Option<String>,

    /// Security protocol: PLAINTEXT or SASL_SSL (default: PLAINTEXT)
    #[serde(default = "default_security_protocol")]
    pub kafka_security_protocol: String,

    /// SASL mechanism: SCRAM-SHA-256 or SCRAM-SHA-512 (default: SCRAM-SHA-256)
    #[serde(default = "default_sasl_mechanism")]
    pub kafka_sasl_mechanism: String,
}

fn default_interval() -> u64 {
    5
}

fn default_security_protocol() -> String {
    "PLAINTEXT".to_owned()
}

fn default_sasl_mechanism() -> String {
    "SCRAM-SHA-256".to_owned()
}
