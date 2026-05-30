use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use thiserror::Error;

use crate::config::Config;
use crate::metrics::MetricsSnapshot;

#[derive(Debug, Error)]
pub enum ProducerError {
    #[error("failed to create Kafka producer: {0}")]
    Creation(rdkafka::error::KafkaError),

    #[error("failed to serialise metrics snapshot: {0}")]
    Serialise(#[from] serde_json::Error),

    #[error("failed to produce message: {0}")]
    Send(rdkafka::error::KafkaError),
}

pub struct MetricsProducer {
    inner: FutureProducer,
    topic: String,
}

impl MetricsProducer {
    pub fn new(cfg: &Config) -> Result<Self, ProducerError> {
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &cfg.kafka_brokers)
            // At-least-once: wait for leader acknowledgement before returning
            .set("acks", "1")
            // Retry up to 5 times on transient send failures
            .set("retries", "5")
            .set("retry.backoff.ms", "500")
            .set("message.timeout.ms", "10000")
            .set("security.protocol", &cfg.kafka_security_protocol);

        // Apply SASL settings only when credentials are provided
        if let (Some(username), Some(password)) = (
            cfg.kafka_sasl_username.as_deref(),
            cfg.kafka_sasl_password.as_deref(),
        ) {
            client_config
                .set("sasl.mechanisms", &cfg.kafka_sasl_mechanism)
                .set("sasl.username", username)
                .set("sasl.password", password);
        }

        let inner = client_config
            .create::<FutureProducer>()
            .map_err(ProducerError::Creation)?;

        Ok(Self {
            inner,
            topic: cfg.kafka_topic.clone(),
        })
    }

    /// Serialises the snapshot to JSON and publishes it.
    /// Uses the host field as the partition key so all metrics from one host
    /// land on the same partition and arrive in order.
    pub async fn send(&self, snapshot: &MetricsSnapshot) -> Result<(), ProducerError> {
        let payload = serde_json::to_string(snapshot)?;

        let record = FutureRecord::to(&self.topic)
            .payload(payload.as_bytes())
            .key(snapshot.host.as_bytes());

        self.inner
            .send(record, std::time::Duration::from_secs(10))
            .await
            .map_err(|(e, _)| ProducerError::Send(e))?;

        Ok(())
    }
}
