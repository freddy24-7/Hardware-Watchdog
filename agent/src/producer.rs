use reqwest::Client;
use thiserror::Error;

use crate::metrics::MetricsSnapshot;

#[derive(Debug, Error)]
pub enum ProducerError {
    #[error("failed to serialise metrics snapshot: {0}")]
    Serialise(#[from] serde_json::Error),

    #[error("HTTP send failed: {0}")]
    Send(#[from] reqwest::Error),

    #[error("ingest endpoint returned {0}")]
    Status(reqwest::StatusCode),
}

pub struct MetricsProducer {
    client: Client,
    ingest_url: String,
}

impl MetricsProducer {
    pub fn new(ingest_url: String) -> Self {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("failed to build HTTP client");
        Self { client, ingest_url }
    }

    pub async fn send(&self, snapshot: &MetricsSnapshot) -> Result<(), ProducerError> {
        let resp = self.client
            .post(&self.ingest_url)
            .json(snapshot)
            .send()
            .await?;

        if !resp.status().is_success() {
            return Err(ProducerError::Status(resp.status()));
        }

        Ok(())
    }
}
