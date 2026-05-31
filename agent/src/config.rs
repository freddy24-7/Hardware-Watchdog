use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    /// Full URL of the ingest endpoint, e.g. "https://ingest.up.railway.app/ingest"
    pub ingest_url: String,

    /// How often to collect and publish a snapshot, in seconds
    #[serde(default = "default_interval")]
    pub metrics_interval_secs: u64,
}

fn default_interval() -> u64 {
    5
}
