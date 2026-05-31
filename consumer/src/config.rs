use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    /// PostgreSQL connection URL
    pub database_url: String,

    /// TCP port the HTTP server listens on
    #[serde(default = "default_port")]
    pub port: u16,
}

fn default_port() -> u16 {
    8080
}
