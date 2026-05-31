use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use crate::db;

/// Wire format sent by the agent — must match MetricsSnapshot in agent/src/metrics.rs
#[derive(Debug, Deserialize)]
pub struct MetricsPayload {
    pub collected_at: String,
    pub host: String,
    pub machine_id: String,
    pub cpu_pct: f64,
    pub memory_pct: f64,
    pub disk_read_bytes: u64,
    pub disk_write_bytes: u64,
}

#[derive(Serialize)]
struct IngestResponse {
    status: &'static str,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
}

pub async fn ingest(
    State(pool): State<PgPool>,
    Json(payload): Json<MetricsPayload>,
) -> impl IntoResponse {
    match db::upsert(&pool, &payload).await {
        Ok(()) => {
            tracing::info!(
                machine_id = %payload.machine_id,
                host = %payload.host,
                cpu = %payload.cpu_pct,
                "metric ingested"
            );
            (StatusCode::OK, Json(IngestResponse { status: "ok" }))
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to write metric to postgres");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(IngestResponse { status: "error" }))
        }
    }
}

pub async fn health(State(pool): State<PgPool>) -> impl IntoResponse {
    match sqlx::query("SELECT 1").execute(&pool).await {
        Ok(_) => (StatusCode::OK, Json(HealthResponse { status: "ok" })),
        Err(_) => (StatusCode::SERVICE_UNAVAILABLE, Json(HealthResponse { status: "degraded" })),
    }
}
