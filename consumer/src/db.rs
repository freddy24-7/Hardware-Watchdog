use sqlx::PgPool;
use thiserror::Error;

use crate::handlers::MetricsPayload;

#[derive(Debug, Error)]
pub enum DbError {
    #[error("failed to connect to database: {0}")]
    Connect(#[from] sqlx::Error),

    #[error("upsert failed: {0}")]
    Upsert(sqlx::Error),
}

pub async fn connect(database_url: &str) -> Result<PgPool, DbError> {
    let pool = sqlx::PgPool::connect(database_url).await?;
    Ok(pool)
}

/// Upsert a single metrics row. ON CONFLICT handles agent retries idempotently.
pub async fn upsert(pool: &PgPool, row: &MetricsPayload) -> Result<(), DbError> {
    let collected_at = time::OffsetDateTime::parse(
        &row.collected_at,
        &time::format_description::well_known::Rfc3339,
    )
    .unwrap_or_else(|_| time::OffsetDateTime::now_utc());

    sqlx::query(
        r#"
        INSERT INTO hw_metrics (collected_at, host, machine_id, cpu_pct, memory_pct, disk_read_bytes, disk_write_bytes)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (collected_at, host, machine_id) DO UPDATE SET
            cpu_pct          = EXCLUDED.cpu_pct,
            memory_pct       = EXCLUDED.memory_pct,
            disk_read_bytes  = EXCLUDED.disk_read_bytes,
            disk_write_bytes = EXCLUDED.disk_write_bytes
        "#,
    )
    .bind(collected_at)
    .bind(&row.host)
    .bind(&row.machine_id)
    .bind(row.cpu_pct)
    .bind(row.memory_pct)
    .bind(row.disk_read_bytes as i64)
    .bind(row.disk_write_bytes as i64)
    .execute(pool)
    .await
    .map_err(DbError::Upsert)?;

    Ok(())
}
