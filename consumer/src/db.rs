use sqlx::PgPool;
use thiserror::Error;

use crate::consumer::MetricsRow;

#[derive(Debug, Error)]
pub enum DbError {
    #[error("failed to connect to database: {0}")]
    Connect(#[from] sqlx::Error),

    #[error("batch upsert failed: {0}")]
    Upsert(sqlx::Error),
}

pub async fn connect(database_url: &str) -> Result<PgPool, DbError> {
    let pool = PgPool::connect(database_url).await?;
    Ok(pool)
}

/// Batch upsert all rows using unnest — one round-trip regardless of batch size.
///
/// ON CONFLICT handles at-least-once redelivery: if the consumer restarts and
/// replays a batch, identical (collected_at, host) rows are overwritten in place
/// rather than duplicated.
pub async fn upsert_batch(pool: &PgPool, rows: &[MetricsRow]) -> Result<(), DbError> {
    if rows.is_empty() {
        return Ok(());
    }

    // Decompose the slice of structs into parallel vecs for unnest binding.
    // This is more efficient than individual INSERTs and keeps the query
    // compile-time checkable via a single sqlx::query! call.
    let collected_ats: Vec<time::OffsetDateTime> = rows.iter().map(|r| r.collected_at).collect();
    let hosts: Vec<&str> = rows.iter().map(|r| r.host.as_str()).collect();
    let cpu_pcts: Vec<f64> = rows.iter().map(|r| r.cpu_pct).collect();
    let memory_pcts: Vec<f64> = rows.iter().map(|r| r.memory_pct).collect();
    let disk_read_bytes: Vec<i64> = rows.iter().map(|r| r.disk_read_bytes).collect();
    let disk_write_bytes: Vec<i64> = rows.iter().map(|r| r.disk_write_bytes).collect();

    // sqlx::query (not query!) used here because UNNEST with array parameters
    // cannot be type-checked at compile time without a live DB connection.
    // SQLX_OFFLINE mode does not support this query pattern.
    sqlx::query(
        r#"
        INSERT INTO hw_metrics (collected_at, host, cpu_pct, memory_pct, disk_read_bytes, disk_write_bytes)
        SELECT * FROM UNNEST(
            $1::timestamptz[],
            $2::text[],
            $3::float8[],
            $4::float8[],
            $5::bigint[],
            $6::bigint[]
        )
        ON CONFLICT (collected_at, host) DO UPDATE SET
            cpu_pct          = EXCLUDED.cpu_pct,
            memory_pct       = EXCLUDED.memory_pct,
            disk_read_bytes  = EXCLUDED.disk_read_bytes,
            disk_write_bytes = EXCLUDED.disk_write_bytes
        "#,
    )
    .bind(&collected_ats as &[time::OffsetDateTime])
    .bind(&hosts as &[&str])
    .bind(&cpu_pcts as &[f64])
    .bind(&memory_pcts as &[f64])
    .bind(&disk_read_bytes as &[i64])
    .bind(&disk_write_bytes as &[i64])
    .execute(pool)
    .await
    .map_err(DbError::Upsert)?;

    Ok(())
}
