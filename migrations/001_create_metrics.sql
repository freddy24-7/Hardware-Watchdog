-- hw_metrics: one row per agent collection tick.
--
-- Primary key design: (collected_at, host) composite key rather than a
-- surrogate integer. This gives Grafana's time-range queries a natural
-- clustering key and makes upserts idempotent — if the agent republishes
-- the same snapshot (at-least-once Kafka delivery), the ON CONFLICT clause
-- updates in place instead of duplicating rows.

CREATE TABLE IF NOT EXISTS hw_metrics (
    -- Timestamp of when the agent collected the sample (not insertion time).
    -- TIMESTAMPTZ stores timezone-aware instants; always insert in UTC.
    collected_at      TIMESTAMPTZ     NOT NULL,

    -- Hostname of the machine running the agent; allows multi-host deployments
    -- without schema changes.
    host              TEXT            NOT NULL,

    -- CPU utilisation averaged across all logical cores, 0.0–100.0.
    cpu_pct           DOUBLE PRECISION NOT NULL,

    -- Resident memory utilisation as a percentage of total physical RAM, 0.0–100.0.
    memory_pct        DOUBLE PRECISION NOT NULL,

    -- Raw bytes read from all block devices since last snapshot.
    disk_read_bytes   BIGINT          NOT NULL,

    -- Raw bytes written to all block devices since last snapshot.
    disk_write_bytes  BIGINT          NOT NULL,

    PRIMARY KEY (collected_at, host)
);
