-- Indexes for Grafana's primary access pattern: time-range scans.
--
-- Grafana always queries with a WHERE $__timeFilter(collected_at) clause,
-- which translates to collected_at BETWEEN <start> AND <end>. The primary
-- key (collected_at, host) already provides a B-tree index that satisfies
-- this scan, but we add a dedicated single-column index on collected_at to
-- support queries that do NOT filter by host, which avoids an index scan
-- over the composite key's wider rows.
--
-- The partial index on host is intentionally omitted — with a single agent
-- host, the filter selectivity is low and the index overhead is not worth it.
-- Add it if this schema is extended to multiple hosts.

CREATE INDEX IF NOT EXISTS idx_hw_metrics_collected_at
    ON hw_metrics (collected_at DESC);

-- Covering index for the exact query pattern Grafana emits:
-- SELECT collected_at, cpu_pct FROM hw_metrics WHERE collected_at BETWEEN ...
-- INCLUDE columns avoid a heap fetch for the two most-queried metric columns.
CREATE INDEX IF NOT EXISTS idx_hw_metrics_cpu
    ON hw_metrics (collected_at DESC)
    INCLUDE (cpu_pct);

CREATE INDEX IF NOT EXISTS idx_hw_metrics_memory
    ON hw_metrics (collected_at DESC)
    INCLUDE (memory_pct);
