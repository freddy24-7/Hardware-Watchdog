-- Add machine_id to support multi-user deployments.
--
-- machine_id is a stable UUID generated once by the agent on first run and
-- persisted to ~/.hw-watchdog-id. This lets Grafana dashboards filter to a
-- single user's data without requiring accounts or authentication.
--
-- Existing rows get a placeholder so the column can be NOT NULL.
-- The primary key is extended to include machine_id so two different machines
-- that happen to have the same hostname and collect at the same instant do not
-- collide.

ALTER TABLE hw_metrics
    ADD COLUMN IF NOT EXISTS machine_id TEXT NOT NULL DEFAULT 'unknown';

-- Drop the old PK and replace it with one that includes machine_id
ALTER TABLE hw_metrics DROP CONSTRAINT IF EXISTS hw_metrics_pkey;
ALTER TABLE hw_metrics ADD PRIMARY KEY (collected_at, host, machine_id);
