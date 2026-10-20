ALTER TABLE penalty_decisions
  ADD COLUMN race_year INT,
  ADD COLUMN race_event TEXT,
  ADD COLUMN telemetry_snapshot JSONB;
