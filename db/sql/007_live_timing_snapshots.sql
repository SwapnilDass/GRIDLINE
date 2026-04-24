CREATE TABLE live_timing_snapshots (
  snapshot_id BIGSERIAL PRIMARY KEY,
  race_id INT NOT NULL REFERENCES races (race_id) ON DELETE CASCADE,
  race_entry_id BIGINT NOT NULL REFERENCES race_entries (race_entry_id) ON DELETE CASCADE,
  captured_at TIMESTAMP NOT NULL,
  lap INT CHECK (lap >= 0),
  position INT CHECK (position > 0),
  gap_to_leader_ms INT,
  interval_ahead_ms INT,
  last_lap_ms INT,
  best_lap_ms INT,
  pit_stops INT DEFAULT 0 CHECK (pit_stops >= 0),
  status TEXT NOT NULL
);

CREATE INDEX ix_live_timing_race_time ON live_timing_snapshots (race_id, captured_at DESC);
CREATE INDEX ix_live_timing_race_position ON live_timing_snapshots (race_id, position);
CREATE INDEX ix_live_timing_entry_time ON live_timing_snapshots (race_entry_id, captured_at DESC);
