CREATE TABLE lap_times (
  lap_time_id BIGSERIAL PRIMARY KEY,
  race_id INT NOT NULL REFERENCES races (race_id) ON DELETE CASCADE,
  race_entry_id BIGINT NOT NULL REFERENCES race_entries (race_entry_id) ON DELETE CASCADE,
  lap_number INT NOT NULL CHECK (lap_number > 0),
  lap_time_ms INT NOT NULL CHECK (lap_time_ms > 0),
  sector1_ms INT,
  sector2_ms INT,
  sector3_ms INT
);

CREATE UNIQUE INDEX uq_lap_times_race_entry_lap ON lap_times (race_id, race_entry_id, lap_number);
CREATE INDEX ix_lap_times_race ON lap_times (race_id);
