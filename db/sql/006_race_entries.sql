CREATE TABLE race_entries (
  race_entry_id BIGSERIAL PRIMARY KEY,
  race_id INT NOT NULL REFERENCES races (race_id) ON DELETE CASCADE,
  driver_id INT NOT NULL REFERENCES drivers (driver_id),
  constructor_id INT NOT NULL REFERENCES constructors (constructor_id),
  car_number INT,
  grid_position INT
);

CREATE UNIQUE INDEX uq_race_entries_race_driver ON race_entries (race_id, driver_id);
CREATE INDEX ix_race_entries_race ON race_entries (race_id);
