CREATE TABLE races (
  race_id INT PRIMARY KEY,
  season_year INT NOT NULL REFERENCES seasons (year),
  round INT NOT NULL CHECK (round > 0),
  race_name TEXT NOT NULL,
  circuit_id INT NOT NULL REFERENCES circuits (circuit_id),
  race_datetime_utc TIMESTAMP NOT NULL
);

CREATE UNIQUE INDEX uq_races_season_round ON races (season_year, round);
CREATE INDEX ix_races_season ON races (season_year);
