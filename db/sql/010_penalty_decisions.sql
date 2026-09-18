CREATE TABLE penalty_decisions (
  decision_id BIGSERIAL PRIMARY KEY,
  source_file TEXT NOT NULL,
  driver_text TEXT,
  own_car INT,
  other_car INT,
  competitor TEXT,
  session TEXT,
  incident_time TEXT,
  turn INT,
  fact TEXT,
  infringement TEXT,
  decision TEXT,
  penalty_points INT,
  reason TEXT
);

CREATE UNIQUE INDEX uq_penalty_decisions_source_file ON penalty_decisions (source_file);
