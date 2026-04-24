-- Seed one real race dataset (Ergast raceId=18, 2008 Australian GP)
-- Assumes PostgreSQL and local CSV files available at E:/Gridline/Dataset/*.csv

BEGIN;

CREATE TEMP TABLE stg_races (
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name TEXT,
  date DATE,
  time TEXT,
  url TEXT,
  fp1_date TEXT,
  fp1_time TEXT,
  fp2_date TEXT,
  fp2_time TEXT,
  fp3_date TEXT,
  fp3_time TEXT,
  quali_date TEXT,
  quali_time TEXT,
  sprint_date TEXT,
  sprint_time TEXT
);

CREATE TEMP TABLE stg_drivers (
  driverId INT,
  driverRef TEXT,
  number TEXT,
  code TEXT,
  forename TEXT,
  surname TEXT,
  dob DATE,
  nationality TEXT,
  url TEXT
);

CREATE TEMP TABLE stg_constructors (
  constructorId INT,
  constructorRef TEXT,
  name TEXT,
  nationality TEXT,
  url TEXT
);

CREATE TEMP TABLE stg_circuits (
  circuitId INT,
  circuitRef TEXT,
  name TEXT,
  location TEXT,
  country TEXT,
  lat NUMERIC(9, 6),
  lng NUMERIC(9, 6),
  alt TEXT,
  url TEXT
);

CREATE TEMP TABLE stg_results (
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number TEXT,
  grid INT,
  position TEXT,
  positionText TEXT,
  positionOrder INT,
  points NUMERIC,
  laps INT,
  time TEXT,
  milliseconds INT,
  fastestLap INT,
  rank TEXT,
  fastestLapTime TEXT,
  fastestLapSpeed TEXT,
  statusId INT
);

CREATE TEMP TABLE stg_status (
  statusId INT,
  status TEXT
);

COPY stg_races FROM 'E:/Gridline/Dataset/races.csv' WITH (FORMAT csv, HEADER true, NULL '\N');
COPY stg_drivers FROM 'E:/Gridline/Dataset/drivers.csv' WITH (FORMAT csv, HEADER true, NULL '\N');
COPY stg_constructors FROM 'E:/Gridline/Dataset/constructors.csv' WITH (FORMAT csv, HEADER true, NULL '\N');
COPY stg_circuits FROM 'E:/Gridline/Dataset/circuits.csv' WITH (FORMAT csv, HEADER true, NULL '\N');
COPY stg_results FROM 'E:/Gridline/Dataset/results.csv' WITH (FORMAT csv, HEADER true, NULL '\N');
COPY stg_status FROM 'E:/Gridline/Dataset/status.csv' WITH (FORMAT csv, HEADER true, NULL '\N');

INSERT INTO seasons (year)
SELECT DISTINCT r.year
FROM stg_races r
WHERE r.raceId = 18
ON CONFLICT (year) DO NOTHING;

INSERT INTO circuits (circuit_id, name, country, location, lat, lng)
SELECT c.circuitId, c.name, c.country, c.location, c.lat, c.lng
FROM stg_circuits c
JOIN stg_races r ON r.circuitId = c.circuitId
WHERE r.raceId = 18
ON CONFLICT (circuit_id) DO NOTHING;

INSERT INTO constructors (constructor_id, name, nationality, code)
SELECT DISTINCT c.constructorId, c.name, c.nationality, UPPER(LEFT(c.constructorRef, 8))
FROM stg_constructors c
JOIN stg_results rs ON rs.constructorId = c.constructorId
WHERE rs.raceId = 18
ON CONFLICT (constructor_id) DO NOTHING;

INSERT INTO drivers (
  driver_id,
  driver_ref,
  permanent_number,
  code,
  forename,
  surname,
  dob,
  nationality,
  wiki_url,
  full_name
)
SELECT DISTINCT
  d.driverId,
  d.driverRef,
  CASE WHEN d.number IS NULL THEN NULL ELSE d.number::INT END,
  d.code,
  d.forename,
  d.surname,
  d.dob,
  d.nationality,
  d.url,
  d.forename || ' ' || d.surname
FROM stg_drivers d
JOIN stg_results rs ON rs.driverId = d.driverId
WHERE rs.raceId = 18
ON CONFLICT (driver_id) DO NOTHING;

INSERT INTO races (race_id, season_year, round, race_name, circuit_id, race_datetime_utc)
SELECT
  r.raceId,
  r.year,
  r.round,
  r.name,
  r.circuitId,
  (r.date::TEXT || ' ' || COALESCE(r.time, '00:00:00'))::TIMESTAMP
FROM stg_races r
WHERE r.raceId = 18
ON CONFLICT (race_id) DO NOTHING;

WITH seed_results AS (
  SELECT
    rs.raceId AS race_id,
    rs.driverId AS driver_id,
    rs.constructorId AS constructor_id,
    CASE WHEN rs.number IS NULL THEN NULL ELSE rs.number::INT END AS car_number,
    rs.grid AS grid_position,
    rs.positionOrder AS finish_position,
    rs.laps,
    rs.milliseconds AS race_time_ms,
    rs.fastestLap AS fastest_lap_number,
    CASE
      WHEN rs.fastestLapTime IS NULL THEN NULL
      ELSE (
        split_part(rs.fastestLapTime, ':', 1)::INT * 60000
        + split_part(split_part(rs.fastestLapTime, ':', 2), '.', 1)::INT * 1000
        + split_part(split_part(rs.fastestLapTime, ':', 2), '.', 2)::INT
      )
    END AS fastest_lap_ms,
    UPPER(REPLACE(COALESCE(st.status, 'UNKNOWN'), ' ', '_')) AS status
  FROM stg_results rs
  LEFT JOIN stg_status st ON st.statusId = rs.statusId
  WHERE rs.raceId = 18
)
INSERT INTO race_entries (race_id, driver_id, constructor_id, car_number, grid_position)
SELECT DISTINCT
  sr.race_id,
  sr.driver_id,
  sr.constructor_id,
  sr.car_number,
  sr.grid_position
FROM seed_results sr
ON CONFLICT (race_id, driver_id) DO NOTHING;

WITH seed_results AS (
  SELECT
    rs.raceId AS race_id,
    rs.driverId AS driver_id,
    rs.positionOrder AS finish_position,
    rs.laps,
    CASE
      WHEN rs.fastestLapTime IS NULL THEN NULL
      ELSE (
        split_part(rs.fastestLapTime, ':', 1)::INT * 60000
        + split_part(split_part(rs.fastestLapTime, ':', 2), '.', 1)::INT * 1000
        + split_part(split_part(rs.fastestLapTime, ':', 2), '.', 2)::INT
      )
    END AS fastest_lap_ms,
    CASE
      WHEN st.status = 'Finished' THEN 'FINISHED'
      ELSE UPPER(REPLACE(COALESCE(st.status, 'UNKNOWN'), ' ', '_'))
    END AS status
  FROM stg_results rs
  LEFT JOIN stg_status st ON st.statusId = rs.statusId
  WHERE rs.raceId = 18
)
INSERT INTO live_timing_snapshots (
  race_id,
  race_entry_id,
  captured_at,
  lap,
  position,
  gap_to_leader_ms,
  interval_ahead_ms,
  last_lap_ms,
  best_lap_ms,
  pit_stops,
  status
)
SELECT
  sr.race_id,
  re.race_entry_id,
  r.race_datetime_utc + INTERVAL '2 hour',
  sr.laps,
  sr.finish_position,
  CASE WHEN sr.finish_position = 1 THEN 0 ELSE NULL END,
  NULL,
  NULL,
  sr.fastest_lap_ms,
  0,
  sr.status
FROM seed_results sr
JOIN race_entries re
  ON re.race_id = sr.race_id
 AND re.driver_id = sr.driver_id
JOIN races r ON r.race_id = sr.race_id;

WITH seed_results AS (
  SELECT
    rs.raceId AS race_id,
    rs.driverId AS driver_id,
    rs.fastestLap AS lap_number,
    CASE
      WHEN rs.fastestLapTime IS NULL THEN NULL
      ELSE (
        split_part(rs.fastestLapTime, ':', 1)::INT * 60000
        + split_part(split_part(rs.fastestLapTime, ':', 2), '.', 1)::INT * 1000
        + split_part(split_part(rs.fastestLapTime, ':', 2), '.', 2)::INT
      )
    END AS lap_time_ms
  FROM stg_results rs
  WHERE rs.raceId = 18
)
INSERT INTO lap_times (race_id, race_entry_id, lap_number, lap_time_ms)
SELECT
  sr.race_id,
  re.race_entry_id,
  sr.lap_number,
  sr.lap_time_ms
FROM seed_results sr
JOIN race_entries re
  ON re.race_id = sr.race_id
 AND re.driver_id = sr.driver_id
WHERE sr.lap_number IS NOT NULL
  AND sr.lap_time_ms IS NOT NULL
ON CONFLICT (race_id, race_entry_id, lap_number) DO NOTHING;

COMMIT;
