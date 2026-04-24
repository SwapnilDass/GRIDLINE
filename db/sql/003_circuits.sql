CREATE TABLE circuits (
  circuit_id INT PRIMARY KEY,
  name TEXT NOT NULL,
  country TEXT,
  location TEXT,
  lat NUMERIC(9, 6),
  lng NUMERIC(9, 6)
);

CREATE UNIQUE INDEX uq_circuits_name_country ON circuits (name, country);
