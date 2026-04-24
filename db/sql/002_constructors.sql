CREATE TABLE constructors (
  constructor_id INT PRIMARY KEY,
  name TEXT NOT NULL,
  nationality TEXT,
  code VARCHAR(8)
);

CREATE UNIQUE INDEX uq_constructors_name ON constructors (name);
