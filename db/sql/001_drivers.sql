CREATE TABLE drivers (
  driver_id INT PRIMARY KEY,
  driver_ref TEXT,
  permanent_number INT NULL,
  code TEXT,
  forename TEXT,
  surname TEXT,
  dob DATE,
  nationality TEXT,
  wiki_url TEXT,
  full_name TEXT
);
