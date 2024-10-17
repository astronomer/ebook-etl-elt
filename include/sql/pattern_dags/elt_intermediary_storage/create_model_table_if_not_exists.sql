CREATE TABLE IF NOT EXISTS {{ params.schema }}.{{ params.table }} (
    timestamp TIMESTAMP,
    date DATE,
    day INTEGER,
    month INTEGER,
    year INTEGER,
    temperature_2m NUMERIC,
    relative_humidity_2m NUMERIC,
    precipitation_probability NUMERIC,
    last_updated TIMESTAMP,
    latitude NUMERIC,
    longitude NUMERIC
);