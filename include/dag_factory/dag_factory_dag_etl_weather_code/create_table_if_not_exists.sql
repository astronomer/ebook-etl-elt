CREATE TABLE IF NOT EXISTS {{ params.schema_name }}.{{ params.table_name }} (
    date DATE,
    day INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    weather_code INTEGER NOT NULL,
    last_updated TIMESTAMP WITH TIME ZONE,
    latitude NUMERIC NOT NULL,
    longitude NUMERIC NOT NULL,
    elevation NUMERIC NOT NULL,
    utc_offset_seconds INTEGER NOT NULL,
    timezone TEXT NOT NULL,
    timezone_abbreviation TEXT NOT NULL,
    PRIMARY KEY (date, latitude, longitude)
);