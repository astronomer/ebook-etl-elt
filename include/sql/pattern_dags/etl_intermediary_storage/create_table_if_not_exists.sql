CREATE TABLE IF NOT EXISTS {{ params.schema }}.{{ params.table }} (
    timestamp TIMESTAMP NOT NULL, 
    date DATE NOT NULL,
    day INTEGER NOT NULL, 
    month INTEGER NOT NULL, 
    year INTEGER NOT NULL, 
    temperature_2m NUMERIC NOT NULL, 
    relative_humidity_2m NUMERIC NOT NULL,
    precipitation_probability NUMERIC NOT NULL, 
    last_updated TIMESTAMP NOT NULL, 
    latitude NUMERIC NOT NULL, 
    longitude NUMERIC NOT NULL,
    PRIMARY KEY (timestamp, latitude, longitude)
);
