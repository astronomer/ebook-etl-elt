-- Create a temporary table for weather code data
CREATE TEMP TABLE tmp_wind_data AS 
SELECT * FROM {schema}.{table} LIMIT 0;

-- Copy data into the temporary table (columns from the weather code data)
COPY tmp_wind_data 
(date, day, month, year, wind, last_updated, latitude, longitude, elevation, utc_offset_seconds, timezone, timezone_abbreviation)
FROM STDIN WITH CSV HEADER;

-- Insert data into the target table
INSERT INTO {schema}.{table} 
(date, day, month, year, wind, last_updated, latitude, longitude, elevation, utc_offset_seconds, timezone, timezone_abbreviation)
SELECT 
    date, 
    day,
    month,
    year,
    wind, 
    last_updated, 
    latitude, 
    longitude, 
    elevation, 
    utc_offset_seconds, 
    timezone, 
    timezone_abbreviation
FROM tmp_wind_data
ON CONFLICT (date, latitude, longitude) 
DO UPDATE SET
    day = EXCLUDED.day,
    month = EXCLUDED.month,
    year = EXCLUDED.year,
    wind = EXCLUDED.wind,
    last_updated = EXCLUDED.last_updated,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    elevation = EXCLUDED.elevation,
    utc_offset_seconds = EXCLUDED.utc_offset_seconds,
    timezone = EXCLUDED.timezone,
    timezone_abbreviation = EXCLUDED.timezone_abbreviation;
