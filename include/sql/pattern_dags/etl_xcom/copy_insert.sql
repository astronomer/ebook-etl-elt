CREATE TEMP TABLE tmp_weather_data AS 
SELECT * FROM {schema}.{table} LIMIT 0;

COPY tmp_weather_data 
(temperature_2m, relative_humidity_2m, precipitation_probability, timestamp, date, day, month, year, last_updated, latitude, longitude)
FROM STDIN WITH CSV HEADER;

INSERT INTO {schema}.{table} 
(temperature_2m, relative_humidity_2m, precipitation_probability, timestamp, date, day, month, year, last_updated, latitude, longitude)
SELECT 
    temperature_2m, 
    relative_humidity_2m, 
    precipitation_probability, 
    timestamp, 
    date, 
    day, 
    month, 
    year, 
    last_updated, 
    latitude, 
    longitude
FROM tmp_weather_data
ON CONFLICT (timestamp, latitude, longitude) 
DO UPDATE SET 
    temperature_2m = EXCLUDED.temperature_2m,
    relative_humidity_2m = EXCLUDED.relative_humidity_2m,
    precipitation_probability = EXCLUDED.precipitation_probability,
    date = EXCLUDED.date,
    day = EXCLUDED.day,
    month = EXCLUDED.month,
    year = EXCLUDED.year,
    last_updated = EXCLUDED.last_updated,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude;

