CREATE TABLE IF NOT EXISTS {{ params.schema }}.{{ params.out_table }} AS
SELECT
    year,
    month,
    latitude,
    longitude,
    AVG(temperature_2m) AS avg_temperature_2m,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY temperature_2m) AS median_temperature_2m,
    STDDEV(temperature_2m) AS stddev_temperature_2m,
    AVG(relative_humidity_2m) AS avg_relative_humidity_2m,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY relative_humidity_2m) AS median_relative_humidity_2m,
    STDDEV(relative_humidity_2m) AS stddev_relative_humidity_2m,
    AVG(precipitation_probability) AS avg_precipitation_probability,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY precipitation_probability) AS median_precipitation_probability,
    STDDEV(precipitation_probability) AS stddev_precipitation_probability,
    COUNT(*) AS num_records
FROM 
    {{ params.schema }}.{{ params.in_table }}
GROUP BY
    year, month, latitude, longitude;
