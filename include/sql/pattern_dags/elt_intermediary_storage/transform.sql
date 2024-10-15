CREATE TABLE IF NOT EXISTS {{ params.schema }}.{{ params.out_table }} AS
SELECT
    time_series.time AS timestamp,
    time_series.time::date AS date,
    EXTRACT(DAY FROM time_series.time) AS day,
    EXTRACT(MONTH FROM time_series.time) AS month,
    EXTRACT(YEAR FROM time_series.time) AS year,
    time_series.temperature_2m,
    time_series.relative_humidity_2m,
    time_series.precipitation_probability,
    NOW() AS last_updated,
    (raw_data->>'latitude')::NUMERIC AS latitude,
    (raw_data->>'longitude')::NUMERIC AS longitude
FROM
    {{ params.schema }}.{{ params.in_table }},
    LATERAL (
        SELECT
            to_timestamp(time_elem.value #>> '{}', 'YYYY-MM-DD"T"HH24:MI') AS time,
            (temperature_elem.value #>> '{}')::NUMERIC AS temperature_2m,
            (humidity_elem.value #>> '{}')::NUMERIC AS relative_humidity_2m,
            (precipitation_elem.value #>> '{}')::NUMERIC AS precipitation_probability
        FROM
            jsonb_array_elements(raw_data->'hourly'->'time') WITH ORDINALITY AS time_elem(value, idx)
            INNER JOIN jsonb_array_elements(raw_data->'hourly'->'temperature_2m') WITH ORDINALITY AS temperature_elem(value, idx2)
                ON idx = idx2
            INNER JOIN jsonb_array_elements(raw_data->'hourly'->'relative_humidity_2m') WITH ORDINALITY AS humidity_elem(value, idx3)
                ON idx = idx3
            INNER JOIN jsonb_array_elements(raw_data->'hourly'->'precipitation_probability') WITH ORDINALITY AS precipitation_elem(value, idx4)
                ON idx = idx4
    ) AS time_series;
