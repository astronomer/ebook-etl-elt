CREATE TABLE IF NOT EXISTS {{ params.schema }}.{{ params.table }} (
    raw_data JSONB NOT NULL,
    ingestion_time TIMESTAMPTZ DEFAULT NOW()
);