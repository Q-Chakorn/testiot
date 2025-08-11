-- Migration 003: Add Views and Continuous Aggregates
-- Created: 2024-12-27
-- Description: Adds views and continuous aggregates for better query performance

-- Create sensor_readings view
CREATE OR REPLACE VIEW sensor_readings AS
SELECT 
    id,
    timestamp,
    datetime,
    device_id,
    datapoint,
    CASE 
        WHEN value ~ '^-?[0-9]+\.?[0-9]*$' THEN value::NUMERIC
        ELSE NULL
    END as numeric_value,
    value as raw_value,
    created_at
FROM raw_data
ORDER BY datetime DESC;

COMMENT ON VIEW sensor_readings IS 'View with numeric conversion for sensor data analysis';

-- Create latest_sensor_readings view
CREATE OR REPLACE VIEW latest_sensor_readings AS
SELECT DISTINCT ON (device_id, datapoint)
    device_id,
    datapoint,
    datetime,
    CASE 
        WHEN value ~ '^-?[0-9]+\.?[0-9]*$' THEN value::NUMERIC
        ELSE NULL
    END as numeric_value,
    value as raw_value
FROM raw_data
ORDER BY device_id, datapoint, datetime DESC;

COMMENT ON VIEW latest_sensor_readings IS 'Latest reading for each sensor datapoint per device';

-- Create hourly continuous aggregate
CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_sensor_averages
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 hour', datetime) AS hour,
    device_id,
    datapoint,
    AVG(CASE WHEN value ~ '^-?[0-9]+\.?[0-9]*$' THEN value::NUMERIC ELSE NULL END) as avg_value,
    MIN(CASE WHEN value ~ '^-?[0-9]+\.?[0-9]*$' THEN value::NUMERIC ELSE NULL END) as min_value,
    MAX(CASE WHEN value ~ '^-?[0-9]+\.?[0-9]*$' THEN value::NUMERIC ELSE NULL END) as max_value,
    COUNT(*) as reading_count
FROM raw_data
WHERE value ~ '^-?[0-9]+\.?[0-9]*$'
GROUP BY hour, device_id, datapoint;

COMMENT ON MATERIALIZED VIEW hourly_sensor_averages IS 'Pre-computed hourly statistics for sensor data';

-- Create daily continuous aggregate
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_sensor_averages
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 day', datetime) AS day,
    device_id,
    datapoint,
    AVG(CASE WHEN value ~ '^-?[0-9]+\.?[0-9]*$' THEN value::NUMERIC ELSE NULL END) as avg_value,
    MIN(CASE WHEN value ~ '^-?[0-9]+\.?[0-9]*$' THEN value::NUMERIC ELSE NULL END) as min_value,
    MAX(CASE WHEN value ~ '^-?[0-9]+\.?[0-9]*$' THEN value::NUMERIC ELSE NULL END) as max_value,
    COUNT(*) as reading_count
FROM raw_data
WHERE value ~ '^-?[0-9]+\.?[0-9]*$'
GROUP BY day, device_id, datapoint;

COMMENT ON MATERIALIZED VIEW daily_sensor_averages IS 'Pre-computed daily statistics for sensor data';

-- Add refresh policies
SELECT add_continuous_aggregate_policy('hourly_sensor_averages',
    start_offset => INTERVAL '1 day',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

SELECT add_continuous_aggregate_policy('daily_sensor_averages',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day');

-- Record migration
INSERT INTO schema_migrations (version, description, applied_at) 
VALUES ('003', 'Add views and continuous aggregates', NOW())
ON CONFLICT (version) DO NOTHING;