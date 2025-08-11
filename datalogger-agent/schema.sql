-- TimescaleDB Schema for Smart Hotel IoT Data
-- This schema creates the raw_data table as a hypertable for time-series data

-- Create extension if not exists
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create the main database if it doesn't exist
-- Note: This should be run by a superuser or database owner
-- CREATE DATABASE hotel_iot;

-- Create the raw_data table
CREATE TABLE IF NOT EXISTS raw_data (
    id BIGSERIAL,
    timestamp BIGINT NOT NULL,
    datetime TIMESTAMPTZ NOT NULL,
    device_id VARCHAR(255) NOT NULL,
    datapoint VARCHAR(255) NOT NULL,
    value TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (datetime, device_id, datapoint)
);

-- Convert to hypertable (TimescaleDB specific)
-- This enables time-series optimizations
SELECT create_hypertable('raw_data', 'datetime', 
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_raw_data_device_id ON raw_data (device_id);
CREATE INDEX IF NOT EXISTS idx_raw_data_datapoint ON raw_data (datapoint);
CREATE INDEX IF NOT EXISTS idx_raw_data_timestamp ON raw_data (timestamp);
CREATE INDEX IF NOT EXISTS idx_raw_data_created_at ON raw_data (created_at);

-- Create composite index for common query patterns
CREATE INDEX IF NOT EXISTS idx_raw_data_device_datapoint_datetime 
ON raw_data (device_id, datapoint, datetime DESC);

-- Create index for value queries (useful for numeric data)
CREATE INDEX IF NOT EXISTS idx_raw_data_value_numeric 
ON raw_data (value) WHERE value ~ '^-?[0-9]+\.?[0-9]*$';

-- Add comments for documentation
COMMENT ON TABLE raw_data IS 'Time-series data from IAQ sensors stored in TimescaleDB hypertable format';
COMMENT ON COLUMN raw_data.timestamp IS 'Unix timestamp in seconds';
COMMENT ON COLUMN raw_data.datetime IS 'Timestamp with timezone for time-series partitioning';
COMMENT ON COLUMN raw_data.device_id IS 'Unique identifier for the sensor device';
COMMENT ON COLUMN raw_data.datapoint IS 'Type of measurement (temperature, humidity, co2)';
COMMENT ON COLUMN raw_data.value IS 'Measurement value stored as text for flexibility';
COMMENT ON COLUMN raw_data.created_at IS 'Record insertion timestamp';

-- Create a view for easier data access with proper data types
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

-- Create continuous aggregates for common queries (optional)
-- This creates pre-computed hourly averages for better performance
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
WHERE value ~ '^-?[0-9]+\.?[0-9]*$'  -- Only numeric values
GROUP BY hour, device_id, datapoint;

COMMENT ON MATERIALIZED VIEW hourly_sensor_averages IS 'Pre-computed hourly statistics for sensor data';

-- Create refresh policy for the continuous aggregate
SELECT add_continuous_aggregate_policy('hourly_sensor_averages',
    start_offset => INTERVAL '1 day',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

-- Create retention policy (keeps data for 1 year)
-- Uncomment if you want automatic data retention
-- SELECT add_retention_policy('raw_data', INTERVAL '1 year');

-- Create compression policy for older data (saves storage space)
SELECT add_compression_policy('raw_data', INTERVAL '7 days');

-- Grant permissions (adjust as needed for your setup)
-- GRANT SELECT, INSERT, UPDATE, DELETE ON raw_data TO datalogger_user;
-- GRANT SELECT ON sensor_readings TO datalogger_user;
-- GRANT SELECT ON hourly_sensor_averages TO datalogger_user;