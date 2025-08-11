-- Database Initialization Script for Smart Hotel IoT System
-- This script sets up the complete database structure including users and permissions

-- Create database (run as superuser)
-- CREATE DATABASE hotel_iot;
-- \c hotel_iot;

-- Create TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create application user for the datalogger agent
-- DO $$ 
-- BEGIN
--     IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'datalogger_user') THEN
--         CREATE ROLE datalogger_user WITH LOGIN PASSWORD 'secure_password_here';
--     END IF;
-- END
-- $$;

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
SELECT create_hypertable('raw_data', 'datetime', 
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Create deployment_logs table for tracking deployments
CREATE TABLE IF NOT EXISTS deployment_logs (
    id BIGSERIAL PRIMARY KEY,
    deployment_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    module_name VARCHAR(255) NOT NULL,
    version VARCHAR(100) NOT NULL,
    status VARCHAR(50) NOT NULL CHECK (status IN ('success', 'failed', 'rollback', 'in_progress')),
    logs TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create indexes for raw_data table
CREATE INDEX IF NOT EXISTS idx_raw_data_device_id ON raw_data (device_id);
CREATE INDEX IF NOT EXISTS idx_raw_data_datapoint ON raw_data (datapoint);
CREATE INDEX IF NOT EXISTS idx_raw_data_timestamp ON raw_data (timestamp);
CREATE INDEX IF NOT EXISTS idx_raw_data_created_at ON raw_data (created_at);
CREATE INDEX IF NOT EXISTS idx_raw_data_device_datapoint_datetime 
ON raw_data (device_id, datapoint, datetime DESC);
CREATE INDEX IF NOT EXISTS idx_raw_data_value_numeric 
ON raw_data (value) WHERE value ~ '^-?[0-9]+\.?[0-9]*$';

-- Create indexes for deployment_logs table
CREATE INDEX IF NOT EXISTS idx_deployment_logs_module_name ON deployment_logs (module_name);
CREATE INDEX IF NOT EXISTS idx_deployment_logs_status ON deployment_logs (status);
CREATE INDEX IF NOT EXISTS idx_deployment_logs_deployment_time ON deployment_logs (deployment_time);

-- Add table comments
COMMENT ON TABLE raw_data IS 'Time-series data from IAQ sensors stored in TimescaleDB hypertable format';
COMMENT ON TABLE deployment_logs IS 'Log of deployment events for IoT Edge modules';

-- Add column comments for raw_data
COMMENT ON COLUMN raw_data.timestamp IS 'Unix timestamp in seconds';
COMMENT ON COLUMN raw_data.datetime IS 'Timestamp with timezone for time-series partitioning';
COMMENT ON COLUMN raw_data.device_id IS 'Unique identifier for the sensor device';
COMMENT ON COLUMN raw_data.datapoint IS 'Type of measurement (temperature, humidity, co2)';
COMMENT ON COLUMN raw_data.value IS 'Measurement value stored as text for flexibility';
COMMENT ON COLUMN raw_data.created_at IS 'Record insertion timestamp';

-- Add column comments for deployment_logs
COMMENT ON COLUMN deployment_logs.deployment_time IS 'When the deployment was initiated';
COMMENT ON COLUMN deployment_logs.module_name IS 'Name of the IoT Edge module being deployed';
COMMENT ON COLUMN deployment_logs.version IS 'Version of the module being deployed';
COMMENT ON COLUMN deployment_logs.status IS 'Deployment status: success, failed, rollback, in_progress';
COMMENT ON COLUMN deployment_logs.logs IS 'Detailed deployment logs and error messages';

-- Create views for easier data access
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

-- Create view for latest sensor readings per device
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

-- Create continuous aggregates for performance
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

-- Create daily aggregates
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

-- Add refresh policies for continuous aggregates
SELECT add_continuous_aggregate_policy('hourly_sensor_averages',
    start_offset => INTERVAL '1 day',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

SELECT add_continuous_aggregate_policy('daily_sensor_averages',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day');

-- Add compression policy for older data (saves storage space)
SELECT add_compression_policy('raw_data', INTERVAL '7 days');

-- Add retention policy (optional - keeps data for 1 year)
-- SELECT add_retention_policy('raw_data', INTERVAL '1 year');

-- Grant permissions to application user
-- GRANT CONNECT ON DATABASE hotel_iot TO datalogger_user;
-- GRANT USAGE ON SCHEMA public TO datalogger_user;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON raw_data TO datalogger_user;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON deployment_logs TO datalogger_user;
-- GRANT SELECT ON sensor_readings TO datalogger_user;
-- GRANT SELECT ON latest_sensor_readings TO datalogger_user;
-- GRANT SELECT ON hourly_sensor_averages TO datalogger_user;
-- GRANT SELECT ON daily_sensor_averages TO datalogger_user;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO datalogger_user;

-- Insert sample data for testing (optional)
-- INSERT INTO raw_data (timestamp, datetime, device_id, datapoint, value) VALUES
-- (1703635200, '2024-12-27 00:00:00+00', 'room_101', 'temperature', '24.5'),
-- (1703635200, '2024-12-27 00:00:00+00', 'room_101', 'humidity', '55.0'),
-- (1703635200, '2024-12-27 00:00:00+00', 'room_101', 'co2', '488.8');

-- Create function to check database health
CREATE OR REPLACE FUNCTION check_database_health()
RETURNS TABLE(
    component TEXT,
    status TEXT,
    details TEXT
) AS $$
BEGIN
    -- Check TimescaleDB extension
    RETURN QUERY
    SELECT 
        'timescaledb_extension'::TEXT,
        CASE WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') 
             THEN 'healthy'::TEXT 
             ELSE 'error'::TEXT 
        END,
        'TimescaleDB extension status'::TEXT;
    
    -- Check raw_data table
    RETURN QUERY
    SELECT 
        'raw_data_table'::TEXT,
        CASE WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'raw_data') 
             THEN 'healthy'::TEXT 
             ELSE 'error'::TEXT 
        END,
        'Raw data table existence'::TEXT;
    
    -- Check hypertable status
    RETURN QUERY
    SELECT 
        'raw_data_hypertable'::TEXT,
        CASE WHEN EXISTS (SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = 'raw_data') 
             THEN 'healthy'::TEXT 
             ELSE 'error'::TEXT 
        END,
        'Hypertable configuration status'::TEXT;
    
    -- Check recent data
    RETURN QUERY
    SELECT 
        'recent_data'::TEXT,
        CASE WHEN EXISTS (SELECT 1 FROM raw_data WHERE datetime > NOW() - INTERVAL '1 hour') 
             THEN 'healthy'::TEXT 
             ELSE 'warning'::TEXT 
        END,
        'Recent data availability (last hour)'::TEXT;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION check_database_health() IS 'Function to check overall database health status';

-- Create function to get database statistics
CREATE OR REPLACE FUNCTION get_database_stats()
RETURNS TABLE(
    metric TEXT,
    value BIGINT,
    description TEXT
) AS $$
BEGIN
    -- Total records count
    RETURN QUERY
    SELECT 
        'total_records'::TEXT,
        COUNT(*)::BIGINT,
        'Total number of sensor readings'::TEXT
    FROM raw_data;
    
    -- Unique devices count
    RETURN QUERY
    SELECT 
        'unique_devices'::TEXT,
        COUNT(DISTINCT device_id)::BIGINT,
        'Number of unique sensor devices'::TEXT
    FROM raw_data;
    
    -- Data points types count
    RETURN QUERY
    SELECT 
        'datapoint_types'::TEXT,
        COUNT(DISTINCT datapoint)::BIGINT,
        'Number of different measurement types'::TEXT
    FROM raw_data;
    
    -- Records in last 24 hours
    RETURN QUERY
    SELECT 
        'records_last_24h'::TEXT,
        COUNT(*)::BIGINT,
        'Records inserted in the last 24 hours'::TEXT
    FROM raw_data
    WHERE datetime > NOW() - INTERVAL '24 hours';
    
    -- Database size
    RETURN QUERY
    SELECT 
        'database_size_mb'::TEXT,
        (pg_database_size(current_database()) / 1024 / 1024)::BIGINT,
        'Database size in megabytes'::TEXT;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_database_stats() IS 'Function to get database usage statistics';

-- Print initialization completion message
DO $$
BEGIN
    RAISE NOTICE 'Smart Hotel IoT Database initialization completed successfully!';
    RAISE NOTICE 'Tables created: raw_data, deployment_logs';
    RAISE NOTICE 'Views created: sensor_readings, latest_sensor_readings';
    RAISE NOTICE 'Materialized views created: hourly_sensor_averages, daily_sensor_averages';
    RAISE NOTICE 'Functions created: check_database_health(), get_database_stats()';
END $$;