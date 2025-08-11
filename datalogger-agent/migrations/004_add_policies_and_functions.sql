-- Migration 004: Add Policies and Utility Functions
-- Created: 2024-12-27
-- Description: Adds compression policies, utility functions, and additional indexes

-- Add additional indexes for better performance
CREATE INDEX IF NOT EXISTS idx_raw_data_created_at ON raw_data (created_at);
CREATE INDEX IF NOT EXISTS idx_raw_data_device_datapoint_datetime 
ON raw_data (device_id, datapoint, datetime DESC);
CREATE INDEX IF NOT EXISTS idx_raw_data_value_numeric 
ON raw_data (value) WHERE value ~ '^-?[0-9]+\.?[0-9]*$';

-- Add compression policy for older data
SELECT add_compression_policy('raw_data', INTERVAL '7 days');

-- Create health check function
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

-- Create database statistics function
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

-- Create function to clean old data (if needed)
CREATE OR REPLACE FUNCTION cleanup_old_data(retention_days INTEGER DEFAULT 365)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM raw_data 
    WHERE datetime < NOW() - (retention_days || ' days')::INTERVAL;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION cleanup_old_data(INTEGER) IS 'Function to manually clean old data beyond retention period';

-- Record migration
INSERT INTO schema_migrations (version, description, applied_at) 
VALUES ('004', 'Add policies and utility functions', NOW())
ON CONFLICT (version) DO NOTHING;