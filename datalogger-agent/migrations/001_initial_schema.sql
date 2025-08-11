-- Migration 001: Initial Schema Creation
-- Created: 2024-12-27
-- Description: Creates the initial database schema for Smart Hotel IoT system

-- Create TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

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

-- Convert to hypertable
SELECT create_hypertable('raw_data', 'datetime', 
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Create basic indexes
CREATE INDEX IF NOT EXISTS idx_raw_data_device_id ON raw_data (device_id);
CREATE INDEX IF NOT EXISTS idx_raw_data_datapoint ON raw_data (datapoint);
CREATE INDEX IF NOT EXISTS idx_raw_data_timestamp ON raw_data (timestamp);

-- Add table comments
COMMENT ON TABLE raw_data IS 'Time-series data from IAQ sensors stored in TimescaleDB hypertable format';

-- Record migration
INSERT INTO schema_migrations (version, description, applied_at) 
VALUES ('001', 'Initial schema creation', NOW())
ON CONFLICT (version) DO NOTHING;