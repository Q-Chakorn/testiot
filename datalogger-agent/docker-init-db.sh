#!/bin/bash
# Docker Database Initialization Script
# This script runs when the TimescaleDB container starts for the first time

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting Smart Hotel IoT Database Initialization...${NC}"

# Wait for PostgreSQL to be ready
echo -e "${YELLOW}Waiting for PostgreSQL to be ready...${NC}"
until pg_isready -h localhost -p 5432 -U postgres; do
    echo "PostgreSQL is not ready yet, waiting..."
    sleep 2
done

echo -e "${GREEN}PostgreSQL is ready!${NC}"

# Create the hotel_iot database if it doesn't exist
echo -e "${YELLOW}Creating hotel_iot database...${NC}"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE hotel_iot'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'hotel_iot')\gexec
EOSQL

# Switch to hotel_iot database and run initialization
echo -e "${YELLOW}Initializing hotel_iot database schema...${NC}"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "hotel_iot" <<-EOSQL
    -- Create TimescaleDB extension
    CREATE EXTENSION IF NOT EXISTS timescaledb;
    
    -- Create application user
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'datalogger_user') THEN
            CREATE ROLE datalogger_user WITH LOGIN PASSWORD '${DATALOGGER_PASSWORD:-secure_password}';
        END IF;
    END
    \$\$;
    
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
    
    -- Create deployment_logs table
    CREATE TABLE IF NOT EXISTS deployment_logs (
        id BIGSERIAL PRIMARY KEY,
        deployment_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        module_name VARCHAR(255) NOT NULL,
        version VARCHAR(100) NOT NULL,
        status VARCHAR(50) NOT NULL CHECK (status IN ('success', 'failed', 'rollback', 'in_progress')),
        logs TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    
    -- Create indexes
    CREATE INDEX IF NOT EXISTS idx_raw_data_device_id ON raw_data (device_id);
    CREATE INDEX IF NOT EXISTS idx_raw_data_datapoint ON raw_data (datapoint);
    CREATE INDEX IF NOT EXISTS idx_raw_data_timestamp ON raw_data (timestamp);
    CREATE INDEX IF NOT EXISTS idx_raw_data_created_at ON raw_data (created_at);
    CREATE INDEX IF NOT EXISTS idx_raw_data_device_datapoint_datetime 
    ON raw_data (device_id, datapoint, datetime DESC);
    CREATE INDEX IF NOT EXISTS idx_raw_data_value_numeric 
    ON raw_data (value) WHERE value ~ '^-?[0-9]+\.?[0-9]*\$';
    
    CREATE INDEX IF NOT EXISTS idx_deployment_logs_module_name ON deployment_logs (module_name);
    CREATE INDEX IF NOT EXISTS idx_deployment_logs_status ON deployment_logs (status);
    CREATE INDEX IF NOT EXISTS idx_deployment_logs_deployment_time ON deployment_logs (deployment_time);
    
    -- Grant permissions
    GRANT CONNECT ON DATABASE hotel_iot TO datalogger_user;
    GRANT USAGE ON SCHEMA public TO datalogger_user;
    GRANT SELECT, INSERT, UPDATE, DELETE ON raw_data TO datalogger_user;
    GRANT SELECT, INSERT, UPDATE, DELETE ON deployment_logs TO datalogger_user;
    GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO datalogger_user;
    
    -- Insert sample data for testing
    INSERT INTO raw_data (timestamp, datetime, device_id, datapoint, value) VALUES
    (1703635200, '2024-12-27 00:00:00+00', 'room_101', 'temperature', '24.5'),
    (1703635200, '2024-12-27 00:00:00+00', 'room_101', 'humidity', '55.0'),
    (1703635200, '2024-12-27 00:00:00+00', 'room_101', 'co2', '488.8'),
    (1703635260, '2024-12-27 00:01:00+00', 'room_101', 'temperature', '24.3'),
    (1703635260, '2024-12-27 00:01:00+00', 'room_101', 'humidity', '54.8'),
    (1703635260, '2024-12-27 00:01:00+00', 'room_101', 'co2', '490.2')
    ON CONFLICT (datetime, device_id, datapoint) DO NOTHING;
    
    -- Log successful initialization
    INSERT INTO deployment_logs (module_name, version, status, logs) VALUES
    ('database_init', '1.0.0', 'success', 'Database initialized successfully with sample data');
EOSQL

echo -e "${GREEN}Database initialization completed successfully!${NC}"

# Run health check
echo -e "${YELLOW}Running database health check...${NC}"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "hotel_iot" <<-EOSQL
    -- Check TimescaleDB extension
    SELECT 
        CASE WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') 
             THEN 'TimescaleDB extension: ✓ Installed'
             ELSE 'TimescaleDB extension: ✗ Missing'
        END as status;
    
    -- Check hypertable
    SELECT 
        CASE WHEN EXISTS (SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = 'raw_data') 
             THEN 'Raw data hypertable: ✓ Created'
             ELSE 'Raw data hypertable: ✗ Missing'
        END as status;
    
    -- Check sample data
    SELECT 
        'Sample data records: ' || COUNT(*) as status
    FROM raw_data;
    
    -- Check deployment logs
    SELECT 
        'Deployment log entries: ' || COUNT(*) as status
    FROM deployment_logs;
EOSQL

echo -e "${GREEN}Health check completed!${NC}"
echo -e "${GREEN}Smart Hotel IoT Database is ready for use.${NC}"