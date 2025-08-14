#!/bin/bash
set -e

echo "Starting database initialization..."

# Wait for PostgreSQL to be ready
until pg_isready -h localhost -p 5432 -U postgres; do
  echo "Waiting for PostgreSQL to be ready..."
  sleep 2
done

echo "PostgreSQL is ready. Initializing database schema..."

# Run schema initialization
psql -v ON_ERROR_STOP=1 --username postgres --dbname hotel_iot <<-EOSQL
    -- Enable TimescaleDB extension
    CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
    
    -- Create the main raw_data table with proper structure
    CREATE TABLE IF NOT EXISTS raw_data (
        id SERIAL,
        timestamp INTEGER NOT NULL,
        datetime TIMESTAMPTZ NOT NULL,
        device_id TEXT NOT NULL,
        datapoint TEXT NOT NULL,
        numeric_value DOUBLE PRECISION,
        raw_value TEXT,
        value TEXT NOT NULL DEFAULT '',
        created_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (id, datetime)
    );

    -- Create hypertable for time-series data
    SELECT create_hypertable('raw_data', 'datetime', if_not_exists => TRUE);

    -- Create the unique constraint that matches the application's ON CONFLICT clause
    ALTER TABLE raw_data DROP CONSTRAINT IF EXISTS raw_data_conflict_constraint;
    ALTER TABLE raw_data ADD CONSTRAINT raw_data_conflict_constraint 
        UNIQUE (datetime, device_id, datapoint);

    -- Create indexes for better performance
    CREATE INDEX IF NOT EXISTS idx_raw_data_timestamp ON raw_data (timestamp);
    CREATE INDEX IF NOT EXISTS idx_raw_data_device_id ON raw_data (device_id);
    CREATE INDEX IF NOT EXISTS idx_raw_data_datapoint ON raw_data (datapoint);
    CREATE INDEX IF NOT EXISTS idx_raw_data_created_at ON raw_data (created_at);
    CREATE INDEX IF NOT EXISTS idx_raw_data_device_datapoint ON raw_data (device_id, datapoint);

    -- Create view for easier data access
    CREATE OR REPLACE VIEW sensor_readings AS
    SELECT 
        id,
        timestamp,
        datetime,
        device_id,
        datapoint,
        CASE 
            WHEN value ~ '^[0-9]*\.?[0-9]+$' THEN value::numeric 
            ELSE NULL 
        END as numeric_value,
        value as raw_value,
        created_at
    FROM raw_data
    ORDER BY datetime DESC;

    -- Function to update numeric_value automatically
    CREATE OR REPLACE FUNCTION update_numeric_value()
    RETURNS TRIGGER AS \$func\$
    BEGIN
        IF NEW.value ~ '^[0-9]*\.?[0-9]+\$' THEN
            NEW.numeric_value := NEW.value::numeric;
        ELSE
            NEW.numeric_value := NULL;
        END IF;
        RETURN NEW;
    END;
    \$func\$ LANGUAGE plpgsql;

    -- Create trigger to auto-update numeric_value
    DROP TRIGGER IF EXISTS trigger_update_numeric_value ON raw_data;
    CREATE TRIGGER trigger_update_numeric_value
        BEFORE INSERT OR UPDATE ON raw_data
        FOR EACH ROW
        EXECUTE FUNCTION update_numeric_value();

    -- Success message
    SELECT 'Database initialized successfully!' as status;

EOSQL

echo "Database initialization completed successfully!"
