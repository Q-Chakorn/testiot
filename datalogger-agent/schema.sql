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
    value TEXT NOT NULL,  -- This is what the application uses
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (id, datetime)
);

-- Create hypertable for time-series data
SELECT create_hypertable('raw_data', 'datetime', if_not_exists => TRUE);

-- Create the unique constraint that matches the application's ON CONFLICT clause
ALTER TABLE raw_data ADD CONSTRAINT IF NOT EXISTS raw_data_conflict_constraint 
    UNIQUE (datetime, device_id, datapoint);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_raw_data_timestamp ON raw_data (timestamp);
CREATE INDEX IF NOT EXISTS idx_raw_data_device_id ON raw_data (device_id);
CREATE INDEX IF NOT EXISTS idx_raw_data_datapoint ON raw_data (datapoint);
CREATE INDEX IF NOT EXISTS idx_raw_data_created_at ON raw_data (created_at);
CREATE INDEX IF NOT EXISTS idx_raw_data_device_datapoint ON raw_data (device_id, datapoint);
CREATE INDEX IF NOT EXISTS idx_raw_data_datetime_device ON raw_data (datetime, device_id);

-- Add comments for documentation
COMMENT ON TABLE raw_data IS 'Main table for storing IoT sensor data';
COMMENT ON COLUMN raw_data.timestamp IS 'Unix timestamp';
COMMENT ON COLUMN raw_data.datetime IS 'Datetime with timezone (used for partitioning)';
COMMENT ON COLUMN raw_data.device_id IS 'Device identifier';
COMMENT ON COLUMN raw_data.datapoint IS 'Type of measurement (temperature, humidity, co2, etc.)';
COMMENT ON COLUMN raw_data.value IS 'String representation of the sensor value';
COMMENT ON COLUMN raw_data.numeric_value IS 'Numeric representation for calculations';

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

COMMENT ON VIEW sensor_readings IS 'View for easy access to sensor readings with automatic numeric conversion';

-- Create aggregated view for hourly averages
CREATE OR REPLACE VIEW hourly_sensor_averages AS
SELECT 
    time_bucket('1 hour', datetime) as hour,
    device_id,
    datapoint,
    AVG(CASE WHEN value ~ '^[0-9]*\.?[0-9]+$' THEN value::numeric ELSE NULL END) as avg_value,
    MIN(CASE WHEN value ~ '^[0-9]*\.?[0-9]+$' THEN value::numeric ELSE NULL END) as min_value,
    MAX(CASE WHEN value ~ '^[0-9]*\.?[0-9]+$' THEN value::numeric ELSE NULL END) as max_value,
    COUNT(*) as sample_count
FROM raw_data
WHERE value IS NOT NULL
GROUP BY hour, device_id, datapoint
ORDER BY hour DESC, device_id, datapoint;

COMMENT ON VIEW hourly_sensor_averages IS 'Hourly aggregated sensor data averages';

-- Function to update numeric_value automatically
CREATE OR REPLACE FUNCTION update_numeric_value()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.value ~ '^[0-9]*\.?[0-9]+$' THEN
        NEW.numeric_value := NEW.value::numeric;
    ELSE
        NEW.numeric_value := NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger to auto-update numeric_value
DROP TRIGGER IF EXISTS trigger_update_numeric_value ON raw_data;
CREATE TRIGGER trigger_update_numeric_value
    BEFORE INSERT OR UPDATE ON raw_data
    FOR EACH ROW
    EXECUTE FUNCTION update_numeric_value();

-- Grant permissions (if needed)
-- GRANT SELECT, INSERT, UPDATE, DELETE ON raw_data TO datalogger_user;
-- GRANT USAGE ON SEQUENCE raw_data_id_seq TO datalogger_user;

COMMENT ON SCHEMA public IS 'Updated schema for IoT data logging with proper constraints and automatic numeric conversion';
