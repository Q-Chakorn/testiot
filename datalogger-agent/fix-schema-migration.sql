-- Migration script to fix schema issues
-- Run this to fix existing database

-- Step 1: Add value column if not exists
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'raw_data' AND column_name = 'value'
    ) THEN
        ALTER TABLE raw_data ADD COLUMN value TEXT;
    END IF;
END $$;

-- Step 2: Update existing raw_value to value if needed
UPDATE raw_data SET value = raw_value WHERE value IS NULL AND raw_value IS NOT NULL;

-- Step 3: Drop existing conflicting constraint
ALTER TABLE raw_data DROP CONSTRAINT IF EXISTS raw_data_unique_datetime_device_datapoint;

-- Step 4: Add the correct constraint for ON CONFLICT
ALTER TABLE raw_data ADD CONSTRAINT IF NOT EXISTS raw_data_conflict_constraint 
    UNIQUE (datetime, device_id, datapoint);

-- Step 5: Make value NOT NULL for new inserts (optional)
-- ALTER TABLE raw_data ALTER COLUMN value SET NOT NULL;

-- Step 6: Create trigger if not exists
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

DROP TRIGGER IF EXISTS trigger_update_numeric_value ON raw_data;
CREATE TRIGGER trigger_update_numeric_value
    BEFORE INSERT OR UPDATE ON raw_data
    FOR EACH ROW
    EXECUTE FUNCTION update_numeric_value();

-- Success message
SELECT 'Schema migration completed successfully!' as status;
