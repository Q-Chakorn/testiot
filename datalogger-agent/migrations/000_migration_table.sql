-- Migration 000: Create Migration Tracking Table
-- Created: 2024-12-27
-- Description: Creates the schema_migrations table to track applied migrations

-- Create schema_migrations table to track applied migrations
CREATE TABLE IF NOT EXISTS schema_migrations (
    version VARCHAR(10) PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    checksum VARCHAR(64)
);

-- Create index for faster lookups
CREATE INDEX IF NOT EXISTS idx_schema_migrations_applied_at ON schema_migrations (applied_at);

-- Add table comment
COMMENT ON TABLE schema_migrations IS 'Tracks database schema migrations that have been applied';
COMMENT ON COLUMN schema_migrations.version IS 'Migration version number (e.g., 001, 002)';
COMMENT ON COLUMN schema_migrations.description IS 'Human-readable description of the migration';
COMMENT ON COLUMN schema_migrations.applied_at IS 'When the migration was applied';
COMMENT ON COLUMN schema_migrations.checksum IS 'Optional checksum for migration file integrity';

-- Insert this migration record
INSERT INTO schema_migrations (version, description, applied_at) 
VALUES ('000', 'Create migration tracking table', NOW())
ON CONFLICT (version) DO NOTHING;