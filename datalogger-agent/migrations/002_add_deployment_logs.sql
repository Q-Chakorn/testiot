-- Migration 002: Add Deployment Logs Table
-- Created: 2024-12-27
-- Description: Adds deployment_logs table for tracking IoT Edge module deployments

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

-- Create indexes for deployment_logs
CREATE INDEX IF NOT EXISTS idx_deployment_logs_module_name ON deployment_logs (module_name);
CREATE INDEX IF NOT EXISTS idx_deployment_logs_status ON deployment_logs (status);
CREATE INDEX IF NOT EXISTS idx_deployment_logs_deployment_time ON deployment_logs (deployment_time);

-- Add table comments
COMMENT ON TABLE deployment_logs IS 'Log of deployment events for IoT Edge modules';
COMMENT ON COLUMN deployment_logs.deployment_time IS 'When the deployment was initiated';
COMMENT ON COLUMN deployment_logs.module_name IS 'Name of the IoT Edge module being deployed';
COMMENT ON COLUMN deployment_logs.version IS 'Version of the module being deployed';
COMMENT ON COLUMN deployment_logs.status IS 'Deployment status: success, failed, rollback, in_progress';
COMMENT ON COLUMN deployment_logs.logs IS 'Detailed deployment logs and error messages';

-- Record migration
INSERT INTO schema_migrations (version, description, applied_at) 
VALUES ('002', 'Add deployment logs table', NOW())
ON CONFLICT (version) DO NOTHING;