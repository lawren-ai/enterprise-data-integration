-- =====================================================
-- Database Setup Script
-- Creates the enterprise data warehouse database
-- =====================================================

-- Note: Run this script as a superuser (e.g., postgres)
-- psql -U postgres -f sql/setup/01_create_database.sql

-- Terminate existing connections (if any)
SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname = 'enterprise_dw'
  AND pid <> pg_backend_pid();

-- Drop database if it exists (for clean setup)
DROP DATABASE IF EXISTS enterprise_dw;

-- Create the database
CREATE DATABASE enterprise_dw
    WITH 
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.UTF-8'
    LC_CTYPE = 'en_US.UTF-8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;

COMMENT ON DATABASE enterprise_dw IS 'Enterprise Data Warehouse - Customer Transaction Analytics';

-- Connect to the new database
\c enterprise_dw

-- Create schemas for organization (optional but recommended)
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS quality;

COMMENT ON SCHEMA staging IS 'Raw data landing zone';
COMMENT ON SCHEMA warehouse IS 'Dimensional model (star schema)';
COMMENT ON SCHEMA quality IS 'Data quality metadata and rules';

-- Grant permissions (adjust as needed)
-- GRANT ALL PRIVILEGES ON DATABASE enterprise_dw TO your_user;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO your_user;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA warehouse TO your_user;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA quality TO your_user;