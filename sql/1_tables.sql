-- =============================================================================
-- 1_tables.sql
-- Creates the schema and raw events table for the production floor data.
--
-- Run this file first before any other SQL files.
-- =============================================================================


-- Create the production schema if it does not already exist
CREATE SCHEMA IF NOT EXISTS production;


-- -----------------------------------------------------------------------------
-- Table: production.raw_events
-- Stores the raw status updates from all production lines.
--
-- Columns:
--   production_line_id : unique identifier of the production line
--   status             : status of the production line
--                        ON    = line is operating normally (heartbeat)
--                        START = line was initiated
--                        STOP  = line was terminated
--   timestamp          : exact timestamp of the status update
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS production.raw_events (
    production_line_id  VARCHAR(50)                 NOT NULL,
    status              VARCHAR(10)                 NOT NULL,
    timestamp           TIMESTAMP WITHOUT TIME ZONE NOT NULL,

    -- Ensure status only accepts valid values
    CONSTRAINT chk_status CHECK (status IN ('ON', 'START', 'STOP'))
);


-- Index on production_line_id and timestamp for faster filtering and sorting
CREATE INDEX IF NOT EXISTS idx_raw_events_line_timestamp
    ON production.raw_events (production_line_id, timestamp);
