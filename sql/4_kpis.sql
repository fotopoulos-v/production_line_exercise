-- =============================================================================
-- 4_kpis.sql
-- Creates views that answer the 3 business questions.
--
-- This file is the SQL equivalent of kpis.py in Python.
--
-- Prerequisites:
--   1_tables.sql must be run first to create the schema and raw table.
--   3_sessions.sql must be run first to create the sessions view.
-- =============================================================================


-- =============================================================================
-- Business Question 1
-- For production line "gr-np-47", return a table with the start timestamp,
-- stop timestamp and duration of each uptime session.
--
-- Equivalent to: get_line_sessions(sessions, "gr-np-47") in Python
-- =============================================================================

CREATE OR REPLACE VIEW production.kpi_line_sessions AS
SELECT
    production_line_id,
    start_timestamp,
    stop_timestamp,
    duration_formatted  AS duration,
    is_complete
FROM production.sessions
WHERE session_type = 'uptime'
ORDER BY start_timestamp;


-- =============================================================================
-- Business Question 2
-- What is the total uptime and downtime of the whole production floor?
--
-- Equivalent to: get_floor_uptime_downtime(sessions) in Python
-- =============================================================================

CREATE OR REPLACE VIEW production.kpi_floor_uptime_downtime AS
SELECT
    session_type,
    -- Sum all raw intervals per session type across all lines
    SUM(duration)                                           AS total_duration,
    -- Format the summed interval as HH:MM:SS for readability
    TO_CHAR(SUM(duration), 'HH24:MI:SS')                   AS total_duration_formatted
FROM production.sessions
GROUP BY session_type
ORDER BY session_type;


-- =============================================================================
-- Business Question 3
-- Which production line had the most downtime and how much was it?
--
-- Equivalent to: get_most_downtime_line(sessions) in Python
-- =============================================================================

CREATE OR REPLACE VIEW production.kpi_most_downtime_line AS
WITH downtime_per_line AS (
    -- Sum downtime duration per production line
    SELECT
        production_line_id,
        SUM(duration)                                       AS total_downtime,
        TO_CHAR(SUM(duration), 'HH24:MI:SS')               AS total_downtime_formatted
    FROM production.sessions
    WHERE session_type = 'downtime'
    GROUP BY production_line_id
)
-- Return only the line with the maximum total downtime
SELECT
    production_line_id,
    total_downtime,
    total_downtime_formatted
FROM downtime_per_line
WHERE total_downtime = (SELECT MAX(total_downtime) FROM downtime_per_line);
