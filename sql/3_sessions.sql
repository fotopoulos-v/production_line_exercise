-- =============================================================================
-- 3_sessions.sql
-- Creates a view that builds the complete timeline of uptime and downtime
-- sessions per production line.
--
-- This view is the SQL equivalent of transforms.build_sessions() in Python.
--
-- Each row represents a single continuous period of either uptime or downtime
-- for a production line. Together, all rows for a line cover the entire
-- observation window without gaps.
--
-- Prerequisites: 1_tables.sql must be run first.
-- =============================================================================


CREATE OR REPLACE VIEW production.sessions AS

WITH

-- -----------------------------------------------------------------------------
-- Step 1: Get the observation window boundaries
-- These are used to handle incomplete sessions (no START or no STOP)
-- Equivalent to: first_timestamp and last_timestamp in Python
-- -----------------------------------------------------------------------------
observation_window AS (
    SELECT
        MIN(timestamp) AS first_timestamp,
        MAX(timestamp) AS last_timestamp
    FROM production.raw_events
),


-- -----------------------------------------------------------------------------
-- Step 2: Filter only START and STOP events, ignoring ON heartbeats
-- For each event, use LEAD() to look ahead at the next event on the same line
-- Equivalent to: events = group[group["status"].isin(["START", "STOP"])]
-- -----------------------------------------------------------------------------
events AS (
    SELECT
        e.production_line_id,
        e.status,
        e.timestamp,
        -- Look at the next event's status and timestamp on the same line
        LEAD(e.status)    OVER (PARTITION BY e.production_line_id ORDER BY e.timestamp) AS next_status,
        LEAD(e.timestamp) OVER (PARTITION BY e.production_line_id ORDER BY e.timestamp) AS next_timestamp
    FROM production.raw_events e
    WHERE e.status IN ('START', 'STOP')
),


-- -----------------------------------------------------------------------------
-- Step 3: Build uptime sessions by pairing START with the next STOP
-- Also handle the case where a STOP has no preceding START
-- Equivalent to: the current_start pairing logic in Python
-- -----------------------------------------------------------------------------
uptime_sessions AS (
    SELECT
        e.production_line_id,
        'uptime'                                    AS session_type,
        e.timestamp                                 AS start_timestamp,
        COALESCE(e.next_timestamp, ow.last_timestamp) AS stop_timestamp,
        -- Flag as incomplete if there is no following STOP
        CASE WHEN e.next_timestamp IS NULL THEN FALSE ELSE TRUE END AS is_complete
    FROM events e
    CROSS JOIN observation_window ow
    WHERE e.status = 'START'

    UNION ALL

    -- Handle lines where first event is a STOP (no preceding START)
    -- The uptime starts from the beginning of the observation window
    SELECT
        e.production_line_id,
        'uptime'                AS session_type,
        ow.first_timestamp      AS start_timestamp,
        e.timestamp             AS stop_timestamp,
        FALSE                   AS is_complete
    FROM events e
    CROSS JOIN observation_window ow
    WHERE e.status = 'STOP'
      AND LAG(e.status) OVER (PARTITION BY e.production_line_id ORDER BY e.timestamp) IS NULL
),


-- -----------------------------------------------------------------------------
-- Step 4: Build downtime gaps between consecutive uptime sessions
-- Equivalent to: the gap calculation loop in Python
-- -----------------------------------------------------------------------------
downtime_gaps AS (
    SELECT
        u.production_line_id,
        'downtime'              AS session_type,
        u.stop_timestamp        AS start_timestamp,
        -- The next uptime session's start is this downtime's end
        LEAD(u.start_timestamp) OVER (
            PARTITION BY u.production_line_id
            ORDER BY u.start_timestamp
        )                       AS stop_timestamp,
        TRUE                    AS is_complete
    FROM uptime_sessions u
),


-- -----------------------------------------------------------------------------
-- Step 5: Add leading downtime for lines that did not start
-- at the beginning of the observation window
-- Equivalent to: the leading downtime logic in Python
-- -----------------------------------------------------------------------------
leading_downtime AS (
    SELECT
        u.production_line_id,
        'downtime'              AS session_type,
        ow.first_timestamp      AS start_timestamp,
        MIN(u.start_timestamp)  AS stop_timestamp,
        FALSE                   AS is_complete
    FROM uptime_sessions u
    CROSS JOIN observation_window ow
    GROUP BY u.production_line_id, ow.first_timestamp
    HAVING MIN(u.start_timestamp) > ow.first_timestamp
),


-- -----------------------------------------------------------------------------
-- Step 6: Add trailing downtime for lines that stopped
-- before the end of the observation window
-- Equivalent to: the trailing downtime logic in Python
-- -----------------------------------------------------------------------------
trailing_downtime AS (
    SELECT
        u.production_line_id,
        'downtime'              AS session_type,
        MAX(u.stop_timestamp)   AS start_timestamp,
        ow.last_timestamp       AS stop_timestamp,
        FALSE                   AS is_complete
    FROM uptime_sessions u
    CROSS JOIN observation_window ow
    GROUP BY u.production_line_id, ow.last_timestamp
    HAVING MAX(u.stop_timestamp) < ow.last_timestamp
),


-- -----------------------------------------------------------------------------
-- Step 7: Handle lines with no START and no STOP events (e.g. gr-np-55)
-- These lines were running throughout the entire observation window
-- -----------------------------------------------------------------------------
no_events_lines AS (
    SELECT
        r.production_line_id,
        'uptime'                AS session_type,
        ow.first_timestamp      AS start_timestamp,
        ow.last_timestamp       AS stop_timestamp,
        FALSE                   AS is_complete
    FROM production.raw_events r
    CROSS JOIN observation_window ow
    GROUP BY r.production_line_id, ow.first_timestamp, ow.last_timestamp
    HAVING SUM(CASE WHEN r.status IN ('START', 'STOP') THEN 1 ELSE 0 END) = 0
)


-- -----------------------------------------------------------------------------
-- Final: Union all session types together and calculate duration
-- Sort by production line and start timestamp for readability
-- -----------------------------------------------------------------------------
SELECT
    production_line_id,
    session_type,
    start_timestamp,
    stop_timestamp,
    -- Calculate duration as an interval
    stop_timestamp - start_timestamp                                    AS duration,
    -- Format duration as HH:MM:SS for readability
    TO_CHAR(stop_timestamp - start_timestamp, 'HH24:MI:SS')            AS duration_formatted,
    is_complete
FROM (
    SELECT * FROM uptime_sessions
    UNION ALL
    -- Only include downtime gaps that have a valid stop timestamp
    SELECT * FROM downtime_gaps      WHERE stop_timestamp IS NOT NULL
    UNION ALL
    SELECT * FROM leading_downtime
    UNION ALL
    SELECT * FROM trailing_downtime
    UNION ALL
    SELECT * FROM no_events_lines
) all_sessions
ORDER BY production_line_id, start_timestamp;
