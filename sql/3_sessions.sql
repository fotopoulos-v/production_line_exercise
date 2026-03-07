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
-- For each event, use LEAD() and LAG() to look at neighbouring events
-- Equivalent to: events = group[group["status"].isin(["START", "STOP"])]
-- -----------------------------------------------------------------------------
events AS (
    SELECT
        e.production_line_id,
        e.status,
        e.timestamp,
        -- Look at the next event's status and timestamp on the same line
        LEAD(e.status)    OVER (PARTITION BY e.production_line_id ORDER BY e.timestamp) AS next_status,
        LEAD(e.timestamp) OVER (PARTITION BY e.production_line_id ORDER BY e.timestamp) AS next_timestamp,
        -- Look at the previous event's status on the same line
        LAG(e.status)     OVER (PARTITION BY e.production_line_id ORDER BY e.timestamp) AS prev_status
    FROM production.raw_events e
    WHERE e.status IN ('START', 'STOP')
),


-- -----------------------------------------------------------------------------
-- Step 3: Build uptime sessions by pairing START with the next STOP
-- Equivalent to: the current_start pairing logic in Python
-- -----------------------------------------------------------------------------
uptime_from_start AS (
    SELECT
        e.production_line_id,
        'uptime'                                        AS session_type,
        e.timestamp                                     AS start_timestamp,
        COALESCE(e.next_timestamp, ow.last_timestamp)   AS stop_timestamp,
        -- Flag as incomplete if there is no following STOP
        CASE WHEN e.next_timestamp IS NULL THEN FALSE ELSE TRUE END AS is_complete
    FROM events e
    CROSS JOIN observation_window ow
    WHERE e.status = 'START'
),


-- -----------------------------------------------------------------------------
-- Step 4: Handle lines where first event is a STOP (no preceding START)
-- The uptime starts from the beginning of the observation window
-- Now safe to filter on prev_status since LAG() was calculated in Step 2
-- -----------------------------------------------------------------------------
uptime_from_window_start AS (
    SELECT
        e.production_line_id,
        'uptime'                AS session_type,
        ow.first_timestamp      AS start_timestamp,
        e.timestamp             AS stop_timestamp,
        FALSE                   AS is_complete
    FROM events e
    CROSS JOIN observation_window ow
    WHERE e.status = 'STOP'
      AND e.prev_status IS NULL
),


-- -----------------------------------------------------------------------------
-- Step 5: Combine all uptime sessions
-- -----------------------------------------------------------------------------
uptime_sessions AS (
    SELECT * FROM uptime_from_start
    UNION ALL
    SELECT * FROM uptime_from_window_start
),


-- -----------------------------------------------------------------------------
-- Step 6: Build downtime gaps between consecutive uptime sessions
-- Equivalent to: the gap calculation loop in Python
-- -----------------------------------------------------------------------------
downtime_gaps AS (
    SELECT
        u.production_line_id,
        'downtime'              AS session_type,
        u.stop_timestamp        AS start_timestamp,
        LEAD(u.start_timestamp) OVER (
            PARTITION BY u.production_line_id
            ORDER BY u.start_timestamp
        )                       AS stop_timestamp,
        TRUE                    AS is_complete
    FROM uptime_sessions u
),


-- -----------------------------------------------------------------------------
-- Step 7: Add leading downtime for lines that did not start
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
-- Step 8: Add trailing downtime for lines that stopped
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
-- Step 9: Handle lines with no START and no STOP events (e.g. gr-np-55)
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
    stop_timestamp - start_timestamp                        AS duration,
    TO_CHAR(stop_timestamp - start_timestamp, 'HH24:MI:SS') AS duration_formatted,
    is_complete
FROM (
    SELECT * FROM uptime_sessions
    UNION ALL
    SELECT * FROM downtime_gaps      WHERE stop_timestamp IS NOT NULL
    UNION ALL
    SELECT * FROM leading_downtime
    UNION ALL
    SELECT * FROM trailing_downtime
    UNION ALL
    SELECT * FROM no_events_lines
) all_sessions
ORDER BY production_line_id, start_timestamp;
