import pandas as pd


def format_duration(td) -> str:
    """
    Formats a timedelta into a human readable HH:MM:SS string.

    Parameters
    ----------
    td : timedelta or None
        The duration to format.

    Returns
    -------
    str or None
        A string in the format HH:MM:SS, or None if input is None.
    """
    if pd.isnull(td):
        return None

    total_seconds = int(td.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    return f"{hours:02}:{minutes:02}:{seconds:02}"


def build_sessions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Builds a complete timeline of uptime and downtime sessions per production line.

    Each row in the output represents a single continuous period of either
    uptime or downtime for a production line. Together, all rows for a line
    cover the entire observation window without gaps.

    Session types:
    - uptime: the line was operating (between START and STOP)
    - downtime: the line was not operating (between STOP and next START,
                before first START, or after last STOP)

    Handles edge cases:
    - START with no following STOP: uptime extends to last timestamp in dataset
    - STOP with no preceding START: uptime extends from first timestamp in dataset
    - No START and no STOP: single uptime session spanning the entire window
    - Leading downtime: added when line does not start at the beginning of the window
    - Trailing downtime: added when line stops before the end of the window

    Parameters
    ----------
    df : pd.DataFrame
        Cleaned DataFrame from loader.load_data(), containing columns:
        production_line_id, status, timestamp.

    Returns
    -------
    pd.DataFrame
        A DataFrame with columns:
        - production_line_id (str)
        - session_type (str): 'uptime' or 'downtime'
        - start_timestamp (datetime)
        - stop_timestamp (datetime)
        - duration (str): duration in HH:MM:SS format
        - is_complete (bool): False if session boundaries were estimated
                              using dataset time boundaries
    """

    sessions = []

    # Get the dataset time boundaries for handling incomplete sessions
    first_timestamp = df["timestamp"].min()
    last_timestamp = df["timestamp"].max()

    # Process each production line independently
    for line_id, group in df.groupby("production_line_id"):

        # Filter only START and STOP events, ignoring ON heartbeats
        events = group[group["status"].isin(["START", "STOP"])].copy()

        # Handle lines with no START and no STOP events (e.g. gr-np-55)
        # These lines were running throughout the entire observation window
        if events.empty:
            sessions.append({
                "production_line_id": line_id,
                "session_type": "uptime",
                "start_timestamp": first_timestamp,
                "stop_timestamp": last_timestamp,
                "duration": last_timestamp - first_timestamp,
                "is_complete": False
            })
            continue

        # --- Build raw uptime sessions by pairing START and STOP events ---

        line_sessions = []
        current_start = None
        current_start_is_real = False

        for _, row in events.iterrows():

            if row["status"] == "START":
                current_start = row["timestamp"]
                current_start_is_real = True

            elif row["status"] == "STOP":

                if current_start is None:
                    # STOP with no preceding START — line was already running
                    # at the beginning of the dataset
                    current_start = first_timestamp
                    current_start_is_real = False

                stop_timestamp = row["timestamp"]

                line_sessions.append({
                    "production_line_id": line_id,
                    "session_type": "uptime",
                    "start_timestamp": current_start,
                    "stop_timestamp": stop_timestamp,
                    "duration": stop_timestamp - current_start,
                    "is_complete": current_start_is_real
                })

                # Reset for the next session
                current_start = None
                current_start_is_real = False

        # After processing all events, if current_start is set but no STOP followed
        # the line was still running at the end of the dataset
        if current_start is not None:
            line_sessions.append({
                "production_line_id": line_id,
                "session_type": "uptime",
                "start_timestamp": current_start,
                "stop_timestamp": last_timestamp,
                "duration": last_timestamp - current_start,
                "is_complete": False
            })

        # --- Add leading downtime if line did not start at beginning of window ---
        first_session_start = line_sessions[0]["start_timestamp"]
        if first_session_start > first_timestamp:
            sessions.append({
                "production_line_id": line_id,
                "session_type": "downtime",
                "start_timestamp": first_timestamp,
                "stop_timestamp": first_session_start,
                "duration": first_session_start - first_timestamp,
                "is_complete": False
            })

        # --- Add uptime sessions and downtime gaps between them ---
        for i, session in enumerate(line_sessions):
            sessions.append(session)

            # Add downtime gap between this session and the next
            if i < len(line_sessions) - 1:
                next_session = line_sessions[i + 1]
                gap_start = session["stop_timestamp"]
                gap_stop = next_session["start_timestamp"]
                sessions.append({
                    "production_line_id": line_id,
                    "session_type": "downtime",
                    "start_timestamp": gap_start,
                    "stop_timestamp": gap_stop,
                    "duration": gap_stop - gap_start,
                    "is_complete": True
                })

        # --- Add trailing downtime if line stopped before end of window ---
        last_session_stop = line_sessions[-1]["stop_timestamp"]
        if last_session_stop < last_timestamp:
            sessions.append({
                "production_line_id": line_id,
                "session_type": "downtime",
                "start_timestamp": last_session_stop,
                "stop_timestamp": last_timestamp,
                "duration": last_timestamp - last_session_stop,
                "is_complete": False
            })

    # Build the final sessions DataFrame and sort by line and start time
    sessions_df = pd.DataFrame(sessions)
    sessions_df = sessions_df.sort_values(
        ["production_line_id", "start_timestamp"]
    ).reset_index(drop=True)

    # Keep raw timedelta for arithmetic in kpis.py
    sessions_df["duration_raw"] = sessions_df["duration"]

    # Format duration into human readable HH:MM:SS for display purposes
    sessions_df["duration"] = sessions_df["duration"].apply(format_duration)

    return sessions_df