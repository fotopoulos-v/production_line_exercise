import pandas as pd
from production_kpi.transforms import format_duration


def get_line_sessions(sessions_df: pd.DataFrame, line_id: str) -> pd.DataFrame:
    """
    Returns the uptime sessions for a specific production line.

    Answers Business Question 1:
    For a given production line, returns a table with the start timestamp,
    stop timestamp and duration of each uptime session.
    If any sessions are incomplete (boundaries estimated from observation window),
    a warning message is printed to inform the user.

    Parameters
    ----------
    sessions_df : pd.DataFrame
        The sessions DataFrame returned by transforms.build_sessions().
    line_id : str
        The production line identifier (e.g. 'gr-np-47').

    Returns
    -------
    pd.DataFrame
        A DataFrame with columns:
        - production_line_id (datetime)
        - start_timestamp (datetime)
        - stop_timestamp (datetime)
        - duration (str): uptime duration in HH:MM:SS format
        - is_complete (bool): False if session boundaries were estimated
    """

    # Filter for the requested production line and uptime sessions only
    mask = (
        (sessions_df["production_line_id"] == line_id) &
        (sessions_df["session_type"] == "uptime")
    )
    result = sessions_df[mask][
        ["production_line_id", "start_timestamp", "stop_timestamp", "duration", "is_complete"]
    ].reset_index(drop=True)

    # Warn the user if the line does not exist in the dataset
    if result.empty:
        print(f"Warning: no uptime sessions found for production line '{line_id}'.")
        return result

    # Warn the user if any sessions have estimated boundaries
    if not result["is_complete"].all():
        print(
            f"Warning: production line '{line_id}' has incomplete sessions "
            f"(is_complete = False). "
            f"Some start or stop timestamps were estimated from the observation "
            f"window boundaries and may not reflect real events."
        )

    return result


def get_floor_uptime_downtime(sessions_df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns the total uptime and downtime across the entire production floor.

    Answers Business Question 2:
    Sums all uptime and downtime durations across all production lines
    and returns a summary table.

    Parameters
    ----------
    sessions_df : pd.DataFrame
        The sessions DataFrame returned by transforms.build_sessions().

    Returns
    -------
    pd.DataFrame
        A DataFrame with columns:
        - session_type (str): 'uptime' or 'downtime'
        - total_duration (str): total duration in HH:MM:SS format
    """

    # Sum raw durations grouped by session type across all lines
    summary = (
        sessions_df
        .groupby("session_type")["duration_raw"]
        .sum()
        .reset_index()
    )

    # Format the summed timedeltas for display
    summary["duration_raw"] = summary["duration_raw"].apply(format_duration)
    summary = summary.rename(columns={"duration_raw": "total_duration"})

    return summary


def get_most_downtime_line(sessions_df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns the production line with the most total downtime.

    Answers Business Question 3:
    Sums downtime durations per production line and returns the line
    with the highest total downtime along with the duration.

    Parameters
    ----------
    sessions_df : pd.DataFrame
        The sessions DataFrame returned by transforms.build_sessions().

    Returns
    -------
    pd.DataFrame
        A DataFrame with columns:
        - production_line_id (str)
        - total_downtime (str): total downtime in HH:MM:SS format
    """

    # Filter downtime sessions only and sum per production line
    downtime_df = sessions_df[sessions_df["session_type"] == "downtime"]
    summary = (
        downtime_df
        .groupby("production_line_id")["duration_raw"]
        .sum()
        .reset_index()
    )

    # Find the line with the maximum total downtime
    max_idx = summary["duration_raw"].idxmax()
    result = summary.loc[[max_idx]].reset_index(drop=True)

    # Format for display
    result["duration_raw"] = result["duration_raw"].apply(format_duration)
    result = result.rename(columns={"duration_raw": "total_downtime"})

    return result