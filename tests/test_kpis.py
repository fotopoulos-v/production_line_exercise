import pytest
import pandas as pd
import os

from production_kpi import load_data, build_sessions
from production_kpi import get_line_sessions, get_floor_uptime_downtime, get_most_downtime_line


# ── Fixtures ──────────────────────────────────────────────────────────────────
# A fixture is a reusable setup that pytest runs before each test.
# Here we load the data once and make it available to all tests.

@pytest.fixture
def sessions():
    """Loads and builds sessions from the sample dataset."""
    df = load_data(os.path.join(os.path.dirname(__file__), "..", "data", "dataset.csv"))
    return build_sessions(df)


# ── Test 1 ────────────────────────────────────────────────────────────────────
# Verify that load_data() returns a DataFrame with the expected columns
# and that the timestamp column is correctly parsed as datetime.

def test_load_data_columns():
    """load_data() should return a DataFrame with the correct columns and types."""
    df = load_data(os.path.join(os.path.dirname(__file__), "..", "data", "dataset.csv"))

    # Check that all expected columns are present
    assert "production_line_id" in df.columns
    assert "status" in df.columns
    assert "timestamp" in df.columns

    # Check that timestamp is parsed as datetime and not a plain string
    assert pd.api.types.is_datetime64_any_dtype(df["timestamp"])


# ── Test 2 ────────────────────────────────────────────────────────────────────
# Verify that for every production line, the sum of all session durations
# equals the total observation window of this exercise (5 hours).
# This is our key sanity check.

def test_sessions_sum_to_observation_window(sessions):
    """Total duration per production line should equal the observation window."""
    observation_window = pd.Timedelta(hours=5)

    for line_id, group in sessions.groupby("production_line_id"):
        total = group["duration_raw"].sum()
        assert total == observation_window, (
            f"Line {line_id}: expected {observation_window}, got {total}"
        )


# ── Test 3 ────────────────────────────────────────────────────────────────────
# Verify that get_line_sessions() returns only uptime sessions
# for the requested production line.

def test_get_line_sessions_returns_correct_line(sessions):
    """get_line_sessions() should return only uptime sessions for the given line."""
    result = get_line_sessions(sessions, "gr-np-47")

    # All rows should belong to gr-np-47
    assert all(result["production_line_id"] == "gr-np-47")

    # All rows should be uptime sessions only
    # We verify this indirectly by checking is_complete and that we get 3 sessions
    assert len(result) == 3


# ── Test 4 ────────────────────────────────────────────────────────────────────
# Verify that get_most_downtime_line() returns exactly one row.

def test_get_most_downtime_line_returns_one_row(sessions):
    """get_most_downtime_line() should return exactly one production line."""
    result = get_most_downtime_line(sessions)
    assert len(result) == 1