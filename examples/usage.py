import sys
import os

# Add the project root to the Python path so the package can be found
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from production_kpi import load_data, build_sessions
from production_kpi import get_line_sessions, get_floor_uptime_downtime, get_most_downtime_line


# ── Configuration ─────────────────────────────────────────────────────────────

# Path to the dataset relative to the project root
DATA_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "dataset.csv")

# Production line to use for Business Question 1
LINE_ID = "gr-np-47"


# ── Load and transform data ───────────────────────────────────────────────────

print("Loading data...")
df = load_data(DATA_PATH)

print("Building sessions...")
sessions = build_sessions(df)


# ── Business Question 1 ───────────────────────────────────────────────────────
# For production line "gr-np-47", show all uptime sessions with their
# start timestamp, stop timestamp and duration

print(f"\n{'='*60}")
print(f"Business Question 1: Uptime sessions for '{LINE_ID}'")
print(f"{'='*60}")
print(get_line_sessions(sessions, LINE_ID).to_string(index=False))


# ── Business Question 2 ───────────────────────────────────────────────────────
# What is the total uptime and downtime of the whole production floor?

print(f"\n{'='*60}")
print("Business Question 2: Total uptime and downtime of the production floor")
print(f"{'='*60}")
print(get_floor_uptime_downtime(sessions).to_string(index=False))


# ── Business Question 3 ───────────────────────────────────────────────────────
# Which production line had the most downtime and how much was it?

print(f"\n{'='*60}")
print("Business Question 3: Production line with the most downtime")
print(f"{'='*60}")
print(get_most_downtime_line(sessions).to_string(index=False))