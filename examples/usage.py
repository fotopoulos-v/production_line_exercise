import sys
import os
import argparse

from production_kpi import load_data, build_sessions
from production_kpi import get_line_sessions, get_floor_uptime_downtime, get_most_downtime_line


# ── Argument Parsing ──────────────────────────────────────────────────────────
# Allows the user to pass the production line ID and data path as command line
# arguments instead of hardcoding them in the script.
# If no arguments are provided, sensible defaults are used.

parser = argparse.ArgumentParser(
    description="Production Line KPI Calculator"
)
parser.add_argument(
    "--line-id",
    type=str,
    default="gr-np-47",
    help="Production line ID for Business Question 1 (default: gr-np-47)"
)
parser.add_argument(
    "--data-path",
    type=str,
    default=os.path.join(os.path.dirname(__file__), "..", "data", "dataset.csv"),
    help="Path to the CSV data file (default: data/dataset.csv)"
)
args = parser.parse_args()


# ── Load and transform data ───────────────────────────────────────────────────

print("Loading data...")
df = load_data(args.data_path)

print("Building sessions...")
sessions = build_sessions(df)


# ── Business Question 1 ───────────────────────────────────────────────────────
# For the given production line, show all uptime sessions with their
# start timestamp, stop timestamp and duration

print(f"\n{'='*60}")
print(f"Business Question 1: Uptime sessions for '{args.line_id}'")
print(f"{'='*60}")
print(get_line_sessions(sessions, args.line_id).to_string(index=False))


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