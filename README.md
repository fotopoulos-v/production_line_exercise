# Production Line KPI Package

A Python package and SQL implementation for calculating production floor KPIs
from production line event data.

---

## Business Questions Answered

1. For production line `gr-np-47`, show all uptime sessions with their start
   timestamp, stop timestamp and duration.
2. What is the total uptime and downtime of the whole production floor?
3. Which production line had the most downtime and how much was it?

---

## Project Structure
```
production_line_exercise/
│
├── production_kpi/               # Python package
│   ├── __init__.py               # Exposes user facing functions
│   ├── loader.py                 # Loads and cleans the raw data from a CSV file
│   ├── transforms.py             # Builds the uptime/downtime session timeline
│   └── kpis.py                   # Functions that answer the 3 business questions
│
├── sql/                          # SQL implementation for DWH
│   ├── README.md                 # DWH implementation guide
│   ├── 1_tables.sql              # Creates the production schema and raw events table
│   ├── 2_sample_data.sql         # (Optional) Inserts sample data for testing
│   ├── 3_sessions.sql            # Creates the sessions view
│   └── 4_kpis.sql                # Creates the 3 KPI views
│
├── tests/                        # Unit tests
│   └── test_kpis.py              # Tests for loader, transforms and kpis functions
│
├── data/                         # Sample dataset
│   └── dataset.csv               # Raw production line event data
│
├── examples/                     # Usage example
│   └── usage.py                  # Demonstrates how to use the package end to end
│
├── README.md                     # Project overview and how to use guide
├── pyproject.toml                # Package installation configuration
├── requirements.txt              # Python dependencies
└── .gitignore                    # Files and folders excluded from version control
```

---

## Python Package

### Installation

Open a terminal and run the following commands one by one:

**1. Clone the repository to your local machine:**
```bash
git clone https://github.com/fotopoulos-v/production_line_exercise.git
```

**2. Navigate into the project folder:**
```bash
cd production_line_exercise
```

**3. Install the package and its dependencies:**
```bash
pip install -e .
```

This installs the `production_kpi` package in editable mode, meaning any
changes to the source files are immediately reflected without reinstalling.
`pytest` can be installed separately for running the tests:
```bash
pip install pytest
```

> **Note on virtual environments:** In production settings it is recommended
> to create a virtual environment before installing dependencies in order to
> avoid conflicts with other Python projects on your system:
> ```bash
> python3 -m venv venv
> source venv/bin/activate        # On Windows: venv\Scripts\activate
> pip install -e .
> pip install pytest
> ```
> For this package we skip this step since the only dependencies are `pandas`
> and `pytest`, and to keep the setup as simple as possible.

---

### Quick Start

To see all three business questions answered with the sample dataset, run the
following command from the project root folder in your terminal:
```bash
python3 examples/usage.py
```

By default Business Question 1 uses production line `gr-np-47`. To use a
different line pass the `--line-id` argument:
```bash
python3 examples/usage.py --line-id gr-np-08
```

---

### Step by Step Usage

Instead of the Quick Start section where the user can obtain the answers of all
three business questions at once, the answers can be obtained through a simple
three step flow. All commands below are run in a Python script or interactive
Python session from the project root folder.

**Step 1 — Import the package functions:**
```python
from production_kpi import load_data, build_sessions
from production_kpi import (
    get_line_sessions,
    get_floor_uptime_downtime,
    get_most_downtime_line
)
```

**Step 2 — Load the raw data and build the session timeline:**
```python
# Load the CSV file and parse timestamps
# Returns a cleaned DataFrame sorted by production_line_id and timestamp
df = load_data("data/dataset.csv")

# Build the complete uptime and downtime session timeline
# Returns a DataFrame where each row is a continuous uptime or downtime period
# for a production line, covering the entire observation window without gaps
sessions = build_sessions(df)
```

**Step 3 — Answer the business questions:**
```python
# Business Question 1
# Returns a table of uptime sessions for the given production line
# with start timestamp, stop timestamp, duration and completeness flag
# Prints a warning if any sessions have estimated boundaries
get_line_sessions(sessions, "gr-np-47")

# Business Question 2
# Returns a summary table with total uptime and total downtime
# aggregated across all production lines on the floor
get_floor_uptime_downtime(sessions)

# Business Question 3
# Returns the production line with the highest total downtime
# and the corresponding duration
get_most_downtime_line(sessions)
```

---

### Running the Tests

From the project root folder in your terminal, run:
```bash
pytest tests/ -v
```

Each test will be listed with a `PASSED` or `FAILED` status. All 4 tests
should pass on the provided sample dataset.

---

## SQL Implementation

For instructions on how to implement the SQL files into your DWH, refer to:

👉 [sql/README.md](sql/README.md)