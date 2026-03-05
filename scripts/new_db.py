import sqlite3

DB_PATH = "bikeshare_all.db"

# Define your clean schema
schema = """
CREATE TABLE IF NOT EXISTS trips (
    ride_id TEXT,
    rideable_type TEXT,
    started_at TEXT,
    ended_at TEXT,
    start_station_name TEXT,
    start_station_id REAL,
    end_station_name TEXT,
    end_station_id REAL,
    start_lat REAL,
    start_lng REAL,
    end_lat REAL,
    end_lng REAL,
    member_casual TEXT,
    year INTEGER,
    month INTEGER
);
"""

with sqlite3.connect(DB_PATH) as conn:
    conn.execute(schema)
    print("✅ New trips table created.")
