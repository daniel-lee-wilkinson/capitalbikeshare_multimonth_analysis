import zipfile
import sqlite3
import pandas as pd
from pathlib import Path
import re

# Paths
ZIP_DIR = Path("input_data/monthly_zips/")
DB_PATH = Path("bikeshare_all.db")
TABLE_NAME = "trips"

# Target schema columns
required_cols = [
    "ride_id", "rideable_type", "started_at", "ended_at",
    "start_station_name", "start_station_id",
    "end_station_name", "end_station_id",
    "start_lat", "start_lng", "end_lat", "end_lng",
    "member_casual", "year", "month"
]

# Known alternate header names → unified column names
column_mapping = {
    "Start date": "started_at",
    "End date": "ended_at",
    "Start station": "start_station_name",
    "Start station number": "start_station_id",
    "End station": "end_station_name",
    "End station number": "end_station_id",
    "Member type": "member_casual",
    "Duration": None,
    "Bike number": None
}

# Regex for matching filenames like "202004-capitalbikeshare-tripdata.zip"
zip_pattern = re.compile(r"(\d{4})(\d{2})-capitalbikeshare-tripdata\.zip", re.IGNORECASE)

# Connect to SQLite
with sqlite3.connect(DB_PATH) as conn:
    for zip_file in ZIP_DIR.glob("*.zip"):
        match = zip_pattern.match(zip_file.name)
        if not match:
            print(f"Skipping unrecognised filename: {zip_file.name}")
            continue

        year, month = map(int, match.groups())
        print(f"\n--- Processing: {zip_file.name} ({year}-{month}) ---")

        # Check if this year-month already exists in DB
        try:
            result = conn.execute(
                f"SELECT 1 FROM {TABLE_NAME} WHERE year = ? AND month = ? LIMIT 1;",
                (year, month)
            ).fetchone()
            if result:
                print(f"Skipping {year}-{month} — data already exists.")
                continue
        except sqlite3.OperationalError:
            print(f"Table '{TABLE_NAME}' does not exist yet — will be created.")

        with zipfile.ZipFile(zip_file) as z:
            csv_file_name = next((f for f in z.namelist() if f.endswith(".csv")), None)
            if not csv_file_name:
                print(f"No CSV found in {zip_file.name}, skipping.")
                continue

            with z.open(csv_file_name) as f:
                try:
                    df = pd.read_csv(f)
                except Exception as e:
                    print(f"Failed to read {csv_file_name}: {e}")
                    continue

            # Rename known alternate column names
            df = df.rename(columns={k: v for k, v in column_mapping.items() if v is not None})

            # Drop explicitly unwanted legacy columns
            df = df.drop(columns=[k for k, v in column_mapping.items() if v is None and k in df.columns])

            # Generate synthetic ride_id for legacy files
            if "ride_id" not in df.columns:
                df["ride_id"] = "legacy_" + df.index.astype(str)

            # Fill missing modern-only fields
            for col in required_cols:
                if col not in df.columns:
                    df[col] = pd.NA

            # Clean and parse timestamps
            df["started_at"] = pd.to_datetime(df["started_at"], errors="coerce")
            df["ended_at"] = pd.to_datetime(df["ended_at"], errors="coerce")
            df = df.dropna(subset=["started_at", "ended_at"])

            # Add year and month
            df["year"] = year
            df["month"] = month

            # Keep only relevant columns
            df = df[required_cols]

            # Write to database
            try:
                df.to_sql(TABLE_NAME, conn, if_exists="append", index=False)
                print(f"✓ Added {len(df)} rows to {TABLE_NAME} for {year}-{month}.")
            except Exception as e:
                print(f"Failed to write to database for {zip_file.name}: {e}")
