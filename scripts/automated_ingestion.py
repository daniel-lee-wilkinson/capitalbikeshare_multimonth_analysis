import zipfile
import sqlite3
import pandas as pd
from pathlib import Path
import re
import requests
from bs4 import BeautifulSoup

# --- CONFIG ---
BASE_URL = "https://s3.amazonaws.com/capitalbikeshare-data/index.html"
ZIP_DIR = Path("input_data/monthly_zips/")
ZIP_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = Path("bikeshare_all.db")
TABLE_NAME = "trips"

# --- Get available file links from Capital Bikeshare archive page ---
def fetch_file_links():
    print("Checking for new Capital Bikeshare data...")
    response = requests.get(BASE_URL)
    soup = BeautifulSoup(response.text, "xml")
    return [link.text for link in soup.find_all("Key") if link.text.endswith(".zip")]

# --- Download new ZIPs ---
def download_new_zips(file_links):
    base_prefix = "https://s3.amazonaws.com/capitalbikeshare-data/"
    downloaded = []
    for link in file_links:
        zip_name = Path(link).name  # use filename only
        zip_path = ZIP_DIR / zip_name
        if not zip_path.exists():
            print(f"Downloading: {zip_name}")
            r = requests.get(base_prefix + link)
            with open(zip_path, "wb") as f:
                f.write(r.content)
            downloaded.append(zip_path)
        else:
            print(f"Already downloaded: {zip_name}")
    return downloaded

# --- Process ZIPs and ingest data into SQLite ---
def process_and_ingest_zips(zip_paths):
    with sqlite3.connect(DB_PATH) as conn:
        for zip_file in zip_paths:
            with zipfile.ZipFile(zip_file) as z:
                csv_name = next((f for f in z.namelist() if f.endswith(".csv")), None)
                if not csv_name:
                    print(f"No CSV found in {zip_file.name}, skipping.")
                    continue

                with z.open(csv_name) as f:
                    try:
                        df = pd.read_csv(f)
                    except Exception as e:
                        print(f"Failed to read {csv_name}: {e}")
                        continue

            # Flexible header harmonisation
            def match_col(possibilities):
                for col in df.columns:
                    if any(p.lower() in col.lower() for p in possibilities):
                        return col
                return None

            col_map = {
                "started_at": match_col(["Start date", "started_at"]),
                "ended_at": match_col(["End date", "ended_at"]),
                "start_station_name": match_col(["Start station", "start_station_name"]),
                "end_station_name": match_col(["End station", "end_station_name"]),
                "start_station_id": match_col(["Start station number", "start_station_id"]),
                "end_station_id": match_col(["End station number", "end_station_id"]),
                "member_casual": match_col(["Member type", "member_casual"]),
            }

            df = df.rename(columns={v: k for k, v in col_map.items() if v})

            if "started_at" not in df.columns or "ended_at" not in df.columns:
                print(f"Missing datetime fields in {csv_name}, skipping.")
                continue

            df["started_at"] = pd.to_datetime(df["started_at"], errors="coerce")
            df["ended_at"] = pd.to_datetime(df["ended_at"], errors="coerce")
            df = df.dropna(subset=["started_at", "ended_at"])

            if "member_casual" in df.columns:
                df["member_casual"] = df["member_casual"].str.lower().str.strip()

            for col in ["ride_id", "rideable_type", "start_lat", "start_lng", "end_lat", "end_lng"]:
                if col not in df.columns:
                    df[col] = pd.NA

            df["year"] = df["started_at"].dt.year
            df["month"] = df["started_at"].dt.month

            grouped = df.groupby(["year", "month"])
            for (year, month), group in grouped:
                result = conn.execute(
                    f"SELECT 1 FROM {TABLE_NAME} WHERE year = ? AND month = ? LIMIT 1;",
                    (year, month)
                ).fetchone()
                if result:
                    print(f"Skipping {zip_file.name} ({year}-{month:02d}) — data already in DB.")
                    continue

                try:
                    group.to_sql(TABLE_NAME, conn, if_exists="append", index=False)
                    print(f"✓ Added {len(group)} rows from {zip_file.name} for {year}-{month:02d} to {TABLE_NAME}.")
                except Exception as e:
                    print(f"Failed to insert into DB: {e}")

# --- Run everything ---
if __name__ == "__main__":
    links = fetch_file_links()
    downloaded_paths = download_new_zips(links)
    if downloaded_paths:
        process_and_ingest_zips(downloaded_paths)
    else:
        print("No new files to process.")
