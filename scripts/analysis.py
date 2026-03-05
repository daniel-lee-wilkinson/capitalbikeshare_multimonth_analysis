import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.ticker as ticker
import time
# Connect to SQLite DB
conn = sqlite3.connect("bikeshare_all.db")

start = time.time()
# --- Monthly ridership and season classification ---
query = """
SELECT
    year,
    month,
    COUNT(*) AS total_trips,
    CASE
        WHEN month IN (12, 1, 2) THEN 'Winter'
        WHEN month IN (3, 4, 5) THEN 'Spring'
        WHEN month IN (6, 7, 8) THEN 'Summer'
        WHEN month IN (9, 10, 11) THEN 'Autumn'
    END AS season
FROM trips
GROUP BY year, month
ORDER BY year, month;
"""
print(f"⏱️ Block completed in {round(time.time() - start, 2)} seconds")

df = pd.read_sql_query(query, conn)

# Create 'YYYY-MM' label and assign season colours
df["year_month"] = df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2)

season_colours = {
    "Winter": "#5DADE2",
    "Spring": "#58D68D",
    "Summer": "#F5B041",
    "Autumn": "#AF7AC5"
}
df["colour"] = df["season"].map(season_colours)

import matplotlib.ticker as ticker

# Plot total trips by month with seasonal colour coding
plt.figure(figsize=(10, 5))
bars = plt.bar(df["year_month"], df["total_trips"], color=df["colour"])
plt.xticks(rotation=45)
plt.title("Total Bikeshare Trips per Month (Coloured by Season)")
plt.xlabel("Month")

# Scale y-axis to thousands and relabel
ax = plt.gca()
ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: f'{int(x/1000)}'))
plt.ylabel("Trips (Thousands)")

# Add legend
# Build the legend in the order seasons appear in the plot
ordered_seasons = df["season"].drop_duplicates().tolist()  # preserves order of appearance
legend_patches = [
    mpatches.Patch(color=season_colours[season], label=season)
    for season in ordered_seasons if season in season_colours
]
plt.legend(handles=legend_patches, title="Season", loc="upper right")

plt.tight_layout()
plt.savefig("figures/trips_by_month_coloured_by_season.png")
plt.show()

import matplotlib.pyplot as plt

df["year_month"] = pd.to_datetime(df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2))
df = df.sort_values("year_month")

plt.figure(figsize=(12, 5))
plt.plot(df["year_month"], df["total_trips"], marker="o", linewidth=2)
plt.title("Total Bikeshare Trips per Month (2018–2025)")
plt.xlabel("Month")
plt.ylabel("Total Trips")
plt.grid(True)
plt.tight_layout()
plt.savefig("figures/trips_by_month_line.png")
plt.show()


start = time.time()
# --- Average trip duration by season ---
duration_query = """
SELECT
    CASE
        WHEN CAST(strftime('%m', started_at) AS INTEGER) IN (12, 1, 2) THEN 'Winter'
        WHEN CAST(strftime('%m', started_at) AS INTEGER) IN (3, 4, 5) THEN 'Spring'
        WHEN CAST(strftime('%m', started_at) AS INTEGER) IN (6, 7, 8) THEN 'Summer'
        WHEN CAST(strftime('%m', started_at) AS INTEGER) IN (9, 10, 11) THEN 'Autumn'
    END AS season,
    AVG((julianday(ended_at) - julianday(started_at)) * 86400) AS avg_duration_sec
FROM trips
WHERE
    started_at IS NOT NULL
    AND ended_at IS NOT NULL
    AND (julianday(ended_at) - julianday(started_at)) * 86400 BETWEEN 60 AND 3600
GROUP BY season
ORDER BY CASE
    WHEN season = 'Winter' THEN 1
    WHEN season = 'Spring' THEN 2
    WHEN season = 'Summer' THEN 3
    WHEN season = 'Autumn' THEN 4
END;
"""
print(f"⏱️ Block completed in {round(time.time() - start, 2)} seconds")

df_duration = pd.read_sql_query(duration_query, conn)
df_duration["avg_duration_min"] = df_duration["avg_duration_sec"] / 60

# Define your desired season order
season_order = ["Spring", "Summer", "Autumn", "Winter"]
df_duration = df_duration.set_index("season").reindex(season_order).reset_index()

# Plot
ax = df_duration.plot(
    x="season",
    y="avg_duration_min",
    kind="bar",
    legend=False,
    color="coral",
    figsize=(8, 5)
)

# Add labels above bars
for bar in ax.patches:
    height = bar.get_height()
    ax.text(
        bar.get_x() + bar.get_width() / 2,
        height + 0.2,  # small vertical offset
        f"{height:.1f}",
        ha='center',
        va='bottom'
    )

y_max = df_duration["avg_duration_min"].max()
ax.set_ylim(0, y_max + 2)  # adjust as needed (2 mins buffer works well)

# Clean up axes
plt.xticks(rotation=0)
ax.set_ylabel("")  # Remove y-axis label
ax.set_yticks([])  # Hide y-axis ticks
plt.title("Average Trip Duration by Season (in minutes)")
plt.tight_layout()
plt.savefig("figures/avg_duration_by_season.png")
plt.show()

start = time.time()
duration_query = """
SELECT
    year,
    month,
    AVG((julianday(ended_at) - julianday(started_at)) * 86400.0) AS avg_duration_sec
FROM trips
WHERE
    started_at IS NOT NULL
    AND ended_at IS NOT NULL
    AND (julianday(ended_at) - julianday(started_at)) * 86400 BETWEEN 60 AND 3600
GROUP BY year, month
ORDER BY year, month;
"""
print(f"⏱️ Block completed in {round(time.time() - start, 2)} seconds")


df = pd.read_sql_query(duration_query, conn)
df["year_month"] = pd.to_datetime(df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2))
df["avg_duration_min"] = df["avg_duration_sec"] / 60

# Line plot
plt.figure(figsize=(12, 5))
plt.plot(df["year_month"], df["avg_duration_min"], marker="o")
plt.title("Average Bikeshare Trip Duration by Month (2018–2025)")
plt.ylabel("Avg Trip Duration (minutes)")
plt.xlabel("Month")
plt.grid(True)
plt.tight_layout()
plt.savefig("figures/avg_duration_by_month.png")
plt.show()


# Count trips by month and rider type
start = time.time()
counts_query = """
SELECT
    year,
    month,
    member_casual,
    COUNT(*) AS total_trips
FROM trips
WHERE member_casual IS NOT NULL
GROUP BY year, month, member_casual
ORDER BY year, month;
"""
print(f"⏱️ Block completed in {round(time.time() - start, 2)} seconds")


df = pd.read_sql_query(counts_query, conn)
df["member_casual"] = df["member_casual"].str.lower().str.strip()

# Convert to datetime
df["year_month"] = pd.to_datetime(df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2))

# Pivot so each rider type is a column
pivot_df = df.pivot(index="year_month", columns="member_casual", values="total_trips")

# Line plot
pivot_df.plot(figsize=(12, 5), marker="o")
plt.title("Monthly Trips by Rider Type (2018–2025)")
plt.ylabel("Total Trips")
plt.xlabel("Month")
plt.grid(True)
plt.tight_layout()
plt.savefig("figures/monthly_rides_by_ridertype.png")
plt.show()

# Close the connection at the very end
conn.close()
