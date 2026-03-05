import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.ticker as ticker

with sqlite3.connect("bikeshare_all.db") as conn:
    conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_started_at ON trips(started_at);
        CREATE INDEX IF NOT EXISTS idx_ended_at ON trips(ended_at);
        CREATE INDEX IF NOT EXISTS idx_year_month ON trips(year, month);
        CREATE INDEX IF NOT EXISTS idx_member_casual ON trips(member_casual);
    """)
print("✅ Indexes created.")
