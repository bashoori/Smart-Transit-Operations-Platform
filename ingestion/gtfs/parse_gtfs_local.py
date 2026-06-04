import os
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

GTFS_PATH = os.getenv("GTFS_LOCAL_PATH", "data/bronze/gtfs")
DB_URL = (
    f"postgresql+psycopg2://"
    f"{os.getenv('POSTGRES_USER', 'translink_user')}:"
    f"{os.getenv('POSTGRES_PASSWORD', 'translink_pass_2024')}@"
    f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
    f"{os.getenv('POSTGRES_PORT', '5432')}/"
    f"{os.getenv('POSTGRES_DB', 'translink_platform')}"
)

engine = create_engine(DB_URL)

def load_gtfs_file(filename, table, schema="bronze"):
    filepath = os.path.join(GTFS_PATH, filename)
    if not os.path.exists(filepath):
        filepath = os.path.join("data/raw", f"sample_{filename.replace('.txt','.csv')}")
        print(f"  Using sample: {filepath}")
    df = pd.read_csv(filepath, dtype=str, keep_default_na=False)
    df["_ingested_at"] = datetime.utcnow().isoformat()
    df["_source_file"] = filename
    df["_pipeline"]    = "gtfs_local_load"
    df.to_sql(name=table, con=engine, schema=schema,
              if_exists="replace", index=False, method="multi", chunksize=10_000)
    print(f"  {len(df):,} rows -> {schema}.{table}")
    return len(df)

files = {
    "routes.txt":         "gtfs_routes_raw",
    "trips.txt":          "gtfs_trips_raw",
    "stops.txt":          "gtfs_stops_raw",
    "stop_times.txt":     "gtfs_stop_times_raw",
    "calendar.txt":       "gtfs_calendar_raw",
    "calendar_dates.txt": "gtfs_calendar_dates_raw",
    "transfers.txt":      "gtfs_transfers_raw",
}

total = 0
for f, t in files.items():
    try:
        total += load_gtfs_file(f, t)
    except Exception as e:
        print(f"  WARNING {f}: {e}")

print(f"\nTotal rows loaded: {total:,}")
