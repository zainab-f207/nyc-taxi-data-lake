import os
import requests
import pyarrow.parquet as pq

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data", "raw")
os.makedirs(DATA_DIR, exist_ok=True)

months = [f"2025-{m:02d}" for m in range(1, 13)]

total_rows = 0
for month in months:
    filename = f"yellow_tripdata_{month}.parquet"
    path = os.path.join(DATA_DIR, filename)

    if os.path.exists(path):
        print(f"Skipping {filename} (already downloaded)")
    else:
        print(f"Downloading {filename} ...")
        response = requests.get(f"{BASE_URL}/{filename}", stream=True, timeout=60)
        response.raise_for_status()
        with open(path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)

    rows = pq.ParquetFile(path).metadata.num_rows
    total_rows += rows
    size_mb = os.path.getsize(path) / (1024 * 1024)
    print(f"  {filename}: {rows:,} rows, {size_mb:.1f} MB")

print(f"\nTotal rows: {total_rows:,}")