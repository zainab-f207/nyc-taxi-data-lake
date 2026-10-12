import pandas as pd

df = pd.read_parquet("data/raw/yellow_tripdata_2025-01.parquet")

print("Rows:", len(df))
print("\nNulls per column:")
print(df.isna().sum())

print("\nNegative fares:", (df["fare_amount"] < 0).sum())
print("Zero distance:", (df["trip_distance"] == 0).sum())
print("Dropoff before pickup:", (df["tpep_dropoff_datetime"] < df["tpep_pickup_datetime"]).sum())

print("\nPayment type of negative-fare rows:")
print(df[df["fare_amount"] < 0]["payment_type"].value_counts())

print("\nAre the null rows the same rows?")
print(df["passenger_count"].isna().equals(df["RatecodeID"].isna()))

print("\nPayment type of the null rows:")
print(df[df["passenger_count"].isna()]["payment_type"].value_counts())

print("\nPayment type of ALL rows:")
print(df["payment_type"].value_counts())

print("\nNegative total_amount rows:", (df["total_amount"] < 0).sum())

mixed = df[(df["fare_amount"] < 0) & (df["total_amount"] >= 0)]
print("\nNegative fare but total >= 0:", len(mixed))
print(mixed[["fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
             "improvement_surcharge", "total_amount", "payment_type"]].head(8))