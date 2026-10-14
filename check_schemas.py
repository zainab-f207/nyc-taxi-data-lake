import os
import pyarrow.parquet as pq

january = pq.read_schema("data/raw/yellow_tripdata_2025-01.parquet")

for name in sorted(os.listdir("data/raw")):
    schema = pq.read_schema("data/raw/" + name)

    if schema.equals(january):
        print(name, "- same as January")
    else:
        print(name, "- DIFFERENT")
        for field in schema:
            if field.name not in january.names:
                print("   new column:", field.name, field.type)
            elif field.type != january.field(field.name).type:
                print("   type changed:", field.name, field.type)
        for name_in_jan in january.names:
            if name_in_jan not in schema.names:
                print("   missing column:", name_in_jan)