import os
import boto3

BUCKET = "zainab-nyc-taxi-lake-555"

session = boto3.Session(profile_name="de-pipeline")
s3 = session.client("s3")

count = 0
for folder, subfolders, files in os.walk("data/silver"):
    for name in files:
        # skip Spark housekeeping files (.crc and _SUCCESS)
        if name.startswith(".") or name.startswith("_"):
            continue
        path = os.path.join(folder, name)
        key = os.path.relpath(path, "data").replace("\\", "/")
        print("Uploading", key)
        s3.upload_file(path, BUCKET, key)
        count = count + 1

print("Uploaded", count, "files")

result = s3.list_objects_v2(Bucket=BUCKET, Prefix="silver/")
print("Files in S3 under silver/:", result["KeyCount"])