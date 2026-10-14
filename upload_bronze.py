import os
import boto3

BUCKET = "zainab-nyc-taxi-lake-555"
FOLDER = "bronze/yellow_taxi/"

# Use the de-pipeline robot user's keys from your home folder
session = boto3.Session(profile_name="de-pipeline")
s3 = session.client("s3")

# 1. Upload every file from data/raw
for name in os.listdir("data/raw"):
    print("Uploading", name)
    s3.upload_file("data/raw/" + name, BUCKET, FOLDER + name)

# 2. Check what is really in S3
print("Done. Checking S3...")
result = s3.list_objects_v2(Bucket=BUCKET, Prefix=FOLDER)
print("Files in S3:", result["KeyCount"])

total = 0
for item in result["Contents"]:
    total = total + item["Size"]
print("Total MB:", round(total / 1024 / 1024))