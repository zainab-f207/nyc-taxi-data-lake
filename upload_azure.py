
import os
import glob
import base64
import struct
from azure.storage.blob import ContainerClient
from azure.core.exceptions import ResourceNotFoundError

container = ContainerClient.from_container_url(
    os.environ["AZURE_CONTAINER_URL"],
    connection_timeout=120,
    read_timeout=120,
    retry_total=5,
    retry_connect=5,
    retry_read=5,
    retry_status=5
)

files = sorted(
    glob.glob("data/raw/yellow_tripdata_2025-*.parquet")
)

if not files:
    raise FileNotFoundError(
        "No 2025 monthly Parquet files found in data/raw."
    )

block_size = 4 * 1024 * 1024
verified = 0
failed = []

for path in files:
    filename = os.path.basename(path)
    blob_name = f"yellow_taxi/{filename}"
    blob = container.get_blob_client(blob_name)
    file_size = os.path.getsize(path)

    print(f"\nProcessing {filename} ({file_size / (1024 * 1024):.1f} MB)")

    try:
        properties = blob.get_blob_properties()

        if properties.size == file_size:
            print("Already uploaded with matching size. Skipping.")
            verified += 1
            continue

        print("Existing size differs. Uploading again.")

    except ResourceNotFoundError:
        print("Not uploaded yet. Starting upload.")

    block_ids = []

    try:
        with open(path, "rb") as file:
            index = 0

            while True:
                chunk = file.read(block_size)
                if not chunk:
                    break

                block_id = base64.b64encode(
                    struct.pack(">I", index)
                ).decode("ascii")

                print(
                    f"  Uploading block {index + 1} "
                    f"({len(chunk) / (1024 * 1024):.1f} MB)"
                )

                blob.stage_block(
                    block_id=block_id,
                    data=chunk,
                    length=len(chunk)
                )

                block_ids.append(block_id)
                index += 1

        blob.commit_block_list(block_ids)

        properties = blob.get_blob_properties()

        if properties.size != file_size:
            raise ValueError(
                f"Size mismatch: expected {file_size}, "
                f"got {properties.size}"
            )

        print("Verified successfully!")
        verified += 1

    except Exception as error:
        print(f"FAILED: {filename}: {error}")
        failed.append(filename)

print("\n========== FINAL SUMMARY ==========")
print(f"Monthly files found: {len(files)}")
print(f"Verified successfully: {verified}")
print(f"Failed: {len(failed)}")

if failed:
    print("Files needing attention:")
    for filename in failed:
        print(f" - {filename}")

print("\nChecking Azure blobs...")

azure_blobs = list(
    container.list_blobs(name_starts_with="yellow_taxi/")
)

print(f"Total blobs under yellow_taxi/: {len(azure_blobs)}")
