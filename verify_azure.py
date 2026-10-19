
import os
from azure.storage.blob import ContainerClient

container = ContainerClient.from_container_url(
    os.environ["AZURE_CONTAINER_URL"],
    connection_timeout=120,
    read_timeout=120
)

print("Checking Azure container...")

blobs = list(container.list_blobs())

print("Number of blobs:", len(blobs))

for blob in blobs:
    print(blob.name, round(blob.size / 1024 / 1024, 2), "MB")
