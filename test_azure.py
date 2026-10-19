import os
from azure.storage.blob import ContainerClient

container = ContainerClient.from_container_url(
    os.environ["AZURE_CONTAINER_URL"],
    connection_timeout=30,
    read_timeout=60
)

blob = container.get_blob_client("diagnostics/connection-test.txt")

blob.upload_blob(
    b"Azure connection test",
    overwrite=True
)

print("Small upload succeeded!")
print("Verified size:", blob.get_blob_properties().size, "bytes")