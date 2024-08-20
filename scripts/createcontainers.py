from azure.storage.blob import BlobServiceClient
from azure.data.tables import TableServiceClient

connection_string = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
connection_table_string = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
taskio_container_client = blob_service_client.get_container_client("taskio")
tasklog_container_client = blob_service_client.get_container_client("tasklogs")
code_container_client = blob_service_client.get_container_client("code")

if not taskio_container_client.exists():
    print("Creating taskio container.")
    taskio_container_client.create_container()
else:
    print("Taskio container already exists.")
if not tasklog_container_client.exists():
    print("Creating tasklog container.")
    tasklog_container_client.create_container()
else:
    print("Tasklog container already exists.")
if not code_container_client.exists():
    print("Creating code container.")
    code_container_client.create_container()
else:
    print("Code container already exists.")

table_client = TableServiceClient.from_connection_string(connection_table_string)

image_key_table_client = table_client.get_table_client("imagekeys")

entity = {
    "PartitionKey": "ingest",
    "RowKey": "default",
    "image": "localhost:5001/pctasks-ingest:latest",
    "environment": "DB_CONNECTION_STRING=postgresql://username:password@database:5432/postgis",
    "tags": None
}

image_key_table_client.upsert_entity(entity)

entity = image_key_table_client.get_entity("ingest", "default")

print(entity)

for blob in code_container_client.list_blobs():
    print(f"Code container {blob.name}")
    #code_container_client.delete_blob(blob.name)