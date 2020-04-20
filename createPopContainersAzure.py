import sys, os, time
import config as cfg
from azure.storage.blob import BlobServiceClient, ContainerClient

def create_container(BlobServiceClient, container_name):
    container_client = blob_service_client.get_container_client(container_name)
    try:
        container_properties = container_client.get_container_properties()
    except Exception as e:
        container_client = blob_service_client.create_container(container_name)
    return container_client

def upload_all_blobs_from_folder(container_client, folder_path):
    blob_list = [blob.name for blob in container_client.list_blobs()]
    for filename in os.listdir(folder_path):
        if filename not in blob_list:
            upload_file_path = os.path.join(folder_path, filename)
            upload_file(container_client, upload_file_path, filename)

def upload_file(container_client, file_path, object_name):
    try:
        with open(file_path, "rb") as file:
            container_client.upload_blob(name=object_name, data=file)
    except Exception as e:
        print(e)

def teardown(blob_service_client):
    for item in cfg.task_one['containers']:
        container_name = item['container']['name']
        container_client = blob_service_client.get_container_client(container_name)

        for blob in container_client.list_blobs():
            container_client.delete_blob(blob.name)
            print("Blob: '" + blob.name + "' successfully deleted.")

        container_client.delete_container()
        print("\tContainer: '" + container_name + "' successfully deleted.")

if __name__ == "__main__":
    try:
        connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        
        if len(sys.argv) > 1 and sys.argv[1] == "reset":
            start_time = time.time()
            teardown(blob_service_client)
            print(str(time.time() - start_time) + " seconds.")
        else:
            for item in cfg.task_one['containers']:
                container_name = item['container']['name']
                upload_dir = item['container']['file_upload_dir']

                start_time = time.time()
                container_client = create_container(blob_service_client, container_name)
                print("Container: '" + container_name + "' successfully created. " + str(time.time() - start_time) + " seconds.")

                start_time = time.time()
                upload_all_blobs_from_folder(container_client, upload_dir)
                print("\tAll files from folder '" + upload_dir + "' have been uploaded successfully. " + str(time.time() - start_time) + " seconds.")
    except Exception as e:
        print(e)
    