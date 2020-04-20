import os, time
import config as cfg
from azure.storage.blob import BlobServiceClient

def main_loop(blob_service_client):
    while 1:
        user_input = input("Do you wish to search, download, or exit? ")
        if user_input == "exit":
            exit()  
        elif user_input == "search":
            print("Search> ", end="")
            user_input = input("All containers [all], a specific container [con], or object [obj]? ")
            
            if user_input == "all":
                start_time = time.time()
                container_names = get_all_container_names(blob_service_client)
                display_all_containers(blob_service_client, container_names)
                print(str(time.time() - start_time) + " seconds.")            
            elif user_input == "con":
                print("Search> ", end="")
                user_input = input("Container name? ")
                start_time = time.time()
                if not is_valid_container(blob_service_client, user_input):
                    print("Error: This container/container name is invalid. Please try again.")
                    continue 
                display_container(blob_service_client, user_input)
                print(str(time.time() - start_time) + " seconds.") 
            elif user_input == "obj":
                print("Search> ", end="")
                user_input = input("Object name? ")
                start_time = time.time() 
                container_name = find_object_all_containers(blob_service_client, user_input)
                if container_name is not None:
                    print("Container: " + container_name)
                else:
                    print("Object '" + user_input + "' not found")
                print(str(time.time() - start_time) + " seconds.") 

        elif user_input == "download":
            print("Download> ", end="")
            container_name = input("Container name? ")
            if not is_valid_container(blob_service_client, container_name):
                print("Error: This container/container name is invalid. Please try again.")
                continue 
            print("Download> ", end="")
            object_name = input("Object name? ")
            start_time = time.time() 
            if find_object(blob_service_client, container_name, object_name):
                if not os.path.isdir(cfg.task_one['download_dir']):
                    os.mkdir(cfg.task_one['download_dir'])
                download_object_to_dir(blob_service_client, container_name, object_name, cfg.task_one['download_dir'])
                print("Downloaded '" + object_name + "' to " + cfg.task_one['download_dir'])
            else:
                print("Object '" + object_name + "' not found")
            print(str(time.time() - start_time) + " seconds.") 

def is_valid_container(blob_service_client, container_name):
    if container_name not in get_all_container_names(blob_service_client):
        return False
    return True

def find_object_all_containers(blob_service_client, object_name):
    container_names = get_all_container_names(blob_service_client)
    for container in container_names:
        if find_object(blob_service_client, container, object_name):
            return container

    return None

def get_all_container_names(blob_service_client):
    container_names = [container['name'] for container in blob_service_client.list_containers()]
    return container_names

def find_object(blob_service_client, container_name, object_name):
    container_client = blob_service_client.get_container_client(container_name)
    for blob in container_client.list_blobs():
        if blob.name == object_name:
            return True
    
    return False

def download_object_to_dir(blob_service_client, container_name, object_name, dir):
    container_client = blob_service_client.get_container_client(container_name)
    with open(os.path.join(dir, object_name), 'wb') as f:
        f.write(container_client.download_blob(object_name).readall())

def display_all_containers(blob_service_client, container_names):
    for container in container_names:
        display_container(blob_service_client, container)
    
def display_container(blob_service_client, container_name):
    print(container_name)
    container_client = blob_service_client.get_container_client(container_name)
    
    for blob in container_client.list_blobs():
        print("\t " + blob.name)

if __name__ == "__main__":
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    main_loop(blob_service_client)
