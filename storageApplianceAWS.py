import os, boto3, time
import config as cfg

def main_loop(s3_client):
    while 1:
        user_input = input("Do you wish to search, download, or exit? ")
        if user_input == "exit":
            exit()  
        elif user_input == "search":
            print("Search> ", end="")
            user_input = input("All containers [all], a specific container [con], or object [obj]? ")
            
            if user_input == "all":
                start_time = time.time()
                bucket_names = get_all_bucket_names(s3_client)
                display_all_buckets(bucket_names)
                print(str(time.time() - start_time) + " seconds.")            
            elif user_input == "con":
                print("Search> ", end="")
                user_input = input("Container name? ")
                start_time = time.time() 
                if not is_valid_bucket(s3_client, user_input):
                    print("Error: This bucket/bucket name is invalid. Please try again.")
                    continue
                display_bucket(user_input)
                print(str(time.time() - start_time) + " seconds.")  
            elif user_input == "obj":
                print("Search> ", end="")
                user_input = input("Object name? ")
                start_time = time.time() 
                bucket_name = find_object_all_buckets(s3_client, user_input)
                if bucket_name is not None:
                    print("Container: " + bucket_name)
                else:
                    print("Object '" + user_input + "' not found")
                print(str(time.time() - start_time) + " seconds.") 

        elif user_input == "download":
            print("Download> ", end="")
            bucket_name = input("Container name? ")
            if not is_valid_bucket(s3_client, bucket_name):
                print("Error: This bucket/bucket name is invalid. Please try again.")
                continue 
            print("Download> ", end="")
            object_name = input("Object name? ")
            start_time = time.time() 
            if find_object(bucket_name, object_name):
                if not os.path.isdir(cfg.task_one['download_dir']):
                    os.mkdir(cfg.task_one['download_dir'])
                download_object_to_dir(s3_client, bucket_name, object_name, cfg.task_one['download_dir'])
                print("Downloaded '" + object_name + "' to " + cfg.task_one['download_dir'])
            else:
                print("Object '" + object_name + "' not found")
            print(str(time.time() - start_time) + " seconds.") 

def is_valid_bucket(s3_client, bucket_name):
    if bucket_name not in get_all_bucket_names(s3_client):
        return False
    return True

def find_object_all_buckets(s3_client, object_name):
    bucket_names = get_all_bucket_names(s3_client)
    for bucket in bucket_names:
        if find_object(bucket, object_name):
            return bucket

    return None

def get_all_bucket_names(s3_client):
    buckets = []
    response = s3_client.list_buckets()
    buckets = [bucket['Name'] for bucket in response['Buckets']]
    return buckets

def find_object(bucket_name, object_name):
    s3 = boto3.resource('s3')
    current_bucket = s3.Bucket(name=bucket_name)

    for bucket_object in current_bucket.objects.all():
        if object_name == bucket_object.key:
            return True

    return False

def download_object_to_dir(s3_client, bucket_name, object_name, dir):
    with open(os.path.join(dir, object_name), 'wb') as f:
        s3_client.download_fileobj(bucket_name, object_name, f)

def display_all_buckets(bucket_names):
    for bucket in bucket_names:
        display_bucket(bucket)
    
def display_bucket(bucket_name):
    print(bucket_name)
    s3 = boto3.resource('s3')
    current_bucket = s3.Bucket(name=bucket_name)
        
    for bucket_object in current_bucket.objects.all():
        print("\t" + bucket_object.key) 

if __name__ == "__main__":
    s3_client = boto3.client('s3')
    main_loop(s3_client)