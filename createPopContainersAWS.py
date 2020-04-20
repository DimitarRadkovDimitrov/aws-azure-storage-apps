import sys, os, boto3, time
import config as cfg
from botocore.exceptions import ClientError

def create_bucket(s3_client, bucket_name):
    try:
        s3_client.create_bucket(Bucket=bucket_name)
    except ClientError as e:
        print(e)
        return False
    return True

def upload_all_objects_from_folder(s3_client, bucket, folder_path):
    for filename in os.listdir(folder_path):
        upload_file_path = os.path.join(folder_path, filename)
        if not upload_file(s3_client, bucket, upload_file_path, filename):
            return False
    return True

def upload_file(s3_client, bucket, file_path, object_name):
    try:
        with open(file_path, "rb") as file:
            s3_client.upload_fileobj(file, bucket, object_name)
    except ClientError as e:
        print(e)
        return False
    return True

def teardown(s3_client):
    try:
        for item in cfg.task_one['containers']:
            bucket_name = item['container']['name']
            
            for obj in s3_client.list_objects_v2(Bucket=bucket_name)['Contents']:
                s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
                print("Object: '" + obj['Key'] + "' successfully deleted.")

            s3_client.delete_bucket(Bucket=bucket_name)
            print("\tBucket: '" + bucket_name + "' successfully deleted.")
    except ClientError as e:
        print(e)
        return False
    return True

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "reset":
        s3_client = boto3.client('s3')
        start_time = time.time()
        teardown(s3_client)
        print(str(time.time() - start_time) + " seconds.")
    else:
        s3_client = boto3.client('s3')
        for item in cfg.task_one['containers']:
            bucket_name = item['container']['name']
            upload_dir = item['container']['file_upload_dir']

            start_time = time.time()
            if create_bucket(s3_client, bucket_name):
                print("Bucket: '" + bucket_name + "' successfully created. " + str(time.time() - start_time) + " seconds.")

            start_time = time.time()
            if upload_all_objects_from_folder(s3_client, bucket_name, upload_dir):
                print("\tAll files from folder '" + upload_dir + "' have been uploaded successfully. " + str(time.time() - start_time) + " seconds.")
    