import os
import boto3
from dotenv import load_dotenv
from boto3.exceptions import Boto3Error

# Load environment variables from .env file
load_dotenv()


def connect_to_aws_s3():
    print("Connecting to AWS S3...")
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("REGION_NAME")
        )
        print("Connected to AWS S3 successfully.")
        return s3_client
    except Boto3Error as e:
        raise Exception(f"❌[ERROR - AWS S3 CONNECTION]: {e}")



def create_bucket_if_not_exists(s3_client, bucket_name, region):
    print(f"Checking if the bucket {bucket_name} exists...")
    try:
        if bucket_name not in [bucket['Name'] for bucket in s3_client.list_buckets()['Buckets']]:
            s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': region})
            print(f"Bucket {bucket_name} created successfully.")
        else:
            print(f"Bucket {bucket_name} already exists.")
        return bucket_name
    except Boto3Error as e:
        raise Exception(f"❌[ERROR - BUCKET CREATION]: {e}")



def upload_file_to_s3(s3_client, local_filename, bucket_name, s3_folder=None):
    print(f"Uploading {local_filename} to the bucket {bucket_name}...")
    try:
        csv_folder = "data/"
        full_csv_file_path = f"{csv_folder}{local_filename}" 
        s3_path = f"{s3_folder}/{local_filename}" if s3_folder else local_filename
        s3_client.upload_file(full_csv_file_path, bucket_name, s3_path)
        print(f"File {local_filename} uploaded to {bucket_name}/{s3_path} successfully.")
    except Exception as e:
        raise Exception(f"❌[ERROR - FILE UPLOAD]: {e}")



def validate_file_in_s3(s3_client, bucket_name, s3_path):
    print(f"Validating the presence of {s3_path} in the bucket {bucket_name}...")
    try:
        s3_client.head_object(Bucket=bucket_name, Key=s3_path)
        print(f"Validation successful: {s3_path} exists in {bucket_name}.")
        return True
    except Boto3Error as e:
        raise Exception(f"❌[ERROR - VALIDATE FILE]: {e}")

 