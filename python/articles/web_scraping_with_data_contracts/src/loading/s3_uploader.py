from utils.aws_utils import connect_to_aws_s3, create_bucket_if_not_exists, upload_file_to_s3, validate_file_in_s3
from utils.db_utils import connect_to_db, fetch_transformed_data, convert_dataframe_to_csv
import os

def loading_job():
    print("Starting data transfer process...")
    connection = None
    try:
        connection = connect_to_db()
        df = fetch_transformed_data(connection)
        local_filename = 'transformed_data.csv'
        convert_dataframe_to_csv(df, local_filename)
        
        s3_client = connect_to_aws_s3()
        bucket_name = create_bucket_if_not_exists(s3_client, os.getenv("S3_BUCKET"), os.getenv("REGION_NAME"))
        s3_folder = os.getenv("S3_FOLDER")
        upload_file_to_s3(s3_client, local_filename, bucket_name, s3_folder)
        
        s3_path = f"{s3_folder}/{local_filename}" if s3_folder else local_filename
        if validate_file_in_s3(s3_client, bucket_name, s3_path):
            print(f'✅ File {local_filename} successfully uploaded to bucket {bucket_name}/{s3_path}')
        else:
            print(f'❌ File {local_filename} not found in bucket {bucket_name}/{s3_path}')
            
    except Exception as e:
        print(f"❌ An error occurred: {e}")
    
    finally:
        if connection:
            print("Closing the database connection.")
            connection.close()

if __name__ == "__main__":
    loading_job()
