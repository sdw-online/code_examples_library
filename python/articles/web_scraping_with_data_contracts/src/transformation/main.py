try:
    from utils.db_utils import connect_to_db, create_transformed_schema_and_table, fetch_extraction_data, insert_transformed_data_to_table
    from tests.data_quality_checks.scan_transformation_data_contract import run_dq_checks_for_transformation_stage
    from transformations import transform_data
except:
    from utils.db_utils import connect_to_db, create_transformed_schema_and_table, fetch_extraction_data, insert_transformed_data_to_table
    from tests.data_quality_checks.scan_transformation_data_contract import run_dq_checks_for_transformation_stage
    from .transformations import transform_data


def transformation_job():
    # Establish a connection to the database
    connection = connect_to_db()

    # Define schema and table names for extracted and transformed data
    extracted_schema_name = 'raw'
    extracted_table_name = 'scraped_fb_data'
    transformed_schema_name = 'staging'
    transformed_table_name = 'transformed_fb_data'

    # Create schema and table for the transformed data if not exist
    create_transformed_schema_and_table(connection, transformed_schema_name, transformed_table_name)

    # Fetch data from the extraction layer
    extracted_data = fetch_extraction_data(connection, extracted_schema_name, extracted_table_name)

    # Perform data transformation
    transformed_data = transform_data(extracted_data)

    # Insert transformed data into the transformed_data table
    insert_transformed_data_to_table(connection, transformed_schema_name, transformed_table_name, transformed_data)

    # Run data quality checks for the transformation stage
    run_dq_checks_for_transformation_stage()

    # Close the database connection
    connection.close()

if __name__ == "__main__":
    transformation_job()
