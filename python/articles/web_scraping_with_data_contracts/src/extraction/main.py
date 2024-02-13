try:
    from .scraping_bot import scrape_data
    from utils.db_utils import connect_to_db, create_extracted_schema_and_table, insert_extracted_data_to_table
    from tests.data_quality_checks.scan_extraction_data_contract import run_dq_checks_for_extraction_stage
except:
    from scraping_bot import scrape_data
    from utils.db_utils import connect_to_db, create_extracted_schema_and_table, insert_extracted_data_to_table
    from tests.data_quality_checks.scan_extraction_data_contract import run_dq_checks_for_extraction_stage



def extraction_job():
    # Flag for running the data quality checks only 
    RUN_DQ_CHECKS_ONLY = True


    if not RUN_DQ_CHECKS_ONLY:
        # Connect to the database
        connection = connect_to_db()

        # Create schema and table if they don't exist
        schema_name = 'raw'
        table_name = "scraped_fb_data"
        create_extracted_schema_and_table(connection, schema_name, table_name)


        # Scrape the data and store in a DataFrame
        dates = [
            "2024-02-13",
            # "2024-03-31",
            # "2024-04-30",
            # "2024-05-31"
        ]
        df = scrape_data(dates, show_output=False)

        # Insert data into the database from the DataFrame
        insert_extracted_data_to_table(connection, schema_name, table_name, df)

        # Close the database connection
        connection.close()


        # Run DQ checks for extraction stage
        run_dq_checks_for_extraction_stage()

    else:
        run_dq_checks_for_extraction_stage()

if __name__=="__main__":
    extraction_job()