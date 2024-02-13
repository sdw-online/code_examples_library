from extraction.main import extraction_job
from transformation.main import transformation_job
from loading.s3_uploader import loading_job


# Create a single point of entry to run the scraping ETL pipeline
def run_etl_pipeline():
    extraction_job()
    transformation_job()
    loading_job()


# Execute the pipeline 
if __name__=="__main__":
    run_etl_pipeline()