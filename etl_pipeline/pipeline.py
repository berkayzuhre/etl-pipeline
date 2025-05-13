from prefect import flow
from prefect.logging import get_run_logger

from etl_pipeline.extract import extract_from_s3
from etl_pipeline.transform import transform_data
from etl_pipeline.load import load_to_postgres

@flow(name="Linkedin Data ETL Pipeline")
def etl_pipeline(bucket_name: str, file_key: str, db_connection: str) -> None:
    '''Main ETL flow to process linkedin data'''
    logger = get_run_logger()
    logger.info("Starting ETL pipeline")
    
    # Extract
    raw_data = extract_from_s3(bucket_name, file_key)
    
    # Transform
    transformed_data = transform_data(raw_data)
    
    # Load
    load_to_postgres(transformed_data, db_connection)
    
    logger.info("ETL pipeline completed successfully")
