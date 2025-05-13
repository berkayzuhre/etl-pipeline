import os
from dotenv import load_dotenv
from etl_pipeline.pipeline import etl_pipeline

if __name__ == "__main__":
    # Load environment variables from .env file
    load_dotenv()   # <-- This line loads the .env file in your current directory.

    # Now get env vars (as you already wrote)
    bucket = os.getenv("S3_BUCKET")
    file = os.getenv("S3_FILE_KEY")
    db_conn = os.getenv(
        "DB_CONNECTION",
        "postgresql://postgres:postgres@localhost:5432/etl_db"
    )
    
    # Run the ETL pipeline
    etl_pipeline(bucket, file, db_conn)