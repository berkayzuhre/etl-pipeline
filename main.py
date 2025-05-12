import os
from etl_pipeline.pipeline import etl_pipeline

if __name__ == "__main__":
    # Get configuration from environment variables or use defaults
    bucket = os.getenv("S3_BUCKET", "sample-sales-data")
    file = os.getenv("S3_FILE_KEY", "sales/2023-05-12/transactions.json")
    db_conn = os.getenv(
        "DB_CONNECTION", 
        "postgresql://postgres:postgres@localhost:5432/sales_db"
    )
    
    # Run the ETL pipeline
    etl_pipeline(bucket, file, db_conn)
