import os
import io
import pandas as pd
import boto3
from typing import List, Dict, Any
from prefect import task
from prefect.logging import get_run_logger
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

@task
def extract_from_s3(bucket_name: str, file_key: str) -> List[Dict[str, Any]]:
    '''Extract data from AWS S3 bucket (CSV file)'''
    logger = get_run_logger()
    logger.info(f"Extracting data from S3: {bucket_name}/{file_key}")

    # Retrieve AWS credentials from environment
    aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    region = os.environ.get("AWS_DEFAULT_REGION")

    # Create boto3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region
    )

    # Download file object
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    csv_contents = response['Body'].read()

    # Read CSV content with pandas
    df = pd.read_csv(io.BytesIO(csv_contents))
    logger.info(f"Extracted {len(df)} records from CSV")

    # Convert to list of dicts for downstream use
    return df.to_dict(orient="records")