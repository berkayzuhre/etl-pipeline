import json
from typing import Dict, List, Any

import boto3
from prefect import task
from prefect.logging import get_run_logger

@task
def extract_from_s3(bucket_name: str, file_key: str) -> List[Dict[str, Any]]:
    '''Extract data from AWS S3 bucket'''
    logger = get_run_logger()
    logger.info(f"Extracting data from S3: {bucket_name}/{file_key}")
    
    # In a real scenario, we would use boto3 to download from S3
    # s3_client = boto3.client('s3')
    # response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    # data = json.loads(response['Body'].read().decode('utf-8'))
    
    # For demonstration, we'll use sample data
    sample_data = [
        {"product_id": "P001", "customer_id": "C1001", "quantity": 5, "price": 10.99, "region": "North"},
        {"product_id": "P002", "customer_id": "C1002", "quantity": 2, "price": 24.99, "region": "South"},
        {"product_id": "P003", "customer_id": "C1003", "quantity": 1, "price": 99.99, "region": "East"},
        {"product_id": "P001", "customer_id": "C1004", "quantity": 3, "price": 10.99, "region": "West"},
        {"product_id": "P004", "customer_id": "C1005", "quantity": 10, "price": 4.99, "region": "North"},
    ]
    
    logger.info(f"Extracted {len(sample_data)} records")
    return sample_data
