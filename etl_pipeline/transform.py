from datetime import datetime
from typing import Dict, List, Any

import pandas as pd
from prefect import task
from prefect.logging import get_run_logger

@task
def transform_data(data: List[Dict[str, Any]]) -> pd.DataFrame:
    '''Transform and enrich the extracted data'''
    logger = get_run_logger()
    logger.info("Transforming data")
    
    # Convert to DataFrame for easier manipulation
    df = pd.DataFrame(data)
    
    # Add calculated fields
    df['total'] = df['quantity'] * df['price']
    df['timestamp'] = datetime.now()
    df['processed_at'] = datetime.now()
    
    # Apply business rules (example: add discount for bulk purchases)
    df.loc[df['quantity'] > 5, 'total'] = df['total'] * 0.9
    
    logger.info(f"Transformed data shape: {df.shape}")
    return df
