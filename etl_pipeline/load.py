import pandas as pd
from prefect import task
from prefect.logging import get_run_logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from etl_pipeline.models import Base, SalesRecord

@task
def load_to_postgres(df: pd.DataFrame, connection_string: str) -> None:
    '''Load transformed data into PostgreSQL'''
    logger = get_run_logger()
    logger.info("Loading data to PostgreSQL")
    
    # Create SQLAlchemy engine
    engine = create_engine(connection_string)
    
    # Create tables if they don't exist
    Base.metadata.create_all(engine)
    
    # Insert data
    Session = sessionmaker(bind=engine)
    with Session() as session:
        for _, row in df.iterrows():
            record = SalesRecord(
                product_id=row['product_id'],
                customer_id=row['customer_id'],
                quantity=row['quantity'],
                price=row['price'],
                total=row['total'],
                region=row['region'],
                timestamp=row['timestamp'],
                processed_at=row['processed_at']
            )
            session.add(record)
        session.commit()
    
    logger.info(f"Successfully loaded {len(df)} records to PostgreSQL")
