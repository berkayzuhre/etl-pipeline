import pandas as pd
from prefect import task
from prefect.logging import get_run_logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from etl_pipeline.models import Base, Profile

@task
def load_to_postgres(df: pd.DataFrame, connection_string: str) -> None:
    '''Load profile data into PostgreSQL'''
    logger = get_run_logger()
    logger.info("Loading profile data to PostgreSQL")

    # Create SQLAlchemy engine
    engine = create_engine(connection_string)

    # Create tables if they don't exist
    Base.metadata.create_all(engine)

    # Insert data
    Session = sessionmaker(bind=engine)
    with Session() as session:
        for _, row in df.iterrows():
            profile = Profile(
                url=row['url'],
                first_name=row['first_name'],
                last_name=row['last_name'],
                job_title=row['job_title'],
                headline=row['headline'],
                company=row['company'],
                industry=row['industry'],
                location=row['location'],
                work_email=row['work_email'],
                other_work_emails=row['other_work_emails'],
                twitter=row['twitter'],
                github=row['github'],
                company_linkedin_url=row['company_linkedin_url'],
                company_domain=row['company_domain'],
                profile_image_url=row['profile_image_url'],
                seniority_level=row['seniority_level'],
                tech_profile=row['tech_profile'],
                email_domain=row['email_domain'],
                has_multiple_emails=row['has_multiple_emails']
            )
            session.add(profile)
        session.commit()

    logger.info(f"Successfully loaded {len(df)} profile records to PostgreSQL")