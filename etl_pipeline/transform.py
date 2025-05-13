from datetime import datetime
from typing import Dict, List, Any

import pandas as pd
from prefect import task
from prefect.logging import get_run_logger
from etl_pipeline.business_logic import get_seniority

@task
def transform_data(data: List[Dict[str, Any]]) -> pd.DataFrame:
    '''Transform and enrich the extracted data'''
    logger = get_run_logger()
    logger.info("Transforming data")
    
    # Convert to DataFrame for easier manipulation
    df = pd.DataFrame(data)

    df = df.rename(columns={
        'Url': 'url',
        'First Name': 'first_name',
        'Last Name': 'last_name',
        'Job Title': 'job_title',
        'Headline': 'headline',
        'Company': 'company',
        'Industry': 'industry',
        'Location': 'location',
        'Work Email': 'work_email',
        'Other Work Emails': 'other_work_emails',
        'Twitter': 'twitter',
        'Github': 'github',
        'Company LinkedinUrl': 'company_linkedin_url',
        'Company Domain': 'company_domain',
        'Profile Image Url': 'profile_image_url'
    })

    df = df.where(pd.notnull(df), None)

    # Apply seniority_level
    df['seniority_level'] = df['job_title'].apply(get_seniority)

    # 2. tech_profile: Boolean flag indicating if github field is populated
    df['tech_profile'] = df['github'].notna() & df['github'].astype(str).str.strip().ne('')

    # 3. email_domain: Extract domain from work_email
    df['email_domain'] = df['work_email'].str.extract(r'@([\w\.-]+)', expand=False)

    # 4. has_multiple_emails: Boolean flag for other_work_emails populated
    df['has_multiple_emails'] = df['other_work_emails'].notna() & df['other_work_emails'].astype(str).str.strip().ne('')

    df = df.where(pd.notnull(df), None)
            
    logger.info(f"Transformed data shape: {df.shape}")
    return df
