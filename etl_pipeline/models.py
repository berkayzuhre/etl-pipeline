from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Profile(Base):
    __tablename__ = "linkedin_data_3"
    
    id = Column(Integer, primary_key=True)
    url = Column(String)                     # LinkedIn profile URL
    first_name = Column(String)
    last_name = Column(String)
    job_title = Column(String)
    headline = Column(String)
    company = Column(String)
    industry = Column(String)
    location = Column(String)
    work_email = Column(String)
    other_work_emails = Column(String)       # Could optionally use JSON or ARRAY type
    twitter = Column(String)
    github = Column(String)
    company_linkedin_url = Column(String)
    company_domain = Column(String)
    profile_image_url = Column(String)
    seniority_level = Column(String)         # Derived field
    tech_profile = Column(String)            # Derived field
    email_domain = Column(String)            # Derived field
    has_multiple_emails = Column(String)     # Derived field