from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class SalesRecord(Base):
    __tablename__ = "sales_records"
    
    id = Column(Integer, primary_key=True)
    product_id = Column(String)
    customer_id = Column(String)
    quantity = Column(Integer)
    price = Column(Float)
    total = Column(Float)
    region = Column(String)
    timestamp = Column(DateTime)
    processed_at = Column(DateTime)
