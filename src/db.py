import os
from sqlalchemy import create_engine

def get_engine():
    # Default local dev URL used if DATABASE_URL not set
    db_url = os.environ.get("DATABASE_URL", "postgresql://pguser:pgpass@localhost:5432/salesdb")
    # echo=False to avoid noisy logs in tests; set to True for debugging
    return create_engine(db_url, future=True)
