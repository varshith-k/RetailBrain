import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

_raw_url = os.getenv('POSTGRES_URL', 'postgresql://user:password@localhost:5432/ecommerce_db')
# Railway gives postgres:// but SQLAlchemy requires postgresql://
POSTGRES_URL = _raw_url.replace('postgres://', 'postgresql://', 1)

engine = create_engine(POSTGRES_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
