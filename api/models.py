from sqlalchemy import Column, Integer, DateTime, Numeric
from database import Base

class SalesStats(Base):
    __tablename__ = "sales_stats"

    minute = Column(DateTime, primary_key=True, index=True)
    total_sales = Column(Numeric(10, 2))
    purchase_count = Column(Integer)
