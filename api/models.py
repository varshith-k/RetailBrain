from sqlalchemy import Column, Integer, DateTime, Numeric, Text
from sqlalchemy.dialects.postgresql import JSONB
from database import Base

class SalesStats(Base):
    __tablename__ = "sales_stats"

    minute = Column(DateTime, primary_key=True, index=True)
    total_sales = Column(Numeric(10, 2))
    purchase_count = Column(Integer)


class MinuteEventStats(Base):
    __tablename__ = "minute_event_stats"

    minute = Column(DateTime, primary_key=True, index=True)
    page_views = Column(Integer, default=0)
    add_to_cart = Column(Integer, default=0)
    purchases = Column(Integer, default=0)
    revenue = Column(Numeric(10, 2), default=0)


class ProductSales(Base):
    __tablename__ = "product_sales"

    product_id = Column(Integer, primary_key=True, index=True)
    category = Column(Text)
    total_revenue = Column(Numeric(10, 2), default=0)
    purchase_count = Column(Integer, default=0)
    last_purchase_at = Column(DateTime)


class AnomalyAlert(Base):
    __tablename__ = "anomaly_alerts"

    id = Column(Integer, primary_key=True, index=True)
    minute = Column(DateTime, index=True)
    alert_type = Column(Text)
    severity = Column(Text)
    message = Column(Text)
    details = Column(JSONB)
    created_at = Column(DateTime)
