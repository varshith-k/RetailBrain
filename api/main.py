from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from typing import List
from datetime import datetime
import models
from database import engine, get_db
from pydantic import BaseModel

# Create tables (if not exist, though processor does it too)
models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="E-commerce Analytics API")

class SalesStatResponse(BaseModel):
    minute: datetime
    total_sales: float
    purchase_count: int

    class Config:
        orm_mode = True

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.get("/sales", response_model=List[SalesStatResponse])
def get_sales_stats(limit: int = 100, db: Session = Depends(get_db)):
    stats = db.query(models.SalesStats).order_by(models.SalesStats.minute.desc()).limit(limit).all()
    return stats
