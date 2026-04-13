from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Optional
import os

from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
import models
from database import engine, get_db
from pydantic import BaseModel
from ai_engine import run_langchain_assistant, run_langgraph_business_update

# Create tables (if not exist, though processor does it too)
models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="RetailBrain Commerce Intelligence API")
POSTGRES_URL = os.getenv('POSTGRES_URL', 'postgresql://user:password@localhost:5432/ecommerce_db')

class SalesStatResponse(BaseModel):
    minute: datetime
    total_sales: float
    purchase_count: int

    class Config:
        orm_mode = True


class ProductTrendResponse(BaseModel):
    product_id: int
    category: Optional[str] = None
    total_revenue: float
    purchase_count: int


class AlertResponse(BaseModel):
    id: int
    minute: datetime
    alert_type: str
    severity: str
    message: str
    details: Optional[dict] = None
    created_at: datetime

    class Config:
        orm_mode = True


class OverviewResponse(BaseModel):
    window_minutes: int
    total_revenue: float
    total_page_views: int
    total_add_to_cart: int
    total_purchases: int
    conversion_rate: float
    abandonment_rate: float


class AssistantQueryRequest(BaseModel):
    question: str


class AssistantResponse(BaseModel):
    answer: str
    evidence: List[str]
    next_actions: List[str]
    workflow_trace: List[str]


def _to_float(value):
    if value is None:
        return 0.0
    if isinstance(value, Decimal):
        return float(value)
    return float(value)


def _compute_overview(db: Session, minutes: int):
    since = datetime.utcnow() - timedelta(minutes=minutes)
    rows = (
        db.query(models.MinuteEventStats)
        .filter(models.MinuteEventStats.minute >= since)
        .all()
    )

    total_revenue = sum(_to_float(row.revenue) for row in rows)
    total_page_views = sum(int(row.page_views or 0) for row in rows)
    total_add_to_cart = sum(int(row.add_to_cart or 0) for row in rows)
    total_purchases = sum(int(row.purchases or 0) for row in rows)

    conversion_rate = 0.0
    abandonment_rate = 0.0
    if total_page_views > 0:
        conversion_rate = total_purchases / total_page_views
    if total_add_to_cart > 0:
        abandonment_rate = max(total_add_to_cart - total_purchases, 0) / total_add_to_cart

    return {
        "window_minutes": minutes,
        "total_revenue": total_revenue,
        "total_page_views": total_page_views,
        "total_add_to_cart": total_add_to_cart,
        "total_purchases": total_purchases,
        "conversion_rate": conversion_rate,
        "abandonment_rate": abandonment_rate,
    }

@app.get("/health")
def health_check():
    return {"status": "ok", "service": "retailbrain-api"}

@app.get("/sales", response_model=List[SalesStatResponse])
def get_sales_stats(limit: int = 100, db: Session = Depends(get_db)):
    stats = db.query(models.SalesStats).order_by(models.SalesStats.minute.desc()).limit(limit).all()
    return stats


@app.get("/metrics/overview", response_model=OverviewResponse)
def get_metrics_overview(minutes: int = 60, db: Session = Depends(get_db)):
    minutes = max(5, min(minutes, 24 * 60))
    return _compute_overview(db, minutes)


@app.get("/metrics/trending-products", response_model=List[ProductTrendResponse])
def get_trending_products(limit: int = 10, db: Session = Depends(get_db)):
    rows = (
        db.query(models.ProductSales)
        .order_by(models.ProductSales.total_revenue.desc())
        .limit(limit)
        .all()
    )
    return [
        {
            "product_id": row.product_id,
            "category": row.category,
            "total_revenue": _to_float(row.total_revenue),
            "purchase_count": int(row.purchase_count or 0),
        }
        for row in rows
    ]


@app.get("/alerts/recent", response_model=List[AlertResponse])
def get_recent_alerts(limit: int = 20, db: Session = Depends(get_db)):
    return (
        db.query(models.AnomalyAlert)
        .order_by(models.AnomalyAlert.created_at.desc())
        .limit(limit)
        .all()
    )


@app.get("/reports/executive-summary")
def get_executive_summary(minutes: int = 60, db: Session = Depends(get_db)):
    overview = _compute_overview(db, minutes)
    alerts = (
        db.query(models.AnomalyAlert)
        .order_by(models.AnomalyAlert.created_at.desc())
        .limit(5)
        .all()
    )
    top_products = (
        db.query(models.ProductSales)
        .order_by(models.ProductSales.total_revenue.desc())
        .limit(3)
        .all()
    )

    highlight_lines = [
        (
            f"Revenue ${overview['total_revenue']:.2f} from {overview['total_purchases']} purchases "
            f"over the last {overview['window_minutes']} minutes."
        ),
        f"Conversion rate is {overview['conversion_rate']:.1%} with abandonment at {overview['abandonment_rate']:.1%}.",
    ]

    if top_products:
        product_text = ", ".join(
            [
                f"#{row.product_id} ({row.category}) ${_to_float(row.total_revenue):.2f}"
                for row in top_products
            ]
        )
        highlight_lines.append(f"Top products by revenue: {product_text}.")

    if alerts:
        highlight_lines.append(f"Recent alerts: {len(alerts)} (latest: {alerts[0].alert_type}).")
    else:
        highlight_lines.append("No anomaly alerts were triggered in the recent window.")

    recommendations = [
        "Monitor checkout conversion and payment latency when purchase_drop appears.",
        "Validate campaign and bot-filtering rules when traffic_spike appears.",
        "Review pricing and product-page friction for high abandonment sessions.",
    ]

    return {
        "window_minutes": minutes,
        "highlights": highlight_lines,
        "recommendations": recommendations,
    }


def _assistant_business_response(question: str, db: Session):
    q = question.lower()
    overview_30 = _compute_overview(db, 30)
    overview_60 = _compute_overview(db, 60)
    alerts = (
        db.query(models.AnomalyAlert)
        .order_by(models.AnomalyAlert.created_at.desc())
        .limit(5)
        .all()
    )
    top_products = (
        db.query(models.ProductSales)
        .order_by(models.ProductSales.total_revenue.desc())
        .limit(5)
        .all()
    )

    top_product_payload = [
        {
            "product_id": row.product_id,
            "category": row.category,
            "total_revenue": _to_float(row.total_revenue),
            "purchase_count": int(row.purchase_count or 0),
        }
        for row in top_products
    ]
    alert_payload = [
        {
            "alert_type": a.alert_type,
            "severity": a.severity,
            "message": a.message,
            "minute": a.minute.isoformat() if a.minute else None,
        }
        for a in alerts
    ]

    ai_context = {
        "overview_30": overview_30,
        "overview_60": overview_60,
        "top_products": top_product_payload,
        "alerts": alert_payload,
    }
    ai_result = run_langchain_assistant(question, ai_context, POSTGRES_URL)
    if ai_result:
        return ai_result

    evidence = []
    next_actions = []
    trace = [
        "MetricsAgent: loaded KPI aggregates for 30m and 60m windows",
        "TrendAgent: compared conversion and abandonment movement",
        "AlertAgent: reviewed latest anomaly alerts",
        "RecommendationAgent: produced operational actions",
    ]

    if "trending" in q or "top" in q:
        product_text = ", ".join(
            [
                f"product {row.product_id} ({row.category}) at ${_to_float(row.total_revenue):.2f}"
                for row in top_products[:3]
            ]
        ) if top_products else "No purchases captured yet"
        answer = f"Top trending products by revenue are: {product_text}."
        evidence.append(f"Computed from {len(top_products)} product aggregates")
        next_actions.append("Promote the top category with inventory checks")
    elif "drop" in q or "conversion" in q:
        answer = (
            f"In the last 30 minutes, conversion is {overview_30['conversion_rate']:.1%} with "
            f"{overview_30['total_purchases']} purchases. Compare that with {overview_60['conversion_rate']:.1%} "
            "over the last hour for context."
        )
        evidence.append(
            f"30m purchases={overview_30['total_purchases']}, 60m purchases={overview_60['total_purchases']}"
        )
        if alerts:
            evidence.append(f"Latest alert: {alerts[0].alert_type} ({alerts[0].severity})")
        next_actions.extend([
            "Check checkout and payment service health",
            "Inspect traffic source quality and campaign shifts",
        ])
    elif "abandon" in q or "cart" in q:
        answer = (
            f"Cart abandonment is {overview_30['abandonment_rate']:.1%} in the last 30 minutes "
            f"with {overview_30['total_add_to_cart']} add-to-cart events."
        )
        evidence.append(
            f"30m add_to_cart={overview_30['total_add_to_cart']}, purchases={overview_30['total_purchases']}"
        )
        next_actions.extend([
            "Audit checkout steps for friction",
            "Review discount and shipping messaging",
        ])
    else:
        answer = (
            f"Last hour summary: revenue is ${overview_60['total_revenue']:.2f}, conversion is "
            f"{overview_60['conversion_rate']:.1%}, abandonment is {overview_60['abandonment_rate']:.1%}."
        )
        evidence.append(f"Active alerts in feed: {len(alerts)}")
        next_actions.append("Run /agent/business-update for a full multi-step analysis")

    if not next_actions:
        next_actions.append("Continue monitoring the anomaly feed")

    return {
        "answer": answer,
        "evidence": evidence,
        "next_actions": next_actions,
        "workflow_trace": trace,
    }


@app.post("/assistant/query", response_model=AssistantResponse)
def assistant_query(payload: AssistantQueryRequest, db: Session = Depends(get_db)):
    return _assistant_business_response(payload.question, db)


@app.get("/agent/business-update")
def business_update(minutes: int = 60, db: Session = Depends(get_db)):
    minutes = max(15, min(minutes, 24 * 60))
    metrics = _compute_overview(db, minutes)
    previous = _compute_overview(db, minutes * 2)
    alerts = (
        db.query(models.AnomalyAlert)
        .order_by(models.AnomalyAlert.created_at.desc())
        .limit(5)
        .all()
    )
    top_products = (
        db.query(models.ProductSales)
        .order_by(models.ProductSales.total_revenue.desc())
        .limit(3)
        .all()
    )

    alert_payload = [
        {
            "alert_type": a.alert_type,
            "severity": a.severity,
            "message": a.message,
            "minute": a.minute.isoformat() if a.minute else None,
        }
        for a in alerts
    ]
    top_product_payload = [
        {
            "product_id": p.product_id,
            "category": p.category,
            "total_revenue": _to_float(p.total_revenue),
            "purchase_count": int(p.purchase_count or 0),
        }
        for p in top_products
    ]

    graph_result = run_langgraph_business_update(
        question=f"Give me a business update for the last {minutes} minutes",
        metrics=metrics,
        previous=previous,
        alerts=alert_payload,
        top_products=top_product_payload,
    )
    if graph_result:
        return {"agents": graph_result, "engine": "langgraph"}

    metric_agent = {
        "window": minutes,
        "revenue": metrics["total_revenue"],
        "purchases": metrics["total_purchases"],
        "conversion_rate": metrics["conversion_rate"],
        "abandonment_rate": metrics["abandonment_rate"],
    }
    trend_agent = {
        "conversion_delta": metrics["conversion_rate"] - previous["conversion_rate"],
        "abandonment_delta": metrics["abandonment_rate"] - previous["abandonment_rate"],
        "revenue_delta": metrics["total_revenue"] - previous["total_revenue"],
    }
    alert_agent = [
        {
            "alert_type": a.alert_type,
            "severity": a.severity,
            "message": a.message,
            "minute": a.minute,
        }
        for a in alerts
    ]
    recommendation_agent = [
        "Check checkout error rates if conversion_delta is negative.",
        "Review source attribution when traffic anomalies appear.",
        "Double down on top-performing categories with healthy inventory.",
    ]
    top_product_text = ", ".join(
        [f"#{p.product_id} ({p.category}) ${_to_float(p.total_revenue):.2f}" for p in top_products]
    ) if top_products else "No top products yet"

    report_agent = (
        f"RetailBrain update ({minutes}m): revenue ${metrics['total_revenue']:.2f}, "
        f"conversion {metrics['conversion_rate']:.1%}, abandonment {metrics['abandonment_rate']:.1%}. "
        f"Top products: {top_product_text}. Alerts in focus: {len(alerts)}."
    )

    return {
        "agents": {
            "metrics_agent": metric_agent,
            "trend_agent": trend_agent,
            "alert_agent": alert_agent,
            "recommendation_agent": recommendation_agent,
            "report_agent": report_agent,
        }
    }
