# RetailBrain: AI-Powered Real-Time Commerce Intelligence

RetailBrain combines data engineering, backend engineering, and an AI analytics layer.
It streams simulated commerce events through Redpanda/Kafka, processes metrics in real time, detects anomalies, and provides a natural-language business assistant plus multi-agent style business updates.

## Architecture

1. **Producer**: Generates synthetic commerce events with category and channel metadata.
2. **Redpanda (Kafka API)**: Event stream backbone.
3. **Processor**:
   - Aggregates minute-level metrics.
   - Builds product-level revenue trends.
   - Detects anomalies (traffic spikes, purchase drops, abandonment spikes).
   - Archives raw event batches to MinIO.
4. **PostgreSQL**: Stores analytics and anomaly tables.
5. **FastAPI**:
   - Metrics and trending endpoints.
   - Alert and executive summary endpoints.
   - Natural-language analytics assistant endpoint.
   - Multi-agent workflow endpoint for business updates.
6. **Streamlit Dashboard**: Live charts, anomaly feed, and AI analyst chat panel.

## Tech Stack

- Python 3.9
- Redpanda (Kafka compatible)
- PostgreSQL
- MinIO
- FastAPI + SQLAlchemy
- Streamlit + Plotly
- Docker Compose

## Core Endpoints

- `GET /sales`
- `GET /metrics/overview`
- `GET /metrics/trending-products`
- `GET /alerts/recent`
- `GET /reports/executive-summary`
- `POST /assistant/query`
- `GET /agent/business-update`

## Run

```bash
docker-compose up --build
```

## Access

- Dashboard: http://localhost:8501
- API docs: http://localhost:8000/docs
- MinIO console: http://localhost:9001

MinIO credentials:
- Username: `minioadmin`
- Password: `minioadmin`

## Data Flow

1. Producer publishes events to topic `events`.
2. Processor consumes events and updates:
   - `sales_stats`
   - `minute_event_stats`
   - `product_sales`
   - `anomaly_alerts`
3. Processor batches raw events to MinIO bucket `raw-events`.
4. API serves live analytics and assistant workflows.
5. Dashboard renders charts, anomalies, and AI responses.
