# RetailBrain: AI-Powered Real-Time Commerce Intelligence

RetailBrain combines data engineering, backend engineering, and an AI analytics layer.
It streams simulated commerce events through Redpanda/Kafka, processes metrics in real time, detects anomalies, and provides a natural-language business assistant plus multi-agent style business updates.

## Implementation Status

- Current AI layer includes real LangChain + LangGraph runtime integration in the API service.
- If `OPENAI_API_KEY` is not set, the system automatically falls back to deterministic local logic.

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
   - Natural-language analytics assistant endpoint (LangChain SQL + LLM reasoning).
   - Multi-agent workflow endpoint for business updates (LangGraph orchestration).
6. **Streamlit Dashboard**: Live charts, anomaly feed, and AI analyst chat panel.

## Tech Stack

- Python 3.9
- Redpanda (Kafka compatible)
- PostgreSQL
- MinIO
- FastAPI + SQLAlchemy
- LangChain + LangGraph + OpenAI SDK integration
- Streamlit + Plotly
- Docker Compose

## AI Runtime Configuration

Set these environment variables before running (or put them in a `.env` file used by Docker Compose):

- `OPENAI_API_KEY=<your_key>`
- `OPENAI_MODEL=gpt-4o-mini` (or another compatible model)
- `ENABLE_REAL_AI=true`

Without API key, endpoints still work using deterministic fallback logic.

## Where LangChain/LangGraph Are Used

- `api/ai_engine.py`
   - `run_langchain_assistant(...)`: generates read-only SQL from natural language and explains results.
   - `run_langgraph_business_update(...)`: executes a multi-node graph (metrics -> trend -> recommendation -> report).
- `api/main.py`
   - `/assistant/query` calls LangChain path first, then fallback logic if unavailable.
   - `/agent/business-update` calls LangGraph path first, then fallback logic if unavailable.

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
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (`admin` / `admin`)

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

## Monitoring (Prometheus + Grafana)

The stack now includes built-in observability for:

- Kafka lag: `kafka_consumer_lag`
- Processing latency: `event_processing_duration_seconds`
- API latency: `api_request_latency_seconds`
- Anomaly rate: `anomalies_detected_total` and `anomaly_rate_per_minute`

Preconfigured components:

- Prometheus scrape config: `monitoring/prometheus.yml`
- Grafana datasource provisioning: `monitoring/grafana/provisioning/datasources/prometheus.yml`
- Grafana dashboard provisioning: `monitoring/grafana/provisioning/dashboards/dashboards.yml`
- Dashboard JSON: `monitoring/grafana/dashboards/retailbrain-observability.json`

After startup, open Grafana and navigate to folder `RetailBrain` to view `RetailBrain Observability`.
