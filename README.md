# Real-Time E-Commerce Analytics Platform

This project is a full-stack Data Engineering and Backend application that simulates an e-commerce environment, processes real-time events, and visualizes sales metrics.

## Architecture

1.  **Producer**: Generates synthetic events (page views, add to cart, purchases) and streams them to **Redpanda** (Kafka).
2.  **Processor**: Consumes events from Kafka, aggregates sales data in real-time, stores raw logs in **MinIO** (Data Lake), and saves aggregated stats to **PostgreSQL** (Data Warehouse).
3.  **Backend API**: A **FastAPI** service that queries PostgreSQL and serves analytics data.
4.  **Dashboard**: A **Streamlit** application that visualizes real-time sales trends.

## Tech Stack

-   **Language**: Python 3.9
-   **Streaming**: Redpanda (Kafka API)
-   **Storage**: PostgreSQL, MinIO (S3 Compatible)
-   **Backend**: FastAPI, SQLAlchemy
-   **Frontend**: Streamlit, Plotly
-   **Infrastructure**: Docker, Docker Compose

## Prerequisites

-   Docker and Docker Compose installed.

## How to Run

1.  **Clone the repository** (if applicable) or navigate to the project root.

2.  **Start the services**:
    ```bash
    docker-compose up --build
    ```

3.  **Access the services**:
    -   **Dashboard**: [http://localhost:8501](http://localhost:8501) - View real-time charts.
    -   **API Docs**: [http://localhost:8000/docs](http://localhost:8000/docs) - Test the API endpoints.
    -   **MinIO Console**: [http://localhost:9001](http://localhost:9001) - Login with `minioadmin` / `minioadmin` to see raw event logs.

## Project Structure

```
.
├── api/                # FastAPI Backend
├── dashboard/          # Streamlit Frontend
├── producer/           # Event Generator
├── processor/          # Stream Processor
├── docker-compose.yml  # Infrastructure Setup
└── README.md
```

## Data Flow

1.  `producer` sends JSON events to `events` topic.
2.  `processor` reads `events` topic.
    -   If `purchase`, updates `sales_stats` table in Postgres.
    -   Batches all events and uploads JSON files to `raw-events` bucket in MinIO.
3.  `dashboard` polls `api` every 5 seconds.
4.  `api` queries `sales_stats` table in Postgres and returns JSON.
