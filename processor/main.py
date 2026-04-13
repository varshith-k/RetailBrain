import os
import json
import time
import io
from datetime import datetime, timedelta
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import Json
from minio import Minio

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC_NAME = os.getenv('TOPIC_NAME', 'events')
POSTGRES_URL = os.getenv('POSTGRES_URL', 'postgresql://user:password@localhost:5432/ecommerce_db')
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')

def get_postgres_connection():
    while True:
        try:
            conn = psycopg2.connect(POSTGRES_URL)
            print("Connected to PostgreSQL")
            return conn
        except Exception as e:
            print(f"Failed to connect to Postgres: {e}. Retrying...")
            time.sleep(5)

def get_minio_client():
    while True:
        try:
            client = Minio(
                MINIO_ENDPOINT.replace('http://', '').replace('https://', ''),
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                secure=False
            )
            if not client.bucket_exists("raw-events"):
                client.make_bucket("raw-events")
            print("Connected to MinIO")
            return client
        except Exception as e:
            print(f"Failed to connect to MinIO: {e}. Retrying...")
            time.sleep(5)

def init_db(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sales_stats (
            minute TIMESTAMP PRIMARY KEY,
            total_sales DECIMAL(10, 2) DEFAULT 0,
            purchase_count INT DEFAULT 0
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS minute_event_stats (
            minute TIMESTAMP PRIMARY KEY,
            page_views INT DEFAULT 0,
            add_to_cart INT DEFAULT 0,
            purchases INT DEFAULT 0,
            revenue DECIMAL(10, 2) DEFAULT 0
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS product_sales (
            product_id INT PRIMARY KEY,
            category TEXT,
            total_revenue DECIMAL(10, 2) DEFAULT 0,
            purchase_count INT DEFAULT 0,
            last_purchase_at TIMESTAMP
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS anomaly_alerts (
            id SERIAL PRIMARY KEY,
            minute TIMESTAMP NOT NULL,
            alert_type TEXT NOT NULL,
            severity TEXT NOT NULL,
            message TEXT NOT NULL,
            details JSONB,
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE (minute, alert_type)
        );
    """)
    conn.commit()
    cur.close()


def upsert_minute_stats(cur, minute_bucket, event, price):
    event_type = event['event_type']
    page_view_inc = 1 if event_type == 'page_view' else 0
    add_to_cart_inc = 1 if event_type == 'add_to_cart' else 0
    purchase_inc = 1 if event_type == 'purchase' else 0
    revenue_inc = price if event_type == 'purchase' else 0

    cur.execute(
        """
        INSERT INTO minute_event_stats (minute, page_views, add_to_cart, purchases, revenue)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (minute)
        DO UPDATE SET
            page_views = minute_event_stats.page_views + EXCLUDED.page_views,
            add_to_cart = minute_event_stats.add_to_cart + EXCLUDED.add_to_cart,
            purchases = minute_event_stats.purchases + EXCLUDED.purchases,
            revenue = minute_event_stats.revenue + EXCLUDED.revenue;
        """,
        (minute_bucket, page_view_inc, add_to_cart_inc, purchase_inc, revenue_inc),
    )


def upsert_purchase_aggregates(cur, minute_bucket, event, price, timestamp):
    cur.execute(
        """
        INSERT INTO sales_stats (minute, total_sales, purchase_count)
        VALUES (%s, %s, 1)
        ON CONFLICT (minute)
        DO UPDATE SET
            total_sales = sales_stats.total_sales + EXCLUDED.total_sales,
            purchase_count = sales_stats.purchase_count + 1;
        """,
        (minute_bucket, price),
    )
    cur.execute(
        """
        INSERT INTO product_sales (product_id, category, total_revenue, purchase_count, last_purchase_at)
        VALUES (%s, %s, %s, 1, %s)
        ON CONFLICT (product_id)
        DO UPDATE SET
            category = EXCLUDED.category,
            total_revenue = product_sales.total_revenue + EXCLUDED.total_revenue,
            purchase_count = product_sales.purchase_count + 1,
            last_purchase_at = EXCLUDED.last_purchase_at;
        """,
        (event['product_id'], event.get('category', 'unknown'), price, timestamp),
    )


def insert_alert(cur, minute_bucket, alert_type, severity, message, details):
    cur.execute(
        """
        INSERT INTO anomaly_alerts (minute, alert_type, severity, message, details)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (minute, alert_type)
        DO UPDATE SET
            severity = EXCLUDED.severity,
            message = EXCLUDED.message,
            details = EXCLUDED.details,
            created_at = NOW();
        """,
        (minute_bucket, alert_type, severity, message, Json(details)),
    )


def detect_anomalies(cur, minute_bucket):
    cur.execute(
        """
        SELECT page_views, add_to_cart, purchases, revenue
        FROM minute_event_stats
        WHERE minute = %s;
        """,
        (minute_bucket,),
    )
    current = cur.fetchone()
    if not current:
        return

    cur_page_views, cur_add_to_cart, cur_purchases, _ = current
    current_abandonment = 0.0
    if cur_add_to_cart > 0:
        current_abandonment = max(cur_add_to_cart - cur_purchases, 0) / cur_add_to_cart

    window_start = minute_bucket - timedelta(minutes=30)
    cur.execute(
        """
        SELECT
            COALESCE(AVG(page_views), 0),
            COALESCE(AVG(purchases), 0),
            COALESCE(
                AVG(
                    CASE
                        WHEN add_to_cart > 0 THEN GREATEST(add_to_cart - purchases, 0)::float / add_to_cart
                        ELSE 0
                    END
                ),
                0
            )
        FROM minute_event_stats
        WHERE minute >= %s AND minute < %s;
        """,
        (window_start, minute_bucket),
    )
    avg_page_views, avg_purchases, avg_abandonment = cur.fetchone()

    if avg_page_views >= 5 and cur_page_views >= avg_page_views * 2:
        insert_alert(
            cur,
            minute_bucket,
            "traffic_spike",
            "high",
            f"Traffic spike detected: page views are {cur_page_views} vs baseline {avg_page_views:.1f}.",
            {
                "current_page_views": cur_page_views,
                "baseline_page_views": round(float(avg_page_views), 2),
            },
        )

    if avg_purchases >= 2 and cur_purchases <= avg_purchases * 0.5:
        insert_alert(
            cur,
            minute_bucket,
            "purchase_drop",
            "high",
            f"Purchase drop detected: purchases are {cur_purchases} vs baseline {avg_purchases:.1f}.",
            {
                "current_purchases": cur_purchases,
                "baseline_purchases": round(float(avg_purchases), 2),
            },
        )

    if cur_add_to_cart >= 5 and current_abandonment >= 0.75 and current_abandonment >= (avg_abandonment * 1.5):
        insert_alert(
            cur,
            minute_bucket,
            "abandonment_spike",
            "medium",
            (
                "Cart abandonment spike detected: current rate "
                f"{current_abandonment:.0%} vs baseline {avg_abandonment:.0%}."
            ),
            {
                "current_abandonment_rate": round(float(current_abandonment), 4),
                "baseline_abandonment_rate": round(float(avg_abandonment), 4),
            },
        )

def process_events():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='processor-group'
    )

    pg_conn = get_postgres_connection()
    init_db(pg_conn)
    minio_client = get_minio_client()

    batch = []
    last_upload_time = time.time()

    print("Starting to process events...")

    for message in consumer:
        event = message.value
        batch.append(event)
        timestamp = datetime.fromtimestamp(event['timestamp'])
        minute_bucket = timestamp.replace(second=0, microsecond=0)
        price = float(event.get('price', 0))

        cur = pg_conn.cursor()
        upsert_minute_stats(cur, minute_bucket, event, price)

        if event['event_type'] == 'purchase':
            upsert_purchase_aggregates(cur, minute_bucket, event, price, timestamp)
            print(f"Processed purchase: ${price}")

        detect_anomalies(cur, minute_bucket)
        pg_conn.commit()
        cur.close()

        # Archival to MinIO (Batching)
        if len(batch) >= 100 or (time.time() - last_upload_time) > 60:
            if batch:
                data = json.dumps(batch).encode('utf-8')
                object_name = f"events_{int(time.time())}.json"
                minio_client.put_object(
                    "raw-events",
                    object_name,
                    io.BytesIO(data),
                    len(data),
                    content_type="application/json"
                )
                print(f"Archived {len(batch)} events to MinIO: {object_name}")
                batch = []
                last_upload_time = time.time()

if __name__ == "__main__":
    process_events()
