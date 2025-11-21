import os
import json
import time
import io
from datetime import datetime
from kafka import KafkaConsumer
import psycopg2
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
    conn.commit()
    cur.close()

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

        # Real-time Aggregation for Purchases
        if event['event_type'] == 'purchase':
            timestamp = datetime.fromtimestamp(event['timestamp'])
            minute_bucket = timestamp.replace(second=0, microsecond=0)
            price = event['price']
            
            cur = pg_conn.cursor()
            cur.execute("""
                INSERT INTO sales_stats (minute, total_sales, purchase_count)
                VALUES (%s, %s, 1)
                ON CONFLICT (minute)
                DO UPDATE SET
                    total_sales = sales_stats.total_sales + EXCLUDED.total_sales,
                    purchase_count = sales_stats.purchase_count + 1;
            """, (minute_bucket, price))
            pg_conn.commit()
            cur.close()
            print(f"Processed purchase: ${price}")

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
