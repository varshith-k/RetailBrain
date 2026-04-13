import time
import json
import random
import os
from kafka import KafkaProducer
from faker import Faker

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC_NAME = os.getenv('TOPIC_NAME', 'events')

fake = Faker()

PRODUCT_CATEGORIES = [
    "electronics",
    "fashion",
    "home",
    "beauty",
    "sports",
]

PRODUCT_PRICE_RANGES = {
    "electronics": (75.0, 900.0),
    "fashion": (20.0, 250.0),
    "home": (15.0, 450.0),
    "beauty": (8.0, 120.0),
    "sports": (18.0, 500.0),
}

def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print(f"Connected to Kafka at {KAFKA_BROKER}")
            return producer
        except Exception as e:
            print(f"Failed to connect to Kafka: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def generate_event():
    event_type = random.choice(['page_view', 'add_to_cart', 'purchase'])
    category = random.choice(PRODUCT_CATEGORIES)
    price_low, price_high = PRODUCT_PRICE_RANGES[category]
    
    event = {
        'event_id': fake.uuid4(),
        'event_type': event_type,
        'session_id': fake.uuid4(),
        'user_id': fake.random_int(min=1, max=100),
        'product_id': fake.random_int(min=1000, max=2000),
        'category': category,
        'device_type': random.choice(['web', 'ios', 'android']),
        'referrer': random.choice(['search', 'email', 'social', 'direct']),
        'timestamp': time.time()
    }

    if event_type == 'purchase':
        event['price'] = round(random.uniform(price_low, price_high), 2)
        event['quantity'] = random.randint(1, 5)
    
    return event

def main():
    producer = create_producer()
    
    print(f"Starting to produce events to topic '{TOPIC_NAME}'...")
    
    while True:
        event = generate_event()
        producer.send(TOPIC_NAME, event)
        print(f"Sent: {event}")
        
        # Simulate traffic pattern
        time.sleep(random.uniform(0.1, 1.0))

if __name__ == "__main__":
    main()
