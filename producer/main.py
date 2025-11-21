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
    
    event = {
        'event_id': fake.uuid4(),
        'event_type': event_type,
        'user_id': fake.random_int(min=1, max=100),
        'product_id': fake.random_int(min=1000, max=2000),
        'timestamp': time.time()
    }

    if event_type == 'purchase':
        event['price'] = round(random.uniform(10.0, 500.0), 2)
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
