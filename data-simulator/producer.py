# data-simulator/producer.py
import json
import time
import random
import uuid
import signal
import sys
from kafka import KafkaProducer

BROKER = "localhost:9092"  # change if needed
RATE_SECONDS = 1.0         # events per second (sleep time)

producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=5,
    retries=3
)

users = [f"user_{i}" for i in range(1, 51)]
products = [f"product_{i}" for i in range(1, 21)]
event_types = ["view", "add_to_cart", "purchase"]
running = True

def handle_exit(sig, frame):
    global running
    print("\nStopping producer...")
    running = False

signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)

try:
    while running:
        event = {
            "event_id": str(uuid.uuid4()),
            "user_id": random.choice(users),
            "product_id": random.choice(products),
            "event_type": random.choices(event_types, weights=[0.6, 0.2, 0.2])[0],
            "timestamp": time.time()
        }

        # decide topic: payments get purchase events (example)
        if event["event_type"] == "purchase":
            topic = "payments"
        else:
            topic = "events"

        future = producer.send(topic, value=event)
        try:
            # optional: block for confirm (timeout in seconds)
            record_metadata = future.get(timeout=10)
        except Exception as e:
            print("Send failed:", e)

        print(f"Sent to {topic}:", event)
        time.sleep(RATE_SECONDS)
except Exception as e:
    print("Producer error:", e)
finally:
    producer.flush()
    producer.close()
    print("Producer closed.")
