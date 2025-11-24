# data-simulator/consumer.py
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "events", "payments",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="latest", # latest or earliest
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

print("Listening to events...")
for msg in consumer:
    print(msg.value)
