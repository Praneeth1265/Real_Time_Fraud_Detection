import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

message = {
    "user_id": 1,
    "amount": 100,
    "timestamp": "2023-10-27T10:00:00"
}

producer.send("test-topic", message)
producer.flush()

print("Message sent:", message)
