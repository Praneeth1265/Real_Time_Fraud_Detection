from kafka import KafkaProducer
from faker import Faker
import json
import time
import random
from datetime import datetime

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC = "transactions"

print("Producing fake transactions..")

while True:
    transaction = {
        "user_id": random.randint(1, 1000),
        "amount": round(random.uniform(10, 5000), 2),
        "merchant": fake.company(),
        "location": fake.city(),
        "timestamp": datetime.utcnow().isoformat()
    }

    producer.send(TOPIC, transaction)
    print("Sent:", transaction)

    time.sleep(1)
