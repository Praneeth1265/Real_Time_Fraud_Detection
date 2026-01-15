import json
import time
import random
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from faker import Faker
from kafka.errors import KafkaError
from datetime import datetime
import random

fake = Faker()
KAFKA_BROKER = "kafka:9092"
TOPIC_NAME = "transactions"
LOCATIONS = ["NY", "LON", "DEL", "BLR", "SFO"]

max_retries = 10
for attempt in range(max_retries):
    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
        break
    except KafkaError:
        print(f"Kafka not ready, retrying {attempt+1}/{max_retries}...")
        time.sleep(2)
else:
    raise Exception("Kafka broker not available after retries")

topic_list = [NewTopic(name=TOPIC_NAME, num_partitions=1, replication_factor=1)]
try:
    admin.create_topics(new_topics=topic_list, validate_only=False)
    print(f"Topic '{TOPIC_NAME}' created successfully")
except Exception:
    print(f"Topic '{TOPIC_NAME}' already exists")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Producing fake transactions..")
TX_PER_SECOND = 100

while True:
    for _ in range(TX_PER_SECOND):
        user_id = fake.random_int(min=100, max=120)
        amount = round(random.uniform(5, 50), 2)
        if random.random() < 0.05:  # rare spike
            amount = round(random.uniform(5000, 10000), 2)

        transaction = {
	    "user_id": user_id,
	    "amount": round(random.uniform(10, 1000), 2),
	    "location": random.choice(LOCATIONS),
	    "timestamp": datetime.utcnow().isoformat()
	}

        producer.send(TOPIC_NAME, transaction)
        print("Sent:", transaction)

    time.sleep(1)
    

