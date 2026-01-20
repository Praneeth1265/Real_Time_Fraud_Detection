import json
import time
import random
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from faker import Faker
from kafka.errors import KafkaError
from datetime import datetime

fake = Faker()

KAFKA_BROKER = "kafka:9092"
TOPIC_NAME = "transactions"

LOCATIONS = ["NY", "LON", "DEL", "BLR", "SFO"]

# Real coordinates
LOCATION_COORDS = {
    "NY":  (40.7128, -74.0060),
    "LON": (51.5074, -0.1278),
    "DEL": (28.6139, 77.2090),
    "BLR": (12.9716, 77.5946),
    "SFO": (37.7749, -122.4194)
}

# Store last known location per user (IN PRODUCER MEMORY)
user_last_location = {}

# ---------------- Kafka Admin ----------------
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

# ---------------- Producer ----------------
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Producing realistic transactions with geo behavior...")

TX_PER_SECOND = 100

while True:
    for _ in range(TX_PER_SECOND):

        user_id = fake.random_int(min=100, max=120)

        # -------- Amount behavior --------
        if random.random() < 0.05:   # 5% fraud spikes
            amount = round(random.uniform(5000, 10000), 2)
        else:
            amount = round(random.uniform(10, 1000), 2)

        # -------- Location behavior --------
        if user_id not in user_last_location:
            # First transaction → random location
            location = random.choice(LOCATIONS)
        else:
            # 80% same location, 20% jump city
            if random.random() < 0.8:
                location = user_last_location[user_id]
            else:
                location = random.choice(LOCATIONS)

        # Save last location
        user_last_location[user_id] = location

        lat, lon = LOCATION_COORDS[location]

        transaction = {
            "user_id": user_id,
            "amount": amount,
            "location": location,
            "lat": lat,
            "lon": lon,
            "timestamp": datetime.utcnow().isoformat()
        }

        producer.send(TOPIC_NAME, transaction)
        print("Sent:", transaction)

    time.sleep(1)

