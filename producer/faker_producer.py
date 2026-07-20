import json
import time
import random
import math
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

KAFKA_BROKER = "kafka:9092"
TOPIC_NAME = "transactions"

LOCATIONS = ["NY", "LON", "DEL", "BLR", "SFO", "MIA", "CHI", "LA"]
LOCATION_COORDS = {
    "NY":  (40.7128, -74.0060),
    "LON": (51.5074, -0.1278),
    "DEL": (28.6139, 77.2090),
    "BLR": (12.9716, 77.5946),
    "SFO": (37.7749, -122.4194),
    "MIA": (25.7617, -80.1918),
    "CHI": (41.8781, -87.6298),
    "LA":  (34.0522, -118.2437)
}

MERCHANTS = [
    "Amazon", "Walmart", "Shell", "Starbucks", "BestBuy",
    "Luxury_Store", "Casino", "Foreign_Site", "Electronics_Hub", "Grocery"
]

# ---------------- Kafka Admin ----------------
for _ in range(10):
    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
        break
    except KafkaError:
        time.sleep(2)
else:
    raise RuntimeError("Kafka not available")

try:
    admin.create_topics(
        [NewTopic(TOPIC_NAME, num_partitions=1, replication_factor=1)]
    )
except Exception:
    pass

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ---------------- User State ----------------
user_profiles = {}

TX_PER_SECOND = 50

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dl/2)**2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))

print("Producing realistic transactions...")

while True:
    now = datetime.utcnow()

    for _ in range(TX_PER_SECOND):
        user_id = random.randint(100, 150)

        if user_id not in user_profiles:
            home = random.choice(LOCATIONS)
            user_profiles[user_id] = {
                "home": home,
                "lat": LOCATION_COORDS[home][0],
                "lon": LOCATION_COORDS[home][1],
                "last_ts": now - timedelta(hours=random.uniform(1, 72)),
                "preferred_merchants": random.sample(MERCHANTS, 4)
            }

        user = user_profiles[user_id]

        # Time progression (2s – 30min)
        delta_seconds = random.uniform(2, 1800)
        timestamp = user["last_ts"] + timedelta(seconds=delta_seconds)

        # Decide fraud
        is_fraud = random.random() < 0.02

        if is_fraud:
            # Impossible travel
            location = random.choice([l for l in LOCATIONS if l != user["home"]])
            lat, lon = LOCATION_COORDS[location]
            merchant = random.choice(["Casino", "Foreign_Site", "Luxury_Store"])
            amount = round(random.uniform(1000, 8000), 2)
        else:
            # Normal behavior
            merchant = (
                random.choice(user["preferred_merchants"])
                if random.random() < 0.7
                else random.choice(MERCHANTS)
            )

            if merchant == "Luxury_Store":
                amount = round(random.uniform(500, 5000), 2)
            elif merchant in ["Starbucks", "Shell"]:
                amount = round(random.uniform(5, 50), 2)
            else:
                amount = round(random.uniform(10, 800), 2)

            # Travel only if enough time passed
            if random.random() < 0.05:
                location = random.choice(LOCATIONS)
            else:
                location = user["home"]

            lat, lon = LOCATION_COORDS[location]

            # Reject unrealistic normal travel
            dist = haversine(user["lat"], user["lon"], lat, lon)
            hours = delta_seconds / 3600
            if hours > 0 and dist / hours > 1200:
                lat, lon = user["lat"], user["lon"]
                location = user["home"]

        transaction = {
            "user_id": user_id,
            "amount": amount,
            "merchant": merchant,
            "location": location,
            "lat": lat,
            "lon": lon,
            "timestamp": timestamp.isoformat()
        }

        producer.send(TOPIC_NAME, transaction)

        # Update state
        user["lat"] = lat
        user["lon"] = lon
        user["last_ts"] = timestamp

        if random.random() < 0.01:
            tag = "FRAUD" if is_fraud else "NORMAL"
            print(f"{tag} | User {user_id} | ${amount:.2f} | {merchant} | {location}")

    time.sleep(1)

