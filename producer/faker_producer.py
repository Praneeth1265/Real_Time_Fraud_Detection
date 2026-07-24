import heapq
import json
import math
import os
import random
import time
import uuid
from datetime import datetime, timedelta

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError

fake = Faker()

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
TOPIC_RAW = os.environ.get("TOPIC_RAW", "transactions.raw")
TOPIC_GROUND_TRUTH = os.environ.get("TOPIC_GROUND_TRUTH", "ground_truth")

INSTANCE_ID = os.environ.get("INSTANCE_ID", "producer")
TX_PER_SECOND = int(os.environ.get("TX_PER_SECOND", "12"))
GT_DELAY_MIN_SECONDS = float(os.environ.get("GT_DELAY_MIN_SECONDS", "5"))
GT_DELAY_MAX_SECONDS = float(os.environ.get("GT_DELAY_MAX_SECONDS", "30"))

# Each producer instance owns a disjoint slice of the simulated user_id
# space. Two producers both hitting the same users would each advance that
# user's "last transaction time" independently and out of causal order --
# since the feature service's Redis state is shared/global, that produces
# negative hours_since_last_tx for whichever transaction lands second,
# which blows up log1p(). Partitioning users avoids the collision entirely
# and still gives two independently-scaling producers.
USER_ID_MIN = int(os.environ.get("USER_ID_MIN", "100"))
USER_ID_MAX = int(os.environ.get("USER_ID_MAX", "150"))

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


def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dl / 2) ** 2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def connect_producer():
    for _ in range(15):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
        except KafkaError:
            print(f"[{INSTANCE_ID}] Kafka not ready, retrying...", flush=True)
            time.sleep(2)
    raise RuntimeError("Kafka not available")


def main():
    producer = connect_producer()

    # User state (per-producer-instance; purely to make generated data look
    # realistic, not the same thing as the feature-service's Redis state).
    user_profiles = {}

    # Ground-truth confirmations pending publication, scheduled for a
    # configurable delay after the transaction was produced (simulates
    # real-world delayed fraud confirmation). Each producer only ever
    # confirms the transactions it itself created, so two producer
    # instances can never publish a duplicate ground-truth record for the
    # same transaction_id -- no cross-process coordination needed.
    pending_confirmations = []  # heap of (ready_at, transaction_id, actual_label)

    print(f"[{INSTANCE_ID}] Producing realistic transactions...", flush=True)

    while True:
        now = datetime.utcnow()

        for _ in range(TX_PER_SECOND):
            user_id = random.randint(USER_ID_MIN, USER_ID_MAX)

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

            # Time progression (2s - 30min)
            delta_seconds = random.uniform(2, 1800)
            timestamp = user["last_ts"] + timedelta(seconds=delta_seconds)

            # Decide fraud (ground truth -- never published into transactions.raw)
            is_fraud = random.random() < 0.02

            if is_fraud:
                # Fraud severity is deliberately varied -- always stacking all
                # three strong signals (impossible travel + high-risk
                # merchant + inflated amount) makes every fraud case trivially
                # obvious to the model, which inflates live precision/recall
                # far above what the model actually achieves on real, more
                # varied fraud (see model_metadata.json's test-set numbers).
                # Each profile below trips a different, narrower subset of
                # signals so the live traffic mix is a fairer approximation.
                fraud_profile = random.choice(["obvious", "travel_only", "merchant_only", "amount_only"])

                if fraud_profile == "obvious":
                    # Blatant: impossible travel + high-risk merchant + very high amount
                    location = random.choice([l for l in LOCATIONS if l != user["home"]])
                    merchant = random.choice(["Casino", "Foreign_Site", "Luxury_Store"])
                    amount = round(random.uniform(1000, 8000), 2)
                elif fraud_profile == "travel_only":
                    # Only the location/travel signal is anomalous
                    location = random.choice([l for l in LOCATIONS if l != user["home"]])
                    merchant = random.choice(user["preferred_merchants"])
                    amount = round(random.uniform(20, 400), 2)
                elif fraud_profile == "merchant_only":
                    # Only merchant risk is anomalous; stays home, ordinary amount
                    location = user["home"]
                    merchant = random.choice(["Casino", "Foreign_Site", "Luxury_Store"])
                    amount = round(random.uniform(50, 600), 2)
                else:  # amount_only
                    # Only a moderately elevated amount; familiar merchant/location
                    location = user["home"]
                    merchant = random.choice(user["preferred_merchants"])
                    amount = round(random.uniform(600, 1500), 2)

                lat, lon = LOCATION_COORDS[location]
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

            transaction_id = str(uuid.uuid4())

            transaction = {
                "transaction_id": transaction_id,
                "user_id": user_id,
                "amount": amount,
                "merchant": merchant,
                "location": location,
                "lat": lat,
                "lon": lon,
                "timestamp": timestamp.isoformat()
            }

            producer.send(TOPIC_RAW, transaction)

            ready_at = time.time() + random.uniform(GT_DELAY_MIN_SECONDS, GT_DELAY_MAX_SECONDS)
            heapq.heappush(pending_confirmations, (ready_at, transaction_id, 1 if is_fraud else 0))

            # Update state
            user["lat"] = lat
            user["lon"] = lon
            user["last_ts"] = timestamp

            if random.random() < 0.01:
                tag = "FRAUD" if is_fraud else "NORMAL"
                print(f"[{INSTANCE_ID}] {tag} | User {user_id} | ${amount:.2f} | {merchant} | {location}", flush=True)

        # Drain any ground-truth confirmations that have become ready.
        drain_now = time.time()
        while pending_confirmations and pending_confirmations[0][0] <= drain_now:
            _, tx_id, label = heapq.heappop(pending_confirmations)
            producer.send(TOPIC_GROUND_TRUTH, {"transaction_id": tx_id, "actual_label": label})

        time.sleep(1)


if __name__ == "__main__":
    main()
