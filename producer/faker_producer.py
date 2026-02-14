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

MERCHANTS = ["Amazon", "Walmart", "Shell", "Starbucks", "BestBuy", 
             "Luxury_Store", "Casino", "Foreign_Site", "Electronics_Hub", "Grocery"]

# Track user state (simulating user behavior)
user_profiles = {}

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

print("Producing realistic transactions with multi-factor patterns...")

TX_PER_SECOND = 100

while True:
    for _ in range(TX_PER_SECOND):
        user_id = fake.random_int(min=100, max=150)
        
        # Initialize user profile if new
        if user_id not in user_profiles:
            user_profiles[user_id] = {
                'home_location': random.choice(LOCATIONS),
                'preferred_merchants': random.sample(MERCHANTS, k=random.randint(3, 5)),
                'last_location': None
            }
        
        user = user_profiles[user_id]
        
        # -------- Fraud pattern injection (2% of transactions) --------
        is_fraud_pattern = random.random() < 0.02
        
        if is_fraud_pattern:
            # Fraudulent behavior
            amount = round(random.uniform(1000, 8000), 2)
            merchant = random.choice(["Casino", "Foreign_Site", "Luxury_Store"])
            location = random.choice([loc for loc in LOCATIONS if loc != user['home_location']])
        else:
            # Normal behavior
            
            # Merchant selection (70% preferred, 30% random)
            if random.random() < 0.7:
                merchant = random.choice(user['preferred_merchants'])
            else:
                merchant = random.choice(MERCHANTS)
            
            # Amount based on merchant
            if merchant == "Luxury_Store":
                amount = round(random.uniform(500, 5000), 2)
            elif merchant in ["Starbucks", "Shell"]:
                amount = round(random.uniform(5, 50), 2)
            else:
                amount = round(random.uniform(10, 800), 2)
            
            # Location (95% home, 5% travel)
            if random.random() < 0.95:
                location = user['home_location']
            else:
                location = random.choice(LOCATIONS)
        
        # Update last location
        user['last_location'] = location
        
        lat, lon = LOCATION_COORDS[location]
        
        transaction = {
            "user_id": user_id,
            "amount": amount,
            "merchant": merchant,
            "location": location,
            "lat": lat,
            "lon": lon,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        producer.send(TOPIC_NAME, transaction)
        
        # Less verbose logging
        if random.random() < 0.01:  # Log 1% of transactions
            fraud_flag = "FRAUD PATTERN" if is_fraud_pattern else "NORMAL"
            print(f"{fraud_flag} | User {user_id} | ${amount:.2f} | {merchant} | {location}")
    
    time.sleep(1)
