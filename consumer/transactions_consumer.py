import json
import redis
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import time

# ---------------- Configuration ----------------
KAFKA_BROKER = "kafka:9092"       # container-internal hostname
TOPIC = "transactions"
REDIS_HOST = "redis"               # container-internal hostname
REDIS_PORT = 6379
FRAUD_THRESHOLD = 5.0              # ratio to trigger alarm
# ------------------------------------------------

# Connect to Redis
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# Connect to Kafka
while True:
    try:
        consumer = KafkaConsumer(
	    TOPIC,
	    bootstrap_servers=["kafka:9092"],
	    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
	    auto_offset_reset="earliest",
	    enable_auto_commit=True,
	    group_id="fraud_detection_group"
	)
        break
    except NoBrokersAvailable:
        print("Waiting for Kafka...")
        time.sleep(5)

print("Listening for transactions...")

for message in consumer:
    transaction = message.value

    user_id = transaction["user_id"]
    user_key = f"user:{user_id}"

    pipe = r.pipeline()

    if not r.exists(user_key):
        pipe.hset(user_key, mapping={"total_amount": 0.0, "tx_count": 0})

    pipe.hincrbyfloat(user_key, "total_amount", transaction["amount"])
    pipe.hincrby(user_key, "tx_count", 1)
    pipe.execute()

    data = r.hgetall(user_key)
    total_amount = float(data["total_amount"])
    tx_count = int(data["tx_count"])
    avg_amount = total_amount / tx_count

    ratio = transaction["amount"] / avg_amount if avg_amount > 0 else 0.0

    if ratio > FRAUD_THRESHOLD:
        print(f"[ALARM] User {user_id}: Current ${transaction['amount']:.2f} | "
              f"Avg ${avg_amount:.2f} | Ratio {ratio:.2f}x | POTENTIAL FRAUD")
    else:
        print(f"[OK] User {user_id}: Current ${transaction['amount']:.2f} | "
              f"Avg ${avg_amount:.2f} | Ratio {ratio:.2f}x")


