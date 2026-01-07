from kafka import KafkaConsumer
import redis
import json

# -----------------------------
# Kafka Consumer Configuration
# -----------------------------
consumer = KafkaConsumer(
    "transactions",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="fraud-consumer-group"
)

# -----------------------------
# Redis Configuration
# -----------------------------
r = redis.Redis(
    host="localhost",
    port=6379,   # host-mapped Redis port
    decode_responses=True
)

print("Listening for transactions...")

# -----------------------------
# Consume Kafka Messages
# -----------------------------
for message in consumer:
    transaction = message.value
    print(f"Received: {transaction}")

    user_id = transaction["user_id"]
    amount = float(transaction["amount"])

    user_key = f"user:{user_id}"

    # -----------------------------
    # Initialize state for new user
    # -----------------------------
    if not r.exists(user_key):
        r.hset(user_key, mapping={
            "total_amount": 0.0,
            "tx_count": 0
        })

    # -----------------------------
    # Update user state
    # -----------------------------
    r.hincrbyfloat(user_key, "total_amount", amount)
    r.hincrby(user_key, "tx_count", 1)

    # -----------------------------
    # Fetch updated state
    # -----------------------------
    total_amount = float(r.hget(user_key, "total_amount"))
    tx_count = int(r.hget(user_key, "tx_count"))

    avg_amount = total_amount / tx_count if tx_count > 0 else 0
    ratio = amount / avg_amount if avg_amount > 0 else 0

    # -----------------------------
    # Enriched Output (Fraud Signal)
    # -----------------------------
    if ratio > 10:
        print(
            f"[ALARM] User {user_id}: "
            f"Current ${amount:.2f} | "
            f"Avg ${avg_amount:.2f} | "
            f"Ratio {ratio:.1f}x | POTENTIAL FRAUD"
        )
    else:
        print(
            f"[OK] User {user_id}: "
            f"Current ${amount:.2f} | "
            f"Avg ${avg_amount:.2f}"
        )

