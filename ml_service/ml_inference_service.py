import json
import os
import time
from datetime import datetime

import clickhouse_connect
import xgboost as xgb
from kafka import KafkaConsumer
from kafka.errors import KafkaError

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
TOPIC_FEATURES = os.environ.get("TOPIC_FEATURES", "transactions.features")
GROUP_ID = "ml-service-group"

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))

INSTANCE_ID = os.environ.get("INSTANCE_ID", "ml-service")

BATCH_SIZE = 100
BATCH_INTERVAL = 2.0  # seconds

# Load model + metadata once at startup.
model = xgb.Booster()
model.load_model("fraud_model.json")

with open("model_metadata.json", "r") as f:
    metadata = json.load(f)
    FEATURE_COLUMNS = metadata["feature_columns"]
    OPTIMAL_THRESHOLD = metadata.get("optimal_threshold", 0.5)

print(f"[{INSTANCE_ID}] Model loaded with {len(FEATURE_COLUMNS)} features, threshold={OPTIMAL_THRESHOLD}", flush=True)


def connect_kafka_consumer():
    for _ in range(15):
        try:
            return KafkaConsumer(
                TOPIC_FEATURES,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id=GROUP_ID,
            )
        except KafkaError:
            print(f"[{INSTANCE_ID}] Kafka not ready, retrying...", flush=True)
            time.sleep(2)
    raise RuntimeError("Kafka not available")


def connect_clickhouse():
    for _ in range(15):
        try:
            client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
            client.ping()
            return client
        except Exception:
            print(f"[{INSTANCE_ID}] ClickHouse not ready, retrying...", flush=True)
            time.sleep(2)
    raise RuntimeError("ClickHouse not available")


def predict(features):
    row = [[features[c] for c in FEATURE_COLUMNS]]
    dmatrix = xgb.DMatrix(row, feature_names=FEATURE_COLUMNS)
    probability = float(model.predict(dmatrix)[0])
    predicted_label = int(probability >= OPTIMAL_THRESHOLD)
    return probability, predicted_label


def main():
    consumer = connect_kafka_consumer()
    ch_client = connect_clickhouse()

    print(f"[{INSTANCE_ID}] ML inference service started, waiting for enriched transactions...", flush=True)

    buffer = []
    columns = ["transaction_id", "timestamp", "merchant", "amount", "fraud_probability", "predicted_label"]

    last_flush = time.time()

    def flush():
        nonlocal buffer, last_flush
        if buffer:
            ch_client.insert("fraud.predictions", buffer, column_names=columns)
            print(f"[{INSTANCE_ID}] Inserted {len(buffer)} predictions into ClickHouse", flush=True)
            buffer = []
        last_flush = time.time()

    for msg in consumer:
        tx = msg.value
        try:
            probability, predicted_label = predict(tx)
            row = [
                tx["transaction_id"],
                datetime.fromisoformat(tx["timestamp"]),
                tx["merchant"],
                tx["amount"],
                probability,
                predicted_label,
            ]
            buffer.append(row)
        except Exception as e:
            print(f"[{INSTANCE_ID}] Error processing transaction: {e}", flush=True)
            continue

        if len(buffer) >= BATCH_SIZE or (time.time() - last_flush) >= BATCH_INTERVAL:
            flush()


if __name__ == "__main__":
    main()
