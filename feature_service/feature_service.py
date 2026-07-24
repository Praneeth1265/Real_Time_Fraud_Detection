import json
import os
import time

import redis
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

from common.features import compute_features

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
TOPIC_RAW = os.environ.get("TOPIC_RAW", "transactions.raw")
TOPIC_FEATURES = os.environ.get("TOPIC_FEATURES", "transactions.features")
GROUP_ID = "feature-service-group"

REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))

INSTANCE_ID = os.environ.get("INSTANCE_ID", "feature-service")


def connect_kafka_consumer():
    for _ in range(15):
        try:
            return KafkaConsumer(
                TOPIC_RAW,
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


def connect_kafka_producer():
    for _ in range(15):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
        except KafkaError:
            print(f"[{INSTANCE_ID}] Kafka not ready, retrying...", flush=True)
            time.sleep(2)
    raise RuntimeError("Kafka not available")


def connect_redis():
    for _ in range(15):
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            r.ping()
            return r
        except redis.exceptions.ConnectionError:
            print(f"[{INSTANCE_ID}] Redis not ready, retrying...", flush=True)
            time.sleep(2)
    raise RuntimeError("Redis not available")


def main():
    r = connect_redis()
    consumer = connect_kafka_consumer()
    producer = connect_kafka_producer()

    print(f"[{INSTANCE_ID}] Feature service started, waiting for transactions...", flush=True)

    for msg in consumer:
        tx = msg.value
        try:
            features = compute_features(r, tx)
            enriched = {**tx, **{k: v for k, v in features.items() if k != "amount"}}
            producer.send(TOPIC_FEATURES, enriched)
        except Exception as e:
            print(f"[{INSTANCE_ID}] Error processing transaction: {e}", flush=True)
            continue


if __name__ == "__main__":
    main()
