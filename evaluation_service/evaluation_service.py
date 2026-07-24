import heapq
import itertools
import json
import os
import time

import clickhouse_connect
from kafka import KafkaConsumer
from kafka.errors import KafkaError

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
TOPIC_GROUND_TRUTH = os.environ.get("TOPIC_GROUND_TRUTH", "ground_truth")
GROUP_ID = "evaluation-service-group"

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))

INSTANCE_ID = os.environ.get("INSTANCE_ID", "evaluation-service")

BATCH_SIZE = 50
BATCH_INTERVAL = 2.0  # seconds

RETRY_DELAY_SECONDS = 1.0
MAX_ATTEMPTS = 3

PREDICTION_QUERY = """
SELECT transaction_id, timestamp, merchant, amount, fraud_probability, predicted_label
FROM fraud.predictions
WHERE transaction_id = {tx_id:String}
LIMIT 1
"""


def connect_kafka_consumer():
    for _ in range(15):
        try:
            return KafkaConsumer(
                TOPIC_GROUND_TRUTH,
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


def classify(predicted_label, actual_label):
    if predicted_label == 1 and actual_label == 1:
        return "TP"
    if predicted_label == 0 and actual_label == 0:
        return "TN"
    if predicted_label == 1 and actual_label == 0:
        return "FP"
    return "FN"


def main():
    consumer = connect_kafka_consumer()
    ch_client = connect_clickhouse()

    print(f"[{INSTANCE_ID}] Evaluation service started, waiting for ground truth...", flush=True)

    buffer = []
    columns = [
        "transaction_id", "timestamp", "merchant", "amount",
        "predicted_label", "actual_label", "fraud_probability",
        "correct_prediction", "classification",
    ]

    last_flush = time.time()
    missing_count = 0

    # Ground-truth confirmations whose prediction wasn't found yet get
    # deferred here instead of blocking the main consume loop with sleep()
    # -- retrying inline turned a handful of misses into unbounded consumer
    # lag (each blocked message stalled the whole partition for seconds).
    # Heap of (retry_at, seq, attempts_so_far, ground_truth_msg). `seq` is a
    # tie-breaker so heapq never falls through to comparing two dicts when
    # retry_at (and attempts) collide -- dicts aren't orderable.
    retry_queue = []
    retry_seq = itertools.count()

    def try_evaluate(gt):
        transaction_id = gt["transaction_id"]
        actual_label = int(gt["actual_label"])
        result = ch_client.query(PREDICTION_QUERY, parameters={"tx_id": transaction_id})
        if not result.row_count:
            return False

        _, timestamp, merchant, amount, fraud_probability, predicted_label = result.first_row
        predicted_label = int(predicted_label)
        correct_prediction = int(predicted_label == actual_label)
        classification = classify(predicted_label, actual_label)

        buffer.append([
            transaction_id, timestamp, merchant, amount,
            predicted_label, actual_label, fraud_probability,
            correct_prediction, classification,
        ])
        return True

    def drain_retries():
        nonlocal missing_count
        now = time.time()
        while retry_queue and retry_queue[0][0] <= now:
            _, _, attempts, gt = heapq.heappop(retry_queue)
            try:
                if try_evaluate(gt):
                    continue
            except Exception as e:
                print(f"[{INSTANCE_ID}] Error retrying evaluation for {gt.get('transaction_id')}: {e}", flush=True)
            if attempts < MAX_ATTEMPTS:
                heapq.heappush(retry_queue, (now + RETRY_DELAY_SECONDS, next(retry_seq), attempts + 1, gt))
            else:
                missing_count += 1
                print(f"[{INSTANCE_ID}] No prediction found for {gt.get('transaction_id')} after "
                      f"{MAX_ATTEMPTS} attempts (total missing so far: {missing_count})", flush=True)

    def flush():
        nonlocal buffer, last_flush
        if buffer:
            ch_client.insert("fraud.evaluation", buffer, column_names=columns)
            print(f"[{INSTANCE_ID}] Inserted {len(buffer)} evaluations into ClickHouse", flush=True)
            buffer = []
        last_flush = time.time()

    for msg in consumer:
        gt = msg.value
        try:
            if not try_evaluate(gt):
                heapq.heappush(retry_queue, (time.time() + RETRY_DELAY_SECONDS, next(retry_seq), 1, gt))
        except Exception as e:
            print(f"[{INSTANCE_ID}] Error evaluating transaction {gt.get('transaction_id')}: {e}", flush=True)

        drain_retries()

        if len(buffer) >= BATCH_SIZE or (time.time() - last_flush) >= BATCH_INTERVAL:
            flush()


if __name__ == "__main__":
    main()
