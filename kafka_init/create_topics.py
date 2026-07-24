import os
import time

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")

TOPICS = [
    NewTopic("transactions.raw", num_partitions=2, replication_factor=2),
    NewTopic("transactions.features", num_partitions=2, replication_factor=2),
    NewTopic("ground_truth", num_partitions=2, replication_factor=2),
]


def connect():
    for _ in range(20):
        try:
            return KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
        except KafkaError:
            print("Kafka not ready, retrying...", flush=True)
            time.sleep(3)
    raise RuntimeError("Kafka not available")


def main():
    admin = connect()
    try:
        for topic in TOPICS:
            try:
                admin.create_topics([topic])
                print(f"Created topic '{topic.name}' ({topic.num_partitions} partitions).", flush=True)
            except TopicAlreadyExistsError:
                print(f"Topic '{topic.name}' already exists, skipping.", flush=True)
    finally:
        admin.close()
    print("Topic initialization complete.", flush=True)


if __name__ == "__main__":
    main()
