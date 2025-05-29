from kafka import KafkaProducer
import time
import random
import csv
import json

KAFKA_TOPIC = "fraud-cc-stream"
KAFKA_BROKER = "localhost:9092"
DATASET_PATH = "fraudTrain.csv"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

with open(DATASET_PATH, "r") as file:
    reader = csv.DictReader(file)

    for row in reader:
        clean_row = {
            k: v for k, v in row.items()
            if k.strip() != "" and k != "" and not k.isdigit()
        }

        producer.send(KAFKA_TOPIC, clean_row)
        print(f"Sent: {clean_row}")
        time.sleep(random.uniform(0.01, 0.1))

producer.flush()
