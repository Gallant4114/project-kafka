from kafka import KafkaConsumer
import json
import os

KAFKA_TOPIC = "fraud-cc-stream"
KAFKA_BROKER = "localhost:9092"
BATCH_SIZE = 5500
BATCH_DIR = "batches"
MAX_BATCHES = 3
os.makedirs(BATCH_DIR, exist_ok=True)

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="fraud-group"
)

batch = []
batch_count = 0

for message in consumer:
    batch.append(message.value)

    if len(batch) >= BATCH_SIZE:
        output_path = os.path.join(BATCH_DIR, f"batch_{batch_count}.json")
        with open(output_path, "w") as f:
            json.dump(batch, f, indent=2)
        print(f"Saved {len(batch)} rows to {output_path}")

        batch = []
        batch_count += 1

        if batch_count >= MAX_BATCHES:
            print("✔ Reached maximum number of batches. Exiting consumer.")
            break

consumer.close()
