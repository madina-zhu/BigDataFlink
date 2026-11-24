import csv
import glob
import json
import os
import time
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "sales_topic")
DATA_DIR = os.getenv("DATA_DIR", "mock_data")

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        linger_ms=10,
    )

    files = sorted(glob.glob(os.path.join(DATA_DIR, "*.csv")))
    if not files:
        print(f"[!] No CSV files found in {DATA_DIR}")
        return

    total = 0
    for path in files:
        print(f"[+] Sending {path}")
        with open(path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # пропускаем пустые строки
                if not row.get("id"):
                    continue
                producer.send(TOPIC, row)
                total += 1
                # лёгкая эмуляция потока
                if total % 500 == 0:
                    producer.flush()
                    time.sleep(0.05)
        producer.flush()

    producer.flush()
    producer.close()
    print(f"[✓] Sent {total} messages to topic '{TOPIC}'")

if __name__ == "__main__":
    main()