"""Эмулятор источника данных: CSV (mock_data/*.csv) -> JSON -> Kafka-топик."""
import csv
import glob
import json
import os
import re
import sys
import time

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "sales_topic")
DATA_DIR = os.getenv("DATA_DIR", "mock_data")


def natural_key(path):
    # mock_data_2.csv раньше mock_data_10.csv
    return [int(t) if t.isdigit() else t for t in re.split(r"(\d+)", path)]


def connect(retries=30, delay=2):
    for attempt in range(1, retries + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                acks="all",
                linger_ms=10,
            )
        except NoBrokersAvailable:
            print(f"[!] Kafka {KAFKA_BOOTSTRAP} недоступна (попытка {attempt}/{retries})", flush=True)
            time.sleep(delay)
    sys.exit("[x] Не удалось подключиться к Kafka")


def main():
    files = sorted(glob.glob(os.path.join(DATA_DIR, "*.csv")), key=natural_key)
    if not files:
        sys.exit(f"[!] No CSV files found in {DATA_DIR}")

    producer = connect()
    total = 0
    for path in files:
        print(f"[+] Sending {path}", flush=True)
        # newline="" обязателен для csv: в полях (product_description) есть переводы строк
        with open(path, encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                if not row.get("id"):  # пропускаем пустые строки
                    continue
                producer.send(TOPIC, row)
                total += 1
                if total % 500 == 0:  # лёгкая эмуляция потока
                    producer.flush()
                    time.sleep(0.05)
        producer.flush()

    producer.flush()
    producer.close()
    print(f"[✓] Sent {total} messages to topic '{TOPIC}'", flush=True)


if __name__ == "__main__":
    main()
