from kafka import KafkaProducer
import csv
import json
import time
from pathlib import Path

TXT_FILE = Path("fire_data.txt")
KAFKA_BROKER = "kafka-container1:9092"  # match your container name
TOPIC = "satellite-fires"

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    with TXT_FILE.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            producer.send(TOPIC, row)
            print(f"Sent to Kafka: {row}")
    
    producer.flush()
    producer.close()
    print("All data sent to Kafka.")

if __name__ == "__main__":
    main()

