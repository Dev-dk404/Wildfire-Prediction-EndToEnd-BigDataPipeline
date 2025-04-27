import pandas as pd
from kafka import KafkaProducer
import json

KAFKA_TOPIC = "weather-data"
KAFKA_BROKER = "kafka-container2:9094"

def main():
    df = pd.read_csv("structured_final.csv")
    df.drop(columns=['cont_clean_date'], inplace=True)


    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype(str)
            
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    for _, row in df.iterrows():
        data = row.to_dict()
        producer.send(KAFKA_TOPIC, value=data)
        print(f"Sent: {data}")
    producer.flush()
    print("All messages sent!")

if __name__ == "__main__":
    main()

