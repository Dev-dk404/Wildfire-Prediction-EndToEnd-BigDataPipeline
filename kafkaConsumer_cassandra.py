from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from uuid import uuid4
import json

KAFKA_BROKER = "kafka-container1:9092"
TOPIC = "satellite-fires"
CASS_HOSTS = ["cassandra-container"]
KEYSPACE = "wildfire"
TABLE = "hotspots"
BATCH_SIZE = 50

def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset='earliest'
        #enable_auto_commit=True,
        #group_id="cassandra-stream-group"
    )

    cluster = Cluster(CASS_HOSTS)
    session = cluster.connect()

    session.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
    WITH replication = {{'class':'SimpleStrategy', 'replication_factor':1}};
    """)
    session.set_keyspace(KEYSPACE)

    session.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        fire_id       uuid PRIMARY KEY,
        latitude      double,
        longitude     double,
        brightness    double,
        scan          double,
        track         double,
        acq_date      date,
        acq_time      text,
        satellite     text,
        instrument    text,
        confidence    text,
        version       text,
        bright_t31    double,
        frp           double,
        daynight      text,
        bright_ti4    double,
        bright_ti5    double,
        type          double
    );
    """)

    insert_cql = f"""
    INSERT INTO {TABLE} (
      fire_id, latitude, longitude, brightness, scan, track,
      acq_date, acq_time, satellite, instrument, confidence, version,
      bright_t31, frp, daynight, bright_ti4, bright_ti5, type)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """
    prepared = session.prepare(insert_cql)
    batch = BatchStatement()
    count = 0

    for msg in consumer:
        print("Received message:", msg.value)
        row = msg.value
        batch.add(prepared, (
            uuid4(),
            float_or_none(row["latitude"]),
            float_or_none(row["longitude"]),
            float_or_none(row["brightness"]),
            float_or_none(row["scan"]),
            float_or_none(row["track"]),
            row["acq_date"],
            row["acq_time"],
            row["satellite"],
            row["instrument"],
            row["confidence"],
            row["version"],
            float_or_none(row["bright_t31"]),
            float_or_none(row["frp"]),
            row["daynight"],
            float_or_none(row["bright_ti4"]),
            float_or_none(row["bright_ti5"]),
            float_or_none(row["type"]),
        ))
        count += 1

        if count % BATCH_SIZE == 0:
            session.execute(batch)
            batch.clear()
            print(f"Inserted {count} rows")

    if batch:
        session.execute(batch)
        print(f"Inserted final {count} rows")

    session.shutdown()
    cluster.shutdown()

def float_or_none(value: str):
    if value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None

if __name__ == "__main__":
    main()

