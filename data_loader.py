#!/usr/bin/env python3
"""
Load NASA FIRMS CSV (17 columns) into Cassandra.
"""
 
import csv
from pathlib import Path
from uuid import uuid4
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement
 
# ------------------------------------------------
# CONFIG – edit these paths/hosts if needed
# ------------------------------------------------
TXT_FILE   = Path("satellite_unstructured.txt")   # your unstructured file
CASS_HOSTS = ["localhost"]                    # ["cassandra"] if using Docker network
KEYSPACE   = "wildfire"
TABLE      = "fires_full"
BATCH_SIZE = 500
# ------------------------------------------------
 
def main():
    # ---------- connect ----------
    cluster = Cluster(CASS_HOSTS)
    session = cluster.connect()
 
    # ---------- keyspace + table ----------
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
 
    # ---------- read & insert ----------
    total = 0
    batch = BatchStatement()
 
    with TXT_FILE.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
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
                float_or_none(row["type"])
            ))
            total += 1
 
            if len(batch) >= BATCH_SIZE:
                session.execute(batch)
                batch.clear()
                print(f"Inserted {total} rows")
 
        # flush final partial batch
        if batch:
            session.execute(batch)
            print(f"Inserted {total} rows (final)")
 
    print("DONE")
    session.shutdown()
    cluster.shutdown()
 
 
def float_or_none(value: str):
    """Convert to float, keep None for empty strings."""
    if value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None
 
 
if __name__ == "__main__":
    main()
