import json
import mysql.connector
from kafka import KafkaConsumer

KAFKA_BROKER = "kafka-container2:9094"
TOPIC = "weather-data"
GROUP_ID = "weather-mysql-consumer"

MYSQL_CONFIG = {
    'host': 'mysql-container',
    'user': 'root',
    'password': 'password',
    'database': 'testdb',
    'autocommit': True
}

def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        #group_id=GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        #enable_auto_commit=True
    )

    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()

    create_table = """
    CREATE TABLE IF NOT EXISTS wildfire_data (
        latitude DOUBLE,
        longitude DOUBLE,
        Temp_pre_30 DOUBLE,
        Temp_pre_15 DOUBLE,
        Temp_pre_7 DOUBLE,
        Temp_cont DOUBLE,
        Wind_pre_30 DOUBLE,
        Wind_pre_15 DOUBLE,
        Wind_pre_7 DOUBLE,
        Wind_cont DOUBLE,
        Hum_pre_30 DOUBLE,
        Hum_pre_15 DOUBLE,
        Hum_pre_7 DOUBLE,
        Hum_cont DOUBLE,
        Prec_pre_30 DOUBLE,
        Prec_pre_15 DOUBLE,
        Prec_pre_7 DOUBLE,
        Prec_cont DOUBLE,
        fire_size DOUBLE,
        state VARCHAR(50),
        discovery_month VARCHAR(20),
        putout_time VARCHAR(50),
        disc_pre_year INT,
        disc_pre_month VARCHAR(20),
        stat_cause_descr VARCHAR(100),
        row_id INT,
        fire_name VARCHAR(100),
        fire_size_class CHAR(1),
        disc_clean_date DATE,
        wstation_usaf VARCHAR(20)
    );
    """
    cursor.execute(create_table)

    insert_query = """
    INSERT INTO wildfire_data VALUES (
        %(latitude)s, %(longitude)s, %(Temp_pre_30)s, %(Temp_pre_15)s, %(Temp_pre_7)s, %(Temp_cont)s,
        %(Wind_pre_30)s, %(Wind_pre_15)s, %(Wind_pre_7)s, %(Wind_cont)s,
        %(Hum_pre_30)s, %(Hum_pre_15)s, %(Hum_pre_7)s, %(Hum_cont)s,
        %(Prec_pre_30)s, %(Prec_pre_15)s, %(Prec_pre_7)s, %(Prec_cont)s,
        %(fire_size)s, %(state)s, %(discovery_month)s, %(putout_time)s,
        %(disc_pre_year)s, %(disc_pre_month)s, %(stat_cause_descr)s, %(row_id)s,
        %(fire_name)s, %(fire_size_class)s, %(disc_clean_date)s, %(wstation_usaf)s
    )
    """

    for msg in consumer:
        row = msg.value
        try:
            cursor.execute(insert_query, row)
            print(f"Inserted: {row}")
        except Exception as e:
            print("Insert failed:", e)

if __name__ == "__main__":
    main()

