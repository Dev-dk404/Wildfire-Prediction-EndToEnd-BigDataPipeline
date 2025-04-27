import pandas as pd
import mysql.connector
from cassandra.cluster import Cluster
from cassandra.util import Date
from joblib import Memory

# Setup joblib cache directory
memory = Memory("cache_dir", verbose=1)

# Cassandra Connection
def connect_to_cassandra():
    cluster = Cluster(["cassandra-container"])
    session = cluster.connect("wildfire")
    return session

# MySQL Connection
def connect_to_mysql():
    conn = mysql.connector.connect(
        host="mysql-container",
        user="root",
        password="password",
        database="testdb"
    )
    return conn

# Cached data fetch from Cassandra
def fetch_cassandra_data(session):
    rows = session.execute("SELECT * FROM hotspots")
    df = pd.DataFrame(list(rows))
    if 'acq_date' in df.columns:
        df['acq_date'] = df['acq_date'].apply(lambda d: pd.to_datetime(str(d)))
    return df

# Cached data fetch from MySQL
def fetch_mysql_data(conn):
    query = "SELECT * FROM wildfire_data"
    df = pd.read_sql(query, conn)
    if 'disc_clean_date' in df.columns:
        df['disc_clean_date'] = pd.to_datetime(df['disc_clean_date'])
    return df

# ETL Function
@memory.cache
def etl(mysql_df, cassandra_df):
    mysql_df['month'] = mysql_df['disc_clean_date'].dt.month
    mysql_df['year'] = mysql_df['disc_clean_date'].dt.year
    cassandra_df['month'] = cassandra_df['acq_date'].dt.month
    cassandra_df['year'] = cassandra_df['acq_date'].dt.year

    merged_df = pd.merge(
        mysql_df,
        cassandra_df,
        on=["month", "year"],
        how="inner"
    )
    return merged_df

def main():
    cassandra_session = connect_to_cassandra()
    mysql_conn = connect_to_mysql()

    cassandra_df = fetch_cassandra_data(cassandra_session).head(50000)
    mysql_df = fetch_mysql_data(mysql_conn).head(50000)

    merged_data = etl(mysql_df, cassandra_df)
    print(merged_data.head())

if __name__ == "__main__":
    main()

