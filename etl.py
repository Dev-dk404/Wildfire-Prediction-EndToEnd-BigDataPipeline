import pandas as pd
from cassandra.cluster import Cluster
import mysql.connector

def create_connections():
    # Cassandra connection
    cassandra_cluster = Cluster(['cassandra-container']) 
    cassandra_session = cassandra_cluster.connect('wildfire')  

    # MySQL connection
    mysql_conn = mysql.connector.connect(
        host='mysql-container',  # MySQL container name or host
        user='root',  # Your MySQL username
        password='password',  # Your MySQL password
        database='testdb'  # Your MySQL database
    )

    return cassandra_session, mysql_conn


def fetch_cassandra_data(session):
    print("Fetching data from cassandra....")
    query = "SELECT * FROM hotspots limit 10000"  
    rows = session.execute(query)
    cassandra_df = pd.DataFrame(list(rows))
    if 'acq_date' in cassandra_df.columns:
        cassandra_df['acq_date'] = cassandra_df['acq_date'].apply(lambda d: pd.to_datetime(str(d)))
    return cassandra_df


def fetch_mysql_data(conn):
    print("Fetching data from mysql....")
    query = "SELECT * FROM wildfire_data limit 10000"  
    mysql_df = pd.read_sql(query, conn)
    if 'disc_clean_date' in mysql_df.columns:
        mysql_df['disc_clean_date'] = pd.to_datetime(mysql_df['disc_clean_date'])
    return mysql_df


def perform_etl(cassandra_df, mysql_df):
    print("Performing ETL....\n")
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
    print("Data merged")
    return merged_df


def main():
    cassandra_session, mysql_conn = create_connections()

    # Fetch data
    cassandra_df = fetch_cassandra_data(cassandra_session)
    mysql_df = fetch_mysql_data(mysql_conn)

    # Perform ETL
    merged_data = perform_etl(cassandra_df, mysql_df)
    print(len(merged_data))

    # save merged data to csv
    merged_data.to_csv('merged_output.csv', index=False)  

    # Close connections
    cassandra_session.shutdown()
    mysql_conn.close()

if __name__ == "__main__":
    main()

