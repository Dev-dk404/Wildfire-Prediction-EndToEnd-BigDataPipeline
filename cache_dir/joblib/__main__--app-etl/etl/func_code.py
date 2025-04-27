# first line: 43
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
