from hdfs import InsecureClient
import pandas as pd

# Connect to HDFS via WebHDFS
client = InsecureClient('http://hdfs-container:50070', user='root')  # use 'hdfs-container' because of Docker network

local_path = './merged_output.csv'

# Upload local file to HDFS
hdfs_path = '/cleaned/merged_output.csv'
client.upload(hdfs_path, local_path, overwrite=True)

print(f"File uploaded to HDFS at {hdfs_path}")

