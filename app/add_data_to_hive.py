from pyhive import hive
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReadCSVFromHDFS") \
    .enableHiveSupport() \
    .getOrCreate()

csv_path = "hdfs://namenode:9000/data/processed_data.csv"
df = spark.read.csv(csv_path, header=True, inferSchema=True)

data = df.select("_c0", "sentiment", "tweet").collect()

conn = hive.Connection(
    host='hive-server',
    port=10000,
    database='bigdata_db'
)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS bigdata_db.processed_data (
    `_c0` STRING,
    sentiment INT,
    tweet STRING
)
STORED AS PARQUET
""")

insert_query = """
INSERT INTO TABLE bigdata_db.processed_data VALUES (%s, %s, %s)
"""

for row in data:
    cursor.execute(insert_query, (row['_c0'], row['sentiment'], row['tweet']))

conn.commit()
cursor.close()
conn.close()

print("Data successfully inserted into the Hive table.")
