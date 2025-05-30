import json
from kafka import KafkaConsumer
from pyspark.sql import SparkSession, Row
import time

KAFKA_BROKER = 'kafka:9092'
TOPIC = 'topic1'
HIVE_TABLE = 'tweets'

def main():
    # Start SparkSession with Hive support
    spark = SparkSession.builder \
        .appName("TweetConsumer") \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
        .enableHiveSupport() \
        .getOrCreate()

    # Drop the table if it exists to avoid schema mismatch
    spark.sql("DROP TABLE IF EXISTS {}".format(HIVE_TABLE))

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='tweet-consumer-group'
    )

    print("Starting Kafka tweet consumer...")
    for message in consumer:
        tweet = message.value
        print("Received tweet:", tweet)

        # Convert tweet dict to Spark DataFrame
        df = spark.createDataFrame([Row(**tweet)])

        # Create Hive table if not exists
        df.write.mode("append").format("hive").saveAsTable(HIVE_TABLE)

        # Optional: sleep to avoid flooding Hive
        time.sleep(1)

if __name__ == "__main__":
    main()
