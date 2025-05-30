import time
from kafka import KafkaProducer
import json
import random
import csv
import os

from pyspark import SparkContext
from pyspark.sql import SQLContext

HDFS_CSV_PATH = 'hdfs://namenode:9000/data/processed_data_test.csv'

def load_tweets_with_spark(hdfs_csv_path):
    sc = SparkContext(appName="TweetProducer")
    sqlContext = SQLContext(sc)
    df = sqlContext.read.format("csv").options(header='true', inferSchema='true').load(hdfs_csv_path)
    tweets = df.collect()
    sc.stop()
    # Convert Row objects to dicts
    return [row.asDict() for row in tweets]

def get_new_tweet(tweets):
    tweet = random.choice(tweets)
    return {
        "ID": tweet["_c0"],
        "text": tweet["tweet"],
        "sentiment": tweet["sentiment"],
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }

def main():
    tweets = load_tweets_with_spark(HDFS_CSV_PATH)
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    topic = 'topic1'

    print("Starting Kafka tweet producer...")
    try:
        while True:
            tweet = get_new_tweet(tweets)
            producer.send(topic, tweet)
            print("Sent tweet: ", tweet)
            time.sleep(5)
    except KeyboardInterrupt:
        print("Producer stopped.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
