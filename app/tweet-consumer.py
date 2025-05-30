import json
from kafka import KafkaConsumer
from pyhive import hive
import time
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.ml import PipelineModel

KAFKA_BROKER = 'kafka:9092'
TOPIC = 'topic1'
HIVE_TABLE = 'bigdata_db.predictions'

def main():
    sc = SparkContext(appName="TweetConsumer")
    sqlContext = SQLContext(sc)
    conn = hive.Connection(
        host='hive-server',
        port=10000,
        database='bigdata_db'
    )
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS {0} (
        id STRING,
        sentiment INT,
        tweet STRING,
        toxicity_prediction INT
    )
    STORED AS PARQUET
    """.format(HIVE_TABLE))

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='tweet-consumer-group'
    )

    print("Starting Kafka tweet consumer...")
    
    insert_query = """
    INSERT INTO TABLE {0} VALUES (%s, %s, %s, %s)
    """.format(HIVE_TABLE)

    for message in consumer:
        loaded_model = PipelineModel.load("hdfs://namenode:9000/data/toxicity_model")

        sc = SparkContext.getOrCreate()
        sqlContext = SQLContext(sc)
        tweet = message.value
        tweet_df = sqlContext.createDataFrame([{"tweet": tweet["text"]}])
        prediction = loaded_model.transform(tweet_df).collect()[0]
        tweet["toxicity_prediction"] = prediction["prediction"]
        print("Received tweet:", tweet)
        cursor.execute(insert_query, (
            tweet.get('id', ''),
            tweet.get('sentiment', 0),
            tweet.get('tweet', ''),
            tweet.get('toxicity_predictions', 0)
        ))
        conn.commit()
        # time.sleep(1)

    cursor.close()
    conn.close()
    sc.stop()

if __name__ == "__main__":
    main()
