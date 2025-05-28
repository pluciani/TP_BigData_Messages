import time
from kafka import KafkaProducer
import json
import random
import csv
import os

CSV_PATH = os.path.join(
    os.path.dirname(__file__),
    'test.csv'
)

def load_tweets_from_csv(csv_path):
    tweets = []
    with open(csv_path) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            tweets.append(row)
    return tweets

def get_new_tweet(tweets):
    # Randomly select a tweet (row) from the CSV
    tweet = random.choice(tweets)
    # You can customize the payload as needed
    return {
        "textID": tweet["textID"],
        "text": tweet["text"],
        "sentiment": tweet["sentiment"],
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }

def main():
    tweets = load_tweets_from_csv(CSV_PATH)
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
