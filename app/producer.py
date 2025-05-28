from kafka import KafkaProducer
import json

# Kafka configuration
KAFKA_BROKER = 'kafka:9092'
KAFKA_TOPIC = 'topic1'

def get_data():
    # Replace with your data source logic
    return {"message": "Hello, Kafka!"}

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    data = get_data()
    producer.send(KAFKA_TOPIC, value=data)
    producer.flush()
    print(f"Sent: {data}")

if __name__ == "__main__":
    main()
