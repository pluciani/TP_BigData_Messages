from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json

app = Flask(__name__)

# Configure your Kafka broker(s) here
KAFKA_BROKER = 'kafka:9092'
KAFKA_TOPIC = 'topic1'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

@app.route('/send', methods=['POST'])
def send_message():
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No JSON payload provided'}), 400

    try:
        producer.send(KAFKA_TOPIC, data)
        producer.flush()
        return jsonify({'status': 'Message sent'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)