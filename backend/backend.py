# backend_test.py
from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json
import random
import time

app = Flask(__name__)

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

@app.route('/products', methods=['GET'])
def get_products():
    products = [
        {"id": "1", "name": "Laptop", "price": 999.99},
        {"id": "2", "name": "Smartphone", "price": 499.49},
        {"id": "3", "name": "Tablet", "price": 299.99}
    ]

    # Envoie les produits dans Kafka
    for product in products:
        producer.send("topic1", product)

    return jsonify({"message": "Produits envoyés à Kafka (topic1).", "data": products})

@app.route('/purchase', methods=['POST'])
def post_purchase():
    # Données d'achat générées aléatoirement
    user_id = str(random.randint(1, 100))
    product_id = random.choice(["1", "2", "3"])
    quantity = random.randint(1, 5)
    prices = {"1": 999.99, "2": 499.49, "3": 299.99}
    total = round(prices[product_id] * quantity, 2)

    data = {
        "user_id": user_id,
        "product_id": product_id,
        "quantity": quantity,
        "total": total
    }

    producer.send("topic2", data)
    return jsonify({"message": "Achat aléatoire publié dans Kafka (topic2).", "data": data})

@app.route('/random-batch', methods=['POST'])
def random_batch():
    batch = []
    for _ in range(10):
        user_id = str(random.randint(1, 100))
        product_id = random.choice(["1", "2", "3"])
        quantity = random.randint(1, 5)
        prices = {"1": 999.99, "2": 499.49, "3": 299.99}
        total = round(prices[product_id] * quantity, 2)
        data = {
            "user_id": user_id,
            "product_id": product_id,
            "quantity": quantity,
            "total": total
        }
        producer.send("topic2", data)
        batch.append(data)
        time.sleep(0.2)  # petite pause pour simuler un flux

    return jsonify({"message": "Batch d'achats aléatoires envoyé dans Kafka.", "data": batch})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
