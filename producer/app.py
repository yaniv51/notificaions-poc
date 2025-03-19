from flask import Flask, request, jsonify
from faker import Faker
from kafka import KafkaProducer
import json
import random
import time

app = Flask(__name__)
fake = Faker()

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def generate_notification(notification_type):
    priority = 'high' if random.random() < 0.5 else 'low'
    partition = random.randint(4, 5) if priority == 'high' else random.randint(0, 3)
    
    notification = {
        'id': fake.uuid4(),
        'type': notification_type,
        'priority': priority,
        'content': fake.text(),
        'timestamp': time.time(),
        'recipient': fake.email() if notification_type == 'EMAIL' else fake.user_agent()
    }
    
    return notification, partition

@app.route('/generate-notifications')
def generate_notifications():
    notification_type = request.args.get('type', '').upper()
    amount = int(request.args.get('amount', 1))
    
    if notification_type not in ['EMAIL', 'PUSH']:
        return jsonify({'error': 'Invalid notification type'}), 400
    
    notifications = []
    for _ in range(amount):
        notification, partition = generate_notification(notification_type)
        topic = 'email' if notification_type == 'EMAIL' else 'push'
        
        # Send to Kafka with partition assignment
        producer.send(topic, value=notification, partition=partition)
        notifications.append(notification)
    
    producer.flush()
    return jsonify({
        'message': f'Generated {amount} {notification_type} notifications',
        'notifications': notifications
    })

if __name__ == '__main__':
    app.run(debug=True, port=5000) 