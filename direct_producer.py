import pika
import random
import time

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare direct exchange
channel.exchange_declare(exchange='direct_logs', exchange_type='direct')

severities = ['info', 'warning', 'error']

while True:
    severity = random.choice(severities)
    message = f'{severity}: Log message at {time.time()}'
    channel.basic_publish(
        exchange='direct_logs',
        routing_key=severity,
        body=message)
    print(f" [x] Sent {severity}:{message}")
    time.sleep(random.uniform(0.1, 1.0))
