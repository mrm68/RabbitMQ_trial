import pika
import random
import time

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='topic_logs', exchange_type='topic')

routing_keys = [
    'kern.info',
    'cron.warning',
    'auth.error',
    'app.debug'
]

while True:
    key = random.choice(routing_keys)
    message = f"{key}: Message at {time.time()}"
    channel.basic_publish(
        exchange='topic_logs',
        routing_key=key,
        body=message)
    print(f" [x] Sent {key}:{message}")
    time.sleep(random.uniform(0.5, 2.0))
