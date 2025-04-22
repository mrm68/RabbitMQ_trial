import pika
import time

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='fanout_logs', exchange_type='fanout')

while True:
    message = f"Broadcast message at {time.time()}"
    channel.basic_publish(
        exchange='fanout_logs',
        routing_key='',  # Ignored in fanout
        body=message)
    print(f" [x] Sent {message}")
    time.sleep(1)
