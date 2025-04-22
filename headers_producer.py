import pika
import random
import time

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='headers_logs', exchange_type='headers')

priority_levels = ['high', 'medium', 'low']
app_ids = ['web', 'api', 'batch']

while True:
    priority = random.choice(priority_levels)
    app = random.choice(app_ids)

    headers = {
        'priority': priority,
        'app': app,
        'x-match': 'any'  # Match any header
    }

    message = f"{priority} priority message from {app} at {time.time()}"

    channel.basic_publish(
        exchange='headers_logs',
        routing_key='',  # Ignored in headers exchange
        body=message,
        properties=pika.BasicProperties(headers=headers))

    print(f" [x] Sent {message}")
    time.sleep(1)
