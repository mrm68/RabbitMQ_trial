import pika
import random
import time
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='hello')
while True:
    r = random.randint(0, 10)
    delay = random.randint(0, 5)*0.1
    for i in range(r):
        message = f'sending {i} from {r}'
        channel.basic_publish(exchange='',
                              routing_key='hello',
                              body=message)
        print(message)
        time.sleep(delay)

print(" [x] Sent 'Hellow World!'")
connection.close()
