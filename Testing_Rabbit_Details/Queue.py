import requests
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='hello')

channel.basic_publish(exchange='',
                      routing_key='hello',
                      body='Hello World!')
print(" [x] Sent 'Hello World!'")

connection.close()


params = pika.ConnectionParameters(host='node1.cluster.internal')

credentials = pika.PlainCredentials('admin', 'pass')
params = pika.ConnectionParameters(credentials=credentials)

props = pika.BasicProperties(message_id='123')
channel.basic_publish(..., properties=props)
