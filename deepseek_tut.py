from dataclasses import dataclass
import pika
import time
import json


@dataclass
class Message:
    content: str
    priority: int = 0
    ttl: int = None


class RabbitMQManager:
    def __init__(self, host='localhost'):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host)
        )
        self.channel = self.connection.channel()

    def setup_infrastructure(self):
        # Dead Letter Exchange
        self.channel.exchange_declare(exchange='dlx', exchange_type='direct')
        self.channel.queue_declare(queue='dead_letters')
        self.channel.queue_bind(exchange='dlx', queue='dead_letters')

        # Priority Queue with DLX
        args = {
            'x-max-priority': 10,
            'x-dead-letter-exchange': 'dlx'
        }
        self.channel.queue_declare(
            queue='orders',
            durable=True,
            arguments=args
        )

    def publish(self, message: Message):
        properties = pika.BasicProperties(
            priority=message.priority,
            delivery_mode=2,
            expiration=str(message.ttl) if message.ttl else None
        )
        self.channel.basic_publish(
            exchange='',
            routing_key='orders',
            body=json.dumps(message.content),
            properties=properties
        )

    def consume(self):
        self.channel.basic_qos(prefetch_count=5)

        def callback(ch, method, properties, body):
            try:
                data = json.loads(body)
                print(f"Processing: {data}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception:
                ch.basic_nack(delivery_tag=method.delivery_tag)

        self.channel.basic_consume(
            queue='orders',
            on_message_callback=callback,
            auto_ack=False
        )
        self.channel.start_consuming()


# Usage
manager = RabbitMQManager()
manager.setup_infrastructure()

# Send message
manager.publish(Message(
    content="New order: 42",
    priority=8,
    ttl=30000
))

# Start consumer
manager.consume()
