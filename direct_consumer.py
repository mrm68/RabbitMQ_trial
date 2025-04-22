import pika
import sys


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='direct_logs', exchange_type='direct')

    # Create temporary queue
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue

    # Bind to specific severity (passed as command line arg)
    severities = sys.argv[1:]
    if not severities:
        sys.stderr.write(f"Usage: {sys.argv[0]} [info] [warning] [error]\n")
        sys.exit(1)

    for severity in severities:
        channel.queue_bind(
            exchange='direct_logs',
            queue=queue_name,
            routing_key=severity)

    def callback(ch, method, properties, body):
        print(f" [x] {method.routing_key}:{body.decode()}")

    channel.basic_consume(
        queue=queue_name,
        on_message_callback=callback,
        auto_ack=True)

    print(' [*] Waiting for logs. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        sys.exit(0)
