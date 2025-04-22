import pika


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='headers_logs', exchange_type='headers')

    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue

    # Bind with specific headers
    headers = {
        'priority': 'high',
        'app': 'web',
        'x-match': 'any'  # Match any of these
    }

    channel.queue_bind(
        exchange='headers_logs',
        queue=queue_name,
        arguments=headers)

    def callback(ch, method, properties, body):
        print(f" [x] Headers:{properties.headers} - {body.decode()}")

    channel.basic_consume(
        queue=queue_name,
        on_message_callback=callback,
        auto_ack=True)

    print(' [*] Waiting for high priority or web app messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
