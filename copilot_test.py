import pika
import ssl
import json
import zlib
import time
import logging
import requests

# Set up logging for tracing, debugging, and monitoring.
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")

#############################################
# Connection Management (Connections, TLS, Auth)
#############################################


class RabbitMQConnectionManager:
    def __init__(self, host, port, username, password, tls=False, virtual_host='/'):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.tls = tls
        self.virtual_host = virtual_host
        self.connection = None

    def connect(self):
        credentials = pika.PlainCredentials(self.username, self.password)
        parameters = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            virtual_host=self.virtual_host,
            credentials=credentials
        )
        if self.tls:
            ssl_context = ssl.create_default_context()
            parameters = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                virtual_host=self.virtual_host,
                credentials=credentials,
                ssl_options=pika.SSLOptions(ssl_context)
            )
        self.connection = pika.BlockingConnection(parameters)
        logging.info("Connected to RabbitMQ broker with TLS=%s", self.tls)
        return self.connection

#############################################
# Channel and Exchange/Queue Management (Exchanges, Queues, Bindings, Prefetch)
#############################################


class RabbitMQChannelManager:
    def __init__(self, connection):
        self.connection = connection
        self.channel = self.connection.channel()

    def setup_exchange(self, exchange, exchange_type='direct', durable=True, arguments=None):
        self.channel.exchange_declare(
            exchange=exchange,
            exchange_type=exchange_type,
            durable=durable,
            arguments=arguments
        )
        logging.info("Exchange '%s' declared (type=%s)",
                     exchange, exchange_type)

    def setup_queue(self, queue, durable=True, arguments=None):
        self.channel.queue_declare(
            queue=queue,
            durable=durable,
            arguments=arguments
        )
        logging.info("Queue '%s' declared", queue)

    def bind_queue(self, queue, exchange, routing_key):
        self.channel.queue_bind(
            queue=queue, exchange=exchange, routing_key=routing_key)
        logging.info("Queue '%s' bound to exchange '%s' with routing key '%s'",
                     queue, exchange, routing_key)

    def set_prefetch(self, prefetch_count):
        self.channel.basic_qos(prefetch_count=prefetch_count)
        logging.info("Channel prefetch count set to %s", prefetch_count)

#############################################
# Producer (Message Publishing, Compression, Rate Limiting, Delayed Messaging, Priority)
#############################################


class RabbitMQProducer:
    def __init__(self, channel, rate_limit=0):
        self.channel = channel
        self.rate_limit = rate_limit  # messages per second (0 means no limit)
        self.last_publish_time = 0

    def _apply_rate_limit(self):
        if self.rate_limit > 0:
            delay = 1.0 / self.rate_limit
            now = time.time()
            elapsed = now - self.last_publish_time
            if elapsed < delay:
                time.sleep(delay - elapsed)
            self.last_publish_time = time.time()

    def publish(self, exchange, routing_key, message, priority=0, ttl=None, delay=None, compress=False):
        self._apply_rate_limit()

        properties = pika.BasicProperties(
            delivery_mode=2,  # Make message persistent
            priority=priority,
            # TTL in milliseconds (for message expiry)
            expiration=str(ttl) if ttl else None
        )

        body = message.encode('utf-8')
        if compress:
            body = zlib.compress(body)
            properties.content_encoding = "deflate"
            logging.info("Message compressed using deflate")
        if delay:
            # Delayed messaging can be implemented by publishing to a special exchange configured
            # with the delayed message plugin. Here the delay is simulated by the expiration property.
            properties.expiration = str(delay)
            logging.info("Publishing with a delay of %sms", delay)

        self.channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=body,
            properties=properties
        )
        logging.info(
            "Published message to exchange '%s' with routing key '%s'", exchange, routing_key)

#############################################
# Consumer (Message Consumption, Acknowledgment, Event-driven processing)
#############################################


class RabbitMQConsumer:
    def __init__(self, channel, queue, prefetch_count=1):
        self.channel = channel
        self.queue = queue
        self.channel.basic_qos(prefetch_count=prefetch_count)
        logging.info(
            "Consumer created for queue '%s' with prefetch count %s", queue, prefetch_count)

    def callback(self, ch, method, properties, body):
        # If message was compressed, decompress it
        if properties.content_encoding == "deflate":
            body = zlib.decompress(body)
        message = body.decode('utf-8')
        logging.info("Received message: %s", message)

        # Simulate processing (event-driven, can integrate tracing/debug)
        time.sleep(1)  # Processing delay simulation

        # Acknowledge message to RabbitMQ
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logging.info("Message acknowledged")

    def start_consuming(self):
        self.channel.basic_consume(
            queue=self.queue, on_message_callback=self.callback, auto_ack=False)
        logging.info("Started consuming on queue '%s'", self.queue)
        self.channel.start_consuming()

#############################################
# Management API Client (Monitoring & Metrics)
#############################################


class ManagementAPIClient:
    def __init__(self, base_url, username, password):
        self.base_url = base_url  # e.g., "http://localhost:15672"
        self.auth = (username, password)

    def get_overview(self):
        try:
            response = requests.get(
                f"{self.base_url}/api/overview", auth=self.auth)
            if response.status_code == 200:
                overview = response.json()
                logging.info("Retrieved management API overview")
                return overview
            else:
                logging.error(
                    "Failed to retrieve overview: status %s", response.status_code)
        except Exception as e:
            logging.error("Error retrieving management overview: %s", str(e))

#############################################
# Plugin Manager (Simulate enabling RabbitMQ plugins)
#############################################


class PluginManager:
    def __init__(self):
        self.enabled_plugins = []

    def enable_plugin(self, plugin_name):
        # In practice, plugins are enabled on the broker via configuration.
        self.enabled_plugins.append(plugin_name)
        logging.info("Plugin '%s' enabled (simulated)", plugin_name)

#############################################
# QueueConfigurator (Dead Letter, TTL, Priority, Mirrored Queues, Memory vs. Disk)
#############################################


class QueueConfigurator:
    def get_arguments(self, dead_letter_exchange=None, ttl=None, max_priority=None, ha_mode=False, durability="disk"):
        arguments = {}
        if dead_letter_exchange:
            arguments['x-dead-letter-exchange'] = dead_letter_exchange
        if ttl:
            arguments['x-message-ttl'] = ttl  # TTL in milliseconds
        if max_priority:
            arguments['x-max-priority'] = max_priority
        if ha_mode:
            # This argument simulates mirror queues for high availability
            arguments['x-ha-policy'] = 'all'
        # Note: "durability" here is signaled via queue declaration; for in-memory queues, one simply declares non-durable queues.
        return arguments

#############################################
# Cluster, Federation & Shovel Configurators (Simulated)
#############################################


class ClusterConfigurator:
    def configure_cluster(self):
        # In a real deployment, clustering is done at the broker level.
        logging.info("Cluster configuration applied (simulated)")


class FederationConfigurator:
    def configure_federation(self, upstream_uri):
        # In practice, federation is set up via the management UI or configuration file.
        logging.info(
            "Federation configured with upstream URI '%s' (simulated)", upstream_uri)


class ShovelConfigurator:
    def configure_shovel(self, source_uri, destination_uri):
        # The shovel plugin moves messages between brokers.
        logging.info("Shovel configured from '%s' to '%s' (simulated)",
                     source_uri, destination_uri)

#############################################
# RabbitMQSystem – Main Integrator of All Aspects
#############################################


class RabbitMQSystem:
    def __init__(self, host, port, username, password, tls=False):
        # Manage connection
        self.connection_manager = RabbitMQConnectionManager(
            host, port, username, password, tls)
        self.connection = self.connection_manager.connect()
        self.channel_manager = RabbitMQChannelManager(self.connection)
        # Simulate enabling plugins
        self.plugin_manager = PluginManager()
        # Simulated advanced configurations
        self.cluster_configurator = ClusterConfigurator()
        self.federation_configurator = FederationConfigurator()
        self.shovel_configurator = ShovelConfigurator()
        self.queue_configurator = QueueConfigurator()
        # Management API for monitoring/metrics
        self.management_api_client = ManagementAPIClient(
            "http://localhost:15672", username, password)

    def setup_system(self):
        # Enable plugins for federation and shovel
        self.plugin_manager.enable_plugin("rabbitmq_shovel")
        self.plugin_manager.enable_plugin("rabbitmq_federation")
        # Apply clustering (simulated)
        self.cluster_configurator.configure_cluster()
        # Apply federation and shovel configuration (simulated)
        self.federation_configurator.configure_federation("amqp://upstream")
        self.shovel_configurator.configure_shovel(
            "amqp://source", "amqp://destination")

        # Setup exchanges
        self.channel_manager.setup_exchange(
            "main_exchange", exchange_type="direct", durable=True)
        # For delayed messaging, we use a plugin-provided exchange type (x-delayed-message)
        self.channel_manager.setup_exchange("delayed_exchange", exchange_type="x-delayed-message", durable=True,
                                            arguments={"x-delayed-type": "direct"})
        # Setup queue with advanced options: DLX, TTL, priority, mirrored (HA), and disk-based durability.
        dlx_arguments = self.queue_configurator.get_arguments(
            dead_letter_exchange="main_exchange",
            ttl=60000,       # messages expire in 60 sec if not consumed
            max_priority=10,  # support up to 10 message priority levels
            ha_mode=True,    # mirrored queue for High Availability
            durability="disk"
        )
        self.channel_manager.setup_queue(
            "task_queue", durable=True, arguments=dlx_arguments)
        # Bind the queue to the main exchange with a routing key.
        self.channel_manager.bind_queue(
            "task_queue", "main_exchange", "task_routing")
        # Optionally, you could set a prefetch count for load balancing and flow control.
        self.channel_manager.set_prefetch(5)

    def send_test_message(self):
        producer = RabbitMQProducer(self.channel_manager.channel, rate_limit=5)
        # Publish a normal persistent message with compression enabled.
        producer.publish("main_exchange", "task_routing",
                         "Hello, RabbitMQ!", priority=5, compress=True)
        # Publish a delayed message (using a special delayed exchange)
        producer.publish("delayed_exchange", "task_routing",
                         "Delayed Hello, RabbitMQ!", delay=5000)

    def start_consumer(self):
        consumer = RabbitMQConsumer(
            self.channel_manager.channel, "task_queue", prefetch_count=5)
        # This call blocks; run in a separate process/thread in production
        consumer.start_consuming()

    def monitor(self):
        overview = self.management_api_client.get_overview()
        if overview:
            logging.info("Management API Overview: %s",
                         json.dumps(overview, indent=2))

#############################################
# Main Execution
#############################################


def main():
    # Initialize the RabbitMQ system with connection details.
    rabbit_system = RabbitMQSystem(
        host="localhost",
        port=5672,
        username="guest",
        password="guest",
        tls=False  # Set to True if TLS is configured on your broker
    )
    # Setup all components: plugins, exchanges, queues, bindings, clustering, etc.
    rabbit_system.setup_system()
    # Send test messages (demonstrates producers, rate limiting, compression and delayed messaging)
    rabbit_system.send_test_message()
    # Monitor the system using the management API (retrieves metrics/overview)
    rabbit_system.monitor()
    # Uncomment the next line to start the consumer (this will block execution)
    # rabbit_system.start_consumer()


if __name__ == "__main__":
    main()
