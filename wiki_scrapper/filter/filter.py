import pika
import redis
import logging
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('Filter')

class Filter:
    def __init__(self, consume_queue='filter_queue', produce_queue='fetch_queue', rabbitmq_host='rabbitmq', rabbitmq_port=5672, redis_host='redis', redis_port=6379):
        self.consume_queue = consume_queue
        self.produce_queue = produce_queue
        self.rabbitmq_host = rabbitmq_host
        self.rabbitmq_port = rabbitmq_port
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.connection = None
        self.channel = None
        self.redis_client = None

    def connect(self):
        """Connect to RabbitMQ and Redis, and declare the queues."""
        try:
            # Connect to RabbitMQ
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.rabbitmq_host, self.rabbitmq_port))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.consume_queue, durable=True)
            self.channel.queue_declare(queue=self.produce_queue, durable=True)
            logger.info(f"Connected to RabbitMQ on {self.rabbitmq_host}, queues: {self.consume_queue}, {self.produce_queue}")

            # Connect to Redis
            self.redis_client = redis.Redis(host=self.redis_host, port=self.redis_port, decode_responses=True)
            logger.info(f"Connected to Redis on {self.redis_host}:{self.redis_port}")
        except Exception as e:
            logger.error(f"Error connecting to RabbitMQ or Redis: {e}")
            raise

    def start(self):
        """Start consuming messages."""
        try:
            self.channel.basic_consume(queue=self.consume_queue, on_message_callback=self.process_message, auto_ack=False)
            logger.info(f"Consuming messages from {self.consume_queue}...")
            self.channel.start_consuming()
        except Exception as e:
            logger.error(f"Error starting consumer: {e}")

    def process_message(self, ch, method, properties, body):
        """Process messages from the consume_queue."""
        try:
            link = body.decode()
            logger.info(f"Received message: {link}")

            # Check if the link is a duplicate
            if self.redis_client.sismember('processed_links', link):
                logger.info(f"Duplicate link ignored: {link}")
                return

            # Add the link to Redis set
            self.redis_client.sadd('processed_links', link)
            logger.info(f"Added new link to Redis: {link}")

            # Publish the unique link to the fetch_queue
            self.channel.basic_publish(exchange='', routing_key=self.produce_queue, body=link, properties=pika.BasicProperties(delivery_mode=2))
            logger.info(f"Pushed link to {self.produce_queue}: {link}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def close(self):
        """Close the connections to RabbitMQ and Redis."""
        try:
            if self.connection:
                self.connection.close()
                logger.info("RabbitMQ connection closed.")
        except Exception as e:
            logger.error(f"Error closing RabbitMQ connection: {e}")
        try:
            if self.redis_client:
                logger.info("Redis connection closed.")
        except Exception as e:
            logger.error(f"Error closing Redis connection: {e}")


# Usage
if __name__ == "__main__":
    rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'localhost')
    rabbitmq_port = os.environ.get('RABBITMQ_PORT', 5672)
    redis_host = os.environ.get('REDIS_HOST', 'localhost')
    redis_port = os.environ.get('REDIS_PORT', 6379)

    logger.info(f"RabbitMQ host: {rabbitmq_host}")
    filter_ = Filter(rabbitmq_host=rabbitmq_host, rabbitmq_port=rabbitmq_port, redis_host=redis_host, redis_port=redis_port)
    
    try:
        logger.info(f"Redis host: {redis_host}")
        filter_.connect()
        filter_.start()
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        filter_.close()
