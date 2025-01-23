import pika
import time
import logging
from urllib.parse import urlparse, urlunparse
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('Parser')


class Parser:
    def __init__(self, port, filter_queue='filter_queue', parser_queue='parser_queue', host='localhost'):
        self.filter_queue = filter_queue
        self.parser_queue = parser_queue
        self.host = host
        self.port = port
        self.connection = None
        self.channel = None

    def connect(self):
        """Connect to RabbitMQ and declare the queues."""
        time.sleep(2)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.host, self.port))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.filter_queue)
        self.channel.queue_declare(queue=self.parser_queue)
        logger.info(f"Connected to RabbitMQ on {self.host}, queues: {self.filter_queue}, {self.parser_queue}")

    def start(self):
        """Start consuming messages."""
        self.channel.basic_consume(queue=self.parser_queue, on_message_callback=self.process_message, auto_ack=True)
        logger.info("Parser is consuming...")
        self.channel.start_consuming()

    def process_message(self, ch, method, properties, body):
        """Process a 'ping' message and send a 'pong' response."""
        message = body.decode()
        logger.info(f"Parser received: {message}")

        # Validate if the URL belongs to 'wiki'
        if not self.is_valid_wiki_url(message):
            logger.info(f"URL rejected (not a wiki URL): {message}")
            return

        # Clean the URL by removing fragments and query parameters
        cleaned_url = self.clean_url(message)
        logger.info(f"Cleaned URL: {cleaned_url}")

        # Send the cleaned URL back to the fetch_queue
        self.channel.basic_publish(exchange='', routing_key=self.filter_queue, body=cleaned_url)
        logger.info(f"Cleaned URL sent to fetch queue: {cleaned_url}")

    def is_valid_wiki_url(self, url):
        """Check if the URL belongs to Wikipedia."""
        parsed_url = urlparse(url)
        return parsed_url.netloc.endswith("wikipedia.org")

    def clean_url(self, url):
        """Remove fragments and query parameters from the URL."""
        parsed_url = urlparse(url)
        # Construct URL without fragment or query
        cleaned_url = urlunparse(parsed_url._replace(fragment='', query=''))
        return cleaned_url

    def close(self):
        """Close the connection."""
        if self.connection:
            self.connection.close()
            logger.info("Connection closed.")

# Usage
if __name__ == "__main__":
    rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'localhost')
    rabbitmq_port = os.environ.get('RABBITMQ_PORT', 5672)

    parser = Parser(port=rabbitmq_port, host=rabbitmq_host)
    try:
        parser.connect()
        parser.start()
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        parser.close()
