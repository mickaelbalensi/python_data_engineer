import pika
import time
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('Parser')

class Parser:
    def __init__(self, port, fetch_queue='fetch_queue', parser_queue='parser_queue', host='localhost'):
        self.fetch_queue = fetch_queue
        self.parser_queue = parser_queue
        self.host = host
        self.port = port
        self.connection = None
        self.channel = None
        self.pong_num = 0

    def connect(self):
        """Connect to RabbitMQ and declare the queues."""
        time.sleep(2)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.host, self.port))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.fetch_queue)
        self.channel.queue_declare(queue=self.parser_queue)
        logger.info(f"Connected to RabbitMQ on {self.host}, queues: {self.fetch_queue}, {self.parser_queue}")

    def start(self):
        """Start consuming messages."""
        self.channel.basic_consume(queue=self.fetch_queue, on_message_callback=self.process_message, auto_ack=True)
        logger.info("Parser is consuming...")
        self.channel.start_consuming()

    def process_message(self, ch, method, properties, body):
        """Process a 'ping' message and send a 'pong' response."""
        message = body.decode()
        logger.info(f"Parser received: {message}")
        if "ping" in message:
            response = "pong" + str(self.pong_num)
            self.channel.basic_publish(exchange='', routing_key=self.parser_queue, body=response)
            logger.info(f"Parser sent: {response}")
            self.pong_num += 1

    def close(self):
        """Close the connection."""
        if self.connection:
            self.connection.close()
            logger.info("Connection closed.")

# Usage
if __name__ == "__main__":
    parser = Parser(port=5672, host='rabbitmq')
    try:
        parser.connect()
        parser.start()
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        parser.close()
