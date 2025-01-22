import pika
import time
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

class Fetcher:
    def __init__(self, port, queue_name='fetch_queue', host='localhost'):
        self.logger = logging.getLogger('Fetcher')
        self.logger.info("Initializing Fetcher...")
        self.queue_name = queue_name
        self.host = host
        self.connection = None
        self.channel = None
        self.port = port

    def connect(self):
        """Connect to RabbitMQ with retry logic."""
        self.logger.info("Starting connection process...")
        max_retries = 10
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                self.logger.info(f"Attempting to connect to RabbitMQ (attempt {attempt + 1}/{max_retries})...")
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=self.host,
                        port=self.port,
                        connection_attempts=3,
                        retry_delay=1
                    )
                )
                self.channel = self.connection.channel()
                self.channel.queue_declare(queue=self.queue_name)
                self.logger.info(f"Successfully connected to RabbitMQ on {self.host}, queue: {self.queue_name}")
                return
            except pika.exceptions.AMQPConnectionError as e:
                self.logger.error(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    self.logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    self.logger.critical("Max retries reached. Could not connect to RabbitMQ")
                    raise

    def send_ping(self):
        """Send 'ping' messages to the queue."""
        self.logger.info("Starting send_ping method...")
        try:
            i = 0
            while True:
                message = f"ping{i}"
                self.channel.basic_publish(
                    exchange='',
                    routing_key=self.queue_name,
                    body=message
                )
                self.logger.info(f"Fetcher sent: {message}")
                time.sleep(2)
                i += 1
        except Exception as e:
            self.logger.error(f"Error in send_ping: {str(e)}", exc_info=True)
            raise
        except KeyboardInterrupt:
            self.logger.info("Fetcher stopped.")
        finally:
            self.close()

    def close(self):
        """Close the connection."""
        if self.connection:
            self.connection.close()
            self.logger.info("Connection closed.")

if __name__ == "__main__":
    logger = logging.getLogger('FetcherMain')
    logger.info("Starting Fetcher main...")
    fetcher = Fetcher(port=5672, host='rabbitmq')
    try:
        fetcher.connect()
        logger.info("Starting to send pings...")
        fetcher.send_ping()
    except Exception as e:
        logger.error(f"Error in main: {str(e)}", exc_info=True)



# class Fetcher:
#     def __init__(self, port, queue_name='fetch_queue', host='localhost'):
#         self.queue_name = queue_name
#         self.host = host
#         self.connection = None
#         self.channel = None
#         self.port = port

#     def connect(self):
#         """Connect to RabbitMQ and declare the queue."""
#         subprocess.run(["/app/wait-for-it.sh", "rabbitmq:5672", "--", "echo", "RabbitMQ is ready!"])
#         time.sleep(2)
#         self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.host, self.port))
#         self.channel = self.connection.channel()
#         self.channel.queue_declare(queue=self.queue_name)
#         print(f"Connected to RabbitMQ on {self.host}, queue: {self.queue_name}")

#     def send_ping(self):
#         """Send 'ping' messages to the queue."""
#         try:
#             i = 0
#             while True:
#                 message = "ping" + str(i)
#                 self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
#                 print(f"Fetcher sent: {message}")
#                 time.sleep(2)
#                 i += 1
#         except KeyboardInterrupt:
#             print("Fetcher stopped.")
#         finally:
#             self.close()

#     def close(self):
#         """Close the connection."""
#         if self.connection:
#             self.connection.close()
#             print("Connection closed.")

# # Usage
# if __name__ == "__main__":
#     fetcher = Fetcher(port= 5672, host='rabbitmq')
#     fetcher.connect()
#     fetcher.send_ping()
