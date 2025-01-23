import requests
from bs4 import BeautifulSoup
import pika
import logging
import json
import os

# Configure logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FIRST_URL = "https://en.wikipedia.org/wiki/Web_scraping"
RABBIT_HOST = 'rabbitmq'
PORT = 5672


class Fetcher:
    def __init__(self, host='localhost', port=5672, fetch_queue='fetch_queue', parser_queue='parser_queue', save_dir='./html_pages'):
        self.host = host
        self.port = port
        self.fetch_queue = fetch_queue
        self.parser_queue = parser_queue
        self.save_dir = save_dir
        self.connection = None
        self.channel = None

        # Create the directory for saving HTML pages if it doesn't exist
        if not os.path.exists(self.save_dir):
            os.makedirs(self.save_dir)

    def connect(self):
        """Connect to RabbitMQ and declare the queues."""
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.host, self.port))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.fetch_queue)
        self.channel.queue_declare(queue=self.parser_queue)
        logger.info(f"Connected to RabbitMQ on {self.host}, queues: {self.fetch_queue}, {self.parser_queue}")

    def seed_queue(self, initial_url):
        """Seed the fetch queue with the initial URL."""
        self.channel.basic_publish(exchange='', routing_key=self.fetch_queue, body=initial_url)
        logger.info(f"Initial URL seeded: {initial_url}")

    def start(self):
        """Start consuming messages."""
        self.channel.basic_consume(queue=self.fetch_queue, on_message_callback=self.process_message, auto_ack=True)
        logger.info("Fetcher is consuming...")
        self.channel.start_consuming()

    def process_message(self, ch, method, properties, body):
        """Process a message containing a Wikipedia URL, save the HTML, and extract links."""
        url = body.decode()
        logger.info(f"Received URL: {url}")
        try:
            # Fetch the HTML content of the URL
            response = requests.get(url)
            response.raise_for_status()  # Raise an exception for HTTP errors
            html_content = response.text

            # Save the HTML content locally
            self.save_html(url, html_content)

            # Extract all links from the HTML
            links = self.extract_links(html_content, url)

            # Push extracted links to the parser queue
            for link in links:
                self.channel.basic_publish(exchange='', routing_key=self.parser_queue, body=link)
            logger.info(f"Fetched and processed URL: {url}. Links sent to parser queue.")
        except requests.RequestException as e:
            logger.error(f"Failed to fetch URL {url}: {e}")

    def save_html(self, url, html_content):
        """Save the HTML content to a local file."""
        filename = os.path.join(self.save_dir, f"{self.sanitize_filename(url)}.html")
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(html_content)
        logger.info(f"HTML content saved to {filename}")

    def extract_links(self, html_content, base_url):
        """Extract all links from the HTML content."""
        soup = BeautifulSoup(html_content, 'html.parser')
        links = []
        for a_tag in soup.find_all('a', href=True):
            link = a_tag['href']
            if link.startswith('/'):  # Convert relative URLs to absolute
                link = requests.compat.urljoin(base_url, link)
            if link.startswith('http'):  # Ensure it's an absolute URL
                links.append(link)
        logger.info(f"Extracted {len(links)} links from {base_url}")
        return links

    def sanitize_filename(self, url):
        """Generate a safe filename from a URL."""
        return url.replace('https://', '').replace('http://', '').replace('/', '_').replace(':', '_')

    def close(self):
        """Close the connection."""
        if self.connection:
            self.connection.close()
            logger.info("Connection closed.")

# Usage
if __name__ == "__main__":
    fetcher = Fetcher(host=RABBIT_HOST, port=PORT)
    fetcher.connect()
    fetcher.seed_queue(FIRST_URL)
    fetcher.start()


# import pika
# import time
# import logging
# import sys

# # Configure logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.StreamHandler(sys.stdout)
#     ]
# )

# class Fetcher:
#     def __init__(self, port, queue_name='fetch_queue', host='localhost'):
#         self.logger = logging.getLogger('Fetcher')
#         self.logger.info("Initializing Fetcher...")
#         self.queue_name = queue_name
#         self.host = host
#         self.connection = None
#         self.channel = None
#         self.port = port

#     def connect(self):
#         """Connect to RabbitMQ with retry logic."""
#         self.logger.info("Starting connection process...")
#         max_retries = 10
#         retry_delay = 2
        
#         for attempt in range(max_retries):
#             try:
#                 self.logger.info(f"Attempting to connect to RabbitMQ (attempt {attempt + 1}/{max_retries})...")
#                 self.connection = pika.BlockingConnection(
#                     pika.ConnectionParameters(
#                         host=self.host,
#                         port=self.port,
#                         connection_attempts=3,
#                         retry_delay=1
#                     )
#                 )
#                 self.channel = self.connection.channel()
#                 self.channel.queue_declare(queue=self.queue_name)
#                 self.logger.info(f"Successfully connected to RabbitMQ on {self.host}, queue: {self.queue_name}")
#                 return
#             except pika.exceptions.AMQPConnectionError as e:
#                 self.logger.error(f"Connection attempt {attempt + 1} failed: {e}")
#                 if attempt < max_retries - 1:
#                     self.logger.info(f"Retrying in {retry_delay} seconds...")
#                     time.sleep(retry_delay)
#                 else:
#                     self.logger.critical("Max retries reached. Could not connect to RabbitMQ")
#                     raise

#     def send_ping(self):
#         """Send 'ping' messages to the queue."""
#         self.logger.info("Starting send_ping method...")
#         try:
#             i = 0
#             while True:
#                 message = f"ping{i}"
#                 self.channel.basic_publish(
#                     exchange='',
#                     routing_key=self.queue_name,
#                     body=message
#                 )
#                 self.logger.info(f"Fetcher sent: {message}")
#                 time.sleep(2)
#                 i += 1
#         except Exception as e:
#             self.logger.error(f"Error in send_ping: {str(e)}", exc_info=True)
#             raise
#         except KeyboardInterrupt:
#             self.logger.info("Fetcher stopped.")
#         finally:
#             self.close()

#     def close(self):
#         """Close the connection."""
#         if self.connection:
#             self.connection.close()
#             self.logger.info("Connection closed.")

# if __name__ == "__main__":
#     logger = logging.getLogger('FetcherMain')
#     logger.info("Starting Fetcher main...")
#     fetcher = Fetcher(port=5672, host='rabbitmq')
#     try:
#         fetcher.connect()
#         logger.info("Starting to send pings...")
#         fetcher.send_ping()
#     except Exception as e:
#         logger.error(f"Error in main: {str(e)}", exc_info=True)

