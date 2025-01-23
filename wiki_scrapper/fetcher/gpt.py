import pika
import requests
import pymongo
import logging

# MongoDB setup
client = pymongo.MongoClient("mongodb://mongo:27017")
db = client.wiki_scraper
links_collection = db.links

# Logger setup
logger = logging.getLogger('fetcher')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
logger.addHandler(ch)

class Fetcher:
    def __init__(self, fetch_queue='fetch_queue', rabbitmq_host='rabbitmq'):
        self.fetch_queue = fetch_queue
        self.rabbitmq_host = rabbitmq_host
        self.connection = None
        self.channel = None

    def connect(self):
        """Connect to RabbitMQ and declare the queue."""
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.rabbitmq_host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.fetch_queue)
        logger.info(f"Connected to RabbitMQ on {self.rabbitmq_host}, queue: {self.fetch_queue}")

    def start(self):
        """Start consuming messages from RabbitMQ."""
        self.channel.basic_consume(queue=self.fetch_queue, on_message_callback=self.process_message, auto_ack=True)
        logger.info("Fetcher is consuming messages...")
        self.channel.start_consuming()

    def process_message(self, ch, method, properties, body):
        """Process the message: save the URL into MongoDB."""
        url = body.decode()
        logger.info(f"Fetching URL: {url}")
        try:
            response = requests.get(url)
            if response.status_code == 200:
                # Save the URL into MongoDB
                links_collection.insert_one({"url": url, "status": "fetched"})
                logger.info(f"Successfully fetched and saved: {url}")
            else:
                logger.warning(f"Failed to fetch {url}: {response.status_code}")
        except requests.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")

    def close(self):
        """Close the RabbitMQ connection."""
        if self.connection:
            self.connection.close()
            logger.info("RabbitMQ connection closed.")

# Start the fetcher
if __name__ == "__main__":
    fetcher = Fetcher()
    fetcher.connect()
    fetcher.start()
