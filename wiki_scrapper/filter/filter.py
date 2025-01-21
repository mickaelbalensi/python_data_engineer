import pika

class PongConsumer:
    def __init__(self, queue_name='parser_queue', host='localhost'):
        self.queue_name = queue_name
        self.host = host
        self.connection = None
        self.channel = None

    def connect(self):
        """Connect to RabbitMQ and declare the queue."""
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name)
        print(f"Connected to RabbitMQ on {self.host}, queue: {self.queue_name}")

    def start(self):
        """Start consuming messages."""
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.process_message, auto_ack=True)
        print("PongConsumer is consuming...")
        self.channel.start_consuming()

    def process_message(self, ch, method, properties, body):
        """Process messages from the parser_queue."""
        print(f"Received from parser_queue: {body.decode()}")

    def close(self):
        """Close the connection."""
        if self.connection:
            self.connection.close()
            print("Connection closed.")

# Usage
if __name__ == "__main__":
    consumer = PongConsumer()
    consumer.connect()
    consumer.start()
