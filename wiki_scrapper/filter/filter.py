import pika

class Filter:
    def __init__(self, queue_name='filter_queue', host='rabbitmq'):
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
        """Process messages from the filter_queue."""
        print(f"Received from filter_queue: {body.decode()}")

    def close(self):
        """Close the connection."""
        if self.connection:
            self.connection.close()
            print("Connection closed.")

# Usage
if __name__ == "__main__":
    consumer = Filter()
    consumer.connect()
    consumer.start()
