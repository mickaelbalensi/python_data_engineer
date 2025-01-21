import pika

class Parser:
    def __init__(self, fetch_queue='fetch_queue', parser_queue='parser_queue', host='localhost'):
        self.fetch_queue = fetch_queue
        self.parser_queue = parser_queue
        self.host = host
        self.connection = None
        self.channel = None
        self.pong_num = 0

    def connect(self):
        """Connect to RabbitMQ and declare the queues."""
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.fetch_queue)
        self.channel.queue_declare(queue=self.parser_queue)
        print(f"Connected to RabbitMQ on {self.host}, queues: {self.fetch_queue}, {self.parser_queue}")

    def start(self):
        """Start consuming messages."""
        self.channel.basic_consume(queue=self.fetch_queue, on_message_callback=self.process_message, auto_ack=True)
        print("Parser is consuming...")
        self.channel.start_consuming()

    def process_message(self, ch, method, properties, body):
        """Process a 'ping' message and send a 'pong' response."""
        message = body.decode()
        print(f"Parser received: {message}")
        if "ping" in message:
            response = "pong" + str(self.pong_num)
            self.channel.basic_publish(exchange='', routing_key=self.parser_queue, body=response)
            print(f"Parser sent: {response}")
            self.pong_num += 1

    def close(self):
        """Close the connection."""
        if self.connection:
            self.connection.close()
            print("Connection closed.")

# Usage
if __name__ == "__main__":
    parser = Parser()
    parser.connect()
    parser.start()
