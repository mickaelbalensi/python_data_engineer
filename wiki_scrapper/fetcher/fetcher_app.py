import pika
import time

class Fetcher:
    def __init__(self, queue_name='fetch_queue', host='localhost'):
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

    def send_ping(self):
        """Send 'ping' messages to the queue."""
        try:
            i = 0
            while True:
                message = "ping" + str(i)
                self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
                print(f"Fetcher sent: {message}")
                time.sleep(2)
                i += 1
        except KeyboardInterrupt:
            print("Fetcher stopped.")
        finally:
            self.close()

    def close(self):
        """Close the connection."""
        if self.connection:
            self.connection.close()
            print("Connection closed.")

# Usage
if __name__ == "__main__":
    fetcher = Fetcher()
    fetcher.connect()
    fetcher.send_ping()
