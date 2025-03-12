import pika
import uuid

class RpcClient:
    def __init__(self):
        """Setup connection and queue for response"""
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()

        # Create a temporary queue for replies
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        # Listen for responses
        self.channel.basic_consume(queue=self.callback_queue, on_message_callback=self.on_response, auto_ack=True)

        self.response = None
        self.corr_id = None

    def on_response(self, ch, method, properties, body):
        """Callback for receiving responses"""
        if self.corr_id == properties.correlation_id:
            self.response = body.decode()

    def call(self, n):
        """Send RPC request"""
        self.response = None
        self.corr_id = str(uuid.uuid4())  # Unique ID for request

        self.channel.basic_publish(
            exchange='',
            routing_key='rpc_queue',
            properties=pika.BasicProperties(reply_to=self.callback_queue, correlation_id=self.corr_id),
            body=str(n),
        )

        # Wait for response
        while self.response is None:
            self.connection.process_data_events()
        
        return int(self.response)

# Run the client
rpc_client = RpcClient()
number = 6  # Change this value to test with different numbers
print(f" [x] Requesting fib({number})")
response = rpc_client.call(number)
print(f" [.] Got {response}")
