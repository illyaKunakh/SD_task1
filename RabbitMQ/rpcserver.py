import pika

def fib(n):
    """Fibonacci function for RPC"""
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        return fib(n - 1) + fib(n - 2)

# Connect to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare the queue for receiving RPC requests
channel.queue_declare(queue='rpc_queue')

def on_request(ch, method, properties, body):
    """Callback function to process RPC requests"""
    n = int(body.decode())
    print(f" [.] fib({n}) requested")
    
    response = fib(n)  # Compute the Fibonacci result

    # Send response back to the client
    ch.basic_publish(
        exchange='',
        routing_key=properties.reply_to,
        properties=pika.BasicProperties(correlation_id=properties.correlation_id),
        body=str(response),
    )

    # Acknowledge message
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Consume messages from the RPC queue
channel.basic_consume(queue='rpc_queue', on_message_callback=on_request)

print(" [x] Waiting for RPC requests")
channel.start_consuming()
