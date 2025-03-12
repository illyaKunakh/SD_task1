import pika

# Connect to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare a fanout exchange
channel.exchange_declare(exchange='logs', exchange_type='fanout')

# Publish a message
message = "Hello, all consumers!"
channel.basic_publish(exchange='logs', routing_key='', body=message)

print(f" [x] Sent '{message}'")

# Close connection
connection.close()
