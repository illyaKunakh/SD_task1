import pika

# Connect to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare exchange
channel.exchange_declare(exchange='notify_subs', exchange_type='fanout')

# Create a new temporary queue (random name, auto-delete when consumer disconnects)
result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue

# Bind the queue to the exchange
channel.queue_bind(exchange='notify_subs', queue=queue_name)

print("Subscriber")
print(' [*] Waiting for messages. To exit, press CTRL+C')

def callback(ch, methos, property, body):
    print(f" [x] Received {body.decode()}")

# Consume messages
channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
channel.start_consuming()