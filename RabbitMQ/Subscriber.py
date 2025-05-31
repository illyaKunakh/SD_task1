import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Exchange declaration
channel.exchange_declare(exchange='notify_subs', exchange_type='fanout')

# New queue declaration, autodelete when user disconnects
result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue

# Bind the queue to the exchange
channel.queue_bind(exchange='notify_subs', queue=queue_name)

print("Subscriber")
print(' Waiting messages')

def callback(ch, methos, property, body):
    print(f" Received {body.decode()}")

channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
channel.start_consuming()