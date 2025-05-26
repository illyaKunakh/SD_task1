import pika
import argparse
import time


insults = ['insult1', 'insult2', 'insult3', 'insult4', 'insult5', 'insult6', 'insult7', 'insult8', 'insult9']

# Connect to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
# Parse command line arguments
parser = argparse.ArgumentParser()

# Add an argument for the queue name
parser.add_argument('--queue', type=str, help='Name of the RabbitMQ queue to consume from')
args = parser.parse_args()  
if args.queue is None:
    queue_name = 'insult_filter_queue'
else:
    queue_name = args.queue

# Declare new rabbit queue
channel.queue_declare(queue=queue_name)

def callback(ch, method, properties, body):
    text = body.decode()
    for insult in insults:
            if insult in text:
                text = text.replace(insult, "CENSORED")
    time.sleep(0.0005)

    # Answer to the client
    ch.basic_publish(exchange='', routing_key=properties.reply_to, body=str(text),)

# Consume messages from the queue
channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

print(f'Consumer: {queue_name}')
print(' Waiting for messages')
channel.start_consuming()