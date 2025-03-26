import pika
import random
import time

import sys
sys.path.append("..")  # Adds the parent directory to the module search path
import text

# Connect to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare a queue
channel.queue_declare(queue='text')

try:
    while True:
        insult = random.choice(text.text)
        channel.basic_publish(exchange='', routing_key='text', body=insult)
        print(f" [x] Sent insult: {insult}")
        time.sleep(5)
except KeyboardInterrupt:
    print(" [x] Stopping InsultProducer...")
    connection.close()
