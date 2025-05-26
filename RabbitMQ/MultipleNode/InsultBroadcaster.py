import pika
import multiprocessing
import redis
import random
import time

def startBroadcast():
    global broadcast_active
    if broadcast_active is None:
        broadcast_active = multiprocessing.Process(target=notify)
        broadcast_active.start()
        print(" Started Broadcast")
    else:
        print (" Broadcast already active")

# Send random insults every 5 seconds to subscribers
def notify():
    insults = ['insult1', 'insult2', 'insult3', 'insult4', 'insult5', 'insult6', 'insult7', 'insult8', 'insult9']
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    while True:
        insult = random.choice(insults)
        channel.basic_publish(exchange='notify_subs', routing_key='', body=insult)
        print(f" Broadcaster sent: '{insult}'")
        time.sleep(5)

def stopBroadcast():
    global broadcast_active
    if broadcast_active is not None:
        broadcast_active.terminate()
        broadcast_active.join()
        broadcast_active = None
        print(" Stopping broadcast ")
    else:
        print (" No Broadcast active ")

def callback(ch, method, properties, body):
    value = body.decode()
    print(f" Received: {value}")
    if value == 'start':
        startBroadcast()
    elif value == 'stop':
        stopBroadcast()

if __name__ == "__main__":
    broadcast_active = None

    # Connect to RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare("broadcast_queue")

    # Declare a fanout exchange
    channel.exchange_declare(exchange='notify_subs', exchange_type='fanout')

    channel.basic_consume(queue="broadcast_queue", on_message_callback=callback, auto_ack=True)

    channel.start_consuming()