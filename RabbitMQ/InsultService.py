import pika
import redis
import random
import time
import argparse
import multiprocessing

def parse_args():
    parser = argparse.ArgumentParser(description="Insult Service for RabbitMQ")
    parser.add_argument('--broadcast', action='store_true', help='Enable broadcasting')
    return parser.parse_args()

# Broadcasts a random insult to all subscribers every 5 seconds connected to a fanout exchange
# 1. Connects to RabbitMQ and declares a fanout exchange
# 2. Retrieves insults from Redis
# 3. Randomly selects an insult from the list
# 4. Publishes the insult to the exchange
# 5. Sleeps for 5 seconds before repeating
def broadcast_insults():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.exchange_declare(exchange='notify_subs', exchange_type='fanout')
    while True:
        insults = redis_client.lrange("insult_list", 0, -1)
        if insults:
            insult = random.choice(insults)
            channel.basic_publish(exchange='notify_subs', routing_key='', body=insult)
            print(f"Broadcasted: {insult}")
        time.sleep(5)
    connection.close()

def start_broadcast():
    global broadcast_process
    if broadcast_process is None:
        broadcast_process = multiprocessing.Process(target=broadcast_insults)
        broadcast_process.start()
        print("Broadcast started")
    else:
        print("Broadcast already active")

def stop_broadcast():
    global broadcast_process
    if broadcast_process is not None:
        broadcast_process.terminate()
        broadcast_process.join()
        broadcast_process = None
        print("Broadcast stopped")
    else:
        print("No broadcast active")

# Called every time a message is received
# 1. Decodes the message body
# 2. Checks if the message is a request for a random insult or a broadcast command
# 3. If it's a request for a random insult, retrieves a random insult from Redis and sends it back
# 4. If it's a broadcast command, starts or stops the broadcast process
# 5. If it's a new insult, stores it in Redis if it doesn't already exist
# 6. Prints the received message and the response sent back
def callback(ch, method, properties, body):
    value = body.decode()
    print(f"Received: {value}")
    if value == 'RANDOM_INSULT':
        insults = redis_client.lrange("insult_list", 0, -1)
        if insults:
            response = random.choice(insults)
            ch.basic_publish(exchange='', routing_key=properties.reply_to, body=response)
            print(f"Sent: {response}")
    elif value == 'BCAST_START':
        if args.broadcast:
            start_broadcast()
        else:
            print("Broadcasting not enabled for this instance")
    elif value == 'BCAST_STOP':
        if args.broadcast:
            stop_broadcast()
        else:
            print("Broadcasting not enabled for this instance")
    else:
        # Assume the value is a new insult to be stored
        if value not in redis_client.lrange("insult_list", 0, -1):
            redis_client.lpush("insult_list", value)
            print(f"Stored insult: {value}")
        else:
            print(f"Insult '{value}' already exists")

if __name__ == "__main__":
    args = parse_args()
    broadcast_process = None
    redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='request_queue')

    channel.basic_consume(queue='request_queue', on_message_callback=callback, auto_ack=True)

    print("InsultService started")
    if args.broadcast:
        print("Broadcasting enabled")
    channel.start_consuming()