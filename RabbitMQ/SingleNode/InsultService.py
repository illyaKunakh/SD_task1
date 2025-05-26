import pika
import multiprocessing
import random
import time

def storeInsult(insult):
    if insult not in insult_list:
        insult_list.append(insult)
        print(f"Save insult: {insult}")
    else:
        print(f"Insult ({insult}) is already in the list")

def getInsult(ch, method, properties, body):
    response = random.choice(insult_list)
    print(f"Send {response}")
    
    # Answer to the client
    ch.basic_publish(exchange='', routing_key=properties.reply_to, body=str(response),)

def notify(list):
    print(f"Notify subscribers...")
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    while True:
        insult = random.choice(list)
        channel.basic_publish(exchange='notify_subs', routing_key='', body=insult)
        print(f" [x] Send '{insult}'")
        time.sleep(5)

def startBroadcast():
    global broadcast_active
    if broadcast_active is None:
        broadcast_active = multiprocessing.Process(target=notify, args=(insult_list,))
        broadcast_active.start()
        print ("Broadcast Actived")
    else:
        print ("Broadcast is also Actived")

def stopBroadcast():
    global broadcast_active
    if broadcast_active is not None:
        broadcast_active.terminate()
        broadcast_active.join()
        broadcast_active = None
        print ("Broadcast Desactived")
    else:
        print ("Broadcast is also Desactived")

def getInsultList(ch, method, properties, body):
    print(f"Send {insult_list}")
    
    # Return response to client
    ch.basic_publish(
        exchange='',
        routing_key=properties.reply_to,
        body=str(insult_list),
    )

# Define the callback function
def callback(ch, method, properties, body):
    value = body.decode()
    print(f" [x] Received {value}")
    if value == 'Z':
        getInsult(ch, method, properties, body)
    elif value == 'O':
        startBroadcast()
    elif value == 'V':
        stopBroadcast()
    elif value == 'X':
        getInsultList(ch, method, properties, body)
    else:
        storeInsult(value)

if __name__ == "__main__":
    insult_list = []
    broadcast_active = None

    # Connect to RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declare a queue (ensure it exists)
    channel.queue_declare(queue='request_queue')

    # Declare a fanout exchange
    channel.exchange_declare(exchange='notify_subs', exchange_type='fanout')

    # Consume messages
    channel.basic_consume(queue='request_queue', on_message_callback=callback, auto_ack=True)

    print(' Waiting for messages')
    channel.start_consuming()