import pika
import multiprocessing
import random
import time

insults = set()

def receive():
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel()
    ch.queue_declare(queue='insults')
    ch.basic_consume(queue='insults', on_message_callback=lambda ch, method, props, body: [insults.add(body.decode()), ch.basic_ack(method.delivery_tag)], auto_ack=False)
    ch.start_consuming()

def broadcast():
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel()
    ch.exchange_declare(exchange='insults_out', exchange_type='fanout')
    while True:
        if insults:
            ch.basic_publish(exchange='insults_out', routing_key='', body=random.choice(list(insults)).encode())
        time.sleep(5)

def list_insults():
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel()
    ch.queue_declare(queue='insults_list')
    ch.basic_consume(queue='insults_list', on_message_callback=lambda ch, method, props, body: [ch.basic_publish('', props.reply_to, ';'.join(insults).encode()), ch.basic_ack(method.delivery_tag)], auto_ack=False)
    ch.start_consuming()

if __name__ == "__main__":
    multiprocessing.Process(target=receive).start()
    multiprocessing.Process(target=broadcast).start()
    multiprocessing.Process(target=list_insults).start()