import pika
import threading
import time
import random

insults = set()

def receive_insults():
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel()
    ch.queue_declare(queue='insults', durable=True)
    def callback(ch, method, props, body):
        insult = body.decode()
        if insult not in insults:
            insults.add(insult)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    ch.basic_consume(queue='insults', on_message_callback=callback)
    ch.start_consuming()

def broadcaster():
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel()
    ch.exchange_declare(exchange='insults_out', exchange_type='fanout')
    while True:
        if insults:
            insult = random.choice(list(insults))
            ch.basic_publish(exchange='insults_out', routing_key='', body=insult.encode())
        time.sleep(5)
    conn.close()

if __name__ == "__main__":
    threading.Thread(target=receive_insults, daemon=True).start()
    threading.Thread(target=broadcaster, daemon=True).start()
    print("RabbitMQ InsultService iniciado.")
    while True:
        time.sleep(1)