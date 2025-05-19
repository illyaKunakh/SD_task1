import os
import sys
import time
import multiprocessing
import pika
import redis
import matplotlib.pyplot as plt

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

from RabbitMQ.InsultService import receive as rmq_insult_receive, list_insults as rmq_list_insults
from RabbitMQ.InsultFilter import receive as rmq_filter_receive, list_filtered as rmq_list_filtered
from Redis.InsultService import receive as rds_insult_receive, list_insults as rds_list_insults
from Redis.InsultFilter import receive as rds_filter_receive, list_filtered as rds_list_filtered

def setup_rabbitmq():
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel()
    for queue in ['insults', 'texts', 'insults_list', 'texts_list']:
        try:
            ch.queue_delete(queue=queue)
        except:
            pass
    conn.close()

def setup_redis():
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    for key in ['insults_queue', 'texts_queue', 'insults', 'filtered']:
        r.delete(key)

def test_rabbitmq(queue, receive_func, list_func, list_queue):
    processed = multiprocessing.Value('i', 0)
    p_receive = multiprocessing.Process(target=receive_func)
    p_list = multiprocessing.Process(target=list_func)
    p_receive.start()
    p_list.start()
    time.sleep(0.5)

    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel()
    ch.queue_declare(queue=queue, durable=False)
    ch.queue_declare(queue=list_queue, durable=False)
    reply_queue = ch.queue_declare(queue='', exclusive=True).method.queue

    start = time.time()
    for i in range(1000):
        ch.basic_publish(exchange='', routing_key=queue, body=f"{queue[:-1]}{i}".encode())
    time.sleep(0.3)
    ch.basic_publish(exchange='', routing_key=list_queue, body='list', properties=pika.BasicProperties(reply_to=reply_queue))

    def callback(ch, method, props, body):
        with processed.get_lock():
            processed.value = len(body.decode().split(';')) if body.decode() != 'empty' else 0
        ch.basic_ack(delivery_tag=method.delivery_tag)

    ch.basic_consume(queue=reply_queue, on_message_callback=callback)
    timeout = time.time() + 5
    while processed.value < 1000 and time.time() < timeout:
        conn.process_data_events(time_limit=0.01)
    
    elapsed = time.time() - start
    p_receive.terminate()
    p_list.terminate()
    conn.close()
    return 1000 / elapsed if elapsed > 0 else 0

def test_redis(queue, receive_func, list_func, list_channel, response_channel):
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    r.delete(queue, queue[:-6])
    processed = multiprocessing.Value('i', 0)
    p_receive = multiprocessing.Process(target=receive_func)
    p_list = multiprocessing.Process(target=list_func)
    p_receive.start()
    p_list.start()
    time.sleep(0.5)

    start = time.time()
    for i in range(1000):
        r.rpush(queue, f"{queue[:-6]}{i}")
    time.sleep(0.3)
    r.publish(list_channel, 'list')

    ps = r.pubsub()
    ps.subscribe(response_channel)
    timeout = time.time() + 5
    while processed.value < 1000 and time.time() < timeout:
        message = ps.get_message(timeout=0.01)
        if message and message['type'] == 'message':
            with processed.get_lock():
                processed.value = len(message['data'].split(';')) if message['data'] != 'empty' else 0
        time.sleep(0.01)
    
    elapsed = time.time() - start
    p_receive.terminate()
    p_list.terminate()
    return 1000 / elapsed if elapsed > 0 else 0

def main():
    setup_rabbitmq()
    setup_redis()
    results = [
        test_rabbitmq('insults', rmq_insult_receive, rmq_list_insults, 'insults_list'),
        test_rabbitmq('texts', rmq_filter_receive, rmq_list_filtered, 'texts_list'),
        test_redis('insults_queue', rds_insult_receive, rds_list_insults, 'insults_list', 'insults_response'),
        test_redis('texts_queue', rds_filter_receive, rds_list_filtered, 'texts_list', 'texts_response')
    ]
    labels = ['RabbitMQ Insult', 'RabbitMQ Filter', 'Redis Insult', 'Redis Filter']
    for label, reqs in zip(labels, results):
        print(f"{label}: {reqs:.2f} req/s")
    
    plt.bar(labels, results)
    plt.ylabel('Requests per second')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('single_node.png')
    plt.show()

if __name__ == '__main__':
    main()