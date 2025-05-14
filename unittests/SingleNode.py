import os
import sys
import time
import multiprocessing
import pika
import redis
import matplotlib.pyplot as plt

# Añadir la carpeta raíz al path para poder importar filtros
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

from RabbitMQ.InsultFilter import filter_text as rmq_filter_text
from Redis.InsultFilter import filter_text as rds_filter_text

# Worker RabbitMQ InsultService para test de throughput
def rabbitmq_insult_worker(processed):
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel()
    ch.queue_declare(queue='insults', durable=False)
    def callback(ch, method, props, body):
        with processed.get_lock():
            processed.value += 1
        ch.basic_ack(delivery_tag=method.delivery_tag)
    ch.basic_consume(queue='insults', on_message_callback=callback)
    ch.start_consuming()

# Worker RabbitMQ InsultFilter para test de throughput
def rabbitmq_filter_worker(processed):
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel()
    ch.queue_declare(queue='texts', durable=False)
    def callback(ch, method, props, body):
        # aplicar filtro importado
        _ = rmq_filter_text(body.decode())
        with processed.get_lock():
            processed.value += 1
        ch.basic_ack(delivery_tag=method.delivery_tag)
    ch.basic_consume(queue='texts', on_message_callback=callback)
    ch.start_consuming()

# Worker Redis InsultService para test
def redis_insult_worker(processed):
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    while True:
        _, insult = r.blpop('insults_queue')
        with processed.get_lock():
            processed.value += 1

# Worker Redis InsultFilter para test
def redis_filter_worker(processed):
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    while True:
        _, text = r.blpop('texts_queue')
        _ = rds_filter_text(text)
        with processed.get_lock():
            processed.value += 1

# Test RabbitMQ InsultService
def test_rabbitmq_insult():
    processed = multiprocessing.Value('i', 0)
    p = multiprocessing.Process(target=rabbitmq_insult_worker, args=(processed,))
    p.start()
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel()
    ch.queue_declare(queue='insults', durable=False)
    start = time.time()
    for i in range(1000):
        ch.basic_publish(exchange='', routing_key='insults', body=f"insult{i}".encode())
    while processed.value < 1000:
        time.sleep(0.01)
    elapsed = time.time() - start
    p.terminate()
    conn.close()
    return 1000 / elapsed

# Test RabbitMQ InsultFilter
def test_rabbitmq_filter():
    processed = multiprocessing.Value('i', 0)
    p = multiprocessing.Process(target=rabbitmq_filter_worker, args=(processed,))
    p.start()
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel()
    ch.queue_declare(queue='texts', durable=False)
    start = time.time()
    for i in range(1000):
        ch.basic_publish(exchange='', routing_key='texts', body=f"text{i}".encode())
    while processed.value < 1000:
        time.sleep(0.01)
    elapsed = time.time() - start
    p.terminate()
    conn.close()
    return 1000 / elapsed

# Test Redis InsultService
def test_redis_insult():
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    r.delete('insults_queue')
    processed = multiprocessing.Value('i', 0)
    p = multiprocessing.Process(target=redis_insult_worker, args=(processed,))
    p.start()
    start = time.time()
    for i in range(1000):
        r.rpush('insults_queue', f"insult{i}")
    while processed.value < 1000:
        time.sleep(0.01)
    elapsed = time.time() - start
    p.terminate()
    return 1000 / elapsed

# Test Redis InsultFilter
def test_redis_filter():
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    r.delete('texts_queue')
    processed = multiprocessing.Value('i', 0)
    p = multiprocessing.Process(target=redis_filter_worker, args=(processed,))
    p.start()
    start = time.time()
    for i in range(1000):
        r.rpush('texts_queue', f"text{i}")
    while processed.value < 1000:
        time.sleep(0.01)
    elapsed = time.time() - start
    p.terminate()
    return 1000 / elapsed

# Bloque principal
def main():
    print(f"RabbitMQ Insult: {test_rabbitmq_insult():.2f} req/s")
    print(f"RabbitMQ Filter: {test_rabbitmq_filter():.2f} req/s")
    print(f"Redis Insult: {test_redis_insult():.2f} req/s")
    print(f"Redis Filter: {test_redis_filter():.2f} req/s")

    rates = [
        test_rabbitmq_insult(),
        test_redis_insult(),
        test_rabbitmq_filter(),
        test_redis_filter()
    ]
    labels = ['RabbitMQ Insult', 'Redis Insult', 'RabbitMQ Filter', 'Redis Filter']
    plt.bar(labels, rates)
    plt.ylabel('Requests per second')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('single_node.png')

if __name__ == '__main__':
    main()
