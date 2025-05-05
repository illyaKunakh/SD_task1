import multiprocessing
import time
import pika
import redis
import matplotlib.pyplot as plt

# RabbitMQ InsultService worker
def rabbitmq_insult_worker(processed):
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel()
    ch.queue_declare(queue='insults')
    insults_dict = {}
    def callback(ch, method, props, body):
        insult = body.decode()
        if insult not in insults_dict:
            insults_dict[insult] = True
        with processed.get_lock():
            processed.value += 1
        ch.basic_ack(delivery_tag=method.delivery_tag)
    ch.basic_consume(queue='insults', on_message_callback=callback)
    ch.start_consuming()

# RabbitMQ InsultFilter worker
def rabbitmq_filter_worker(processed):
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel()
    ch.queue_declare(queue='texts')
    filtered_list = []
    def callback(ch, method, props, body):
        text = body.decode()
        filtered = "CENSORED" if text.lower() in ["insult1", "insult2"] else text
        filtered_list.append(filtered)
        with processed.get_lock():
            processed.value += 1
        ch.basic_ack(delivery_tag=method.delivery_tag)
    ch.basic_consume(queue='texts', on_message_callback=callback)
    ch.start_consuming()

# Redis InsultService worker
def redis_insult_worker():
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    while True:
        _, insult = r.blpop('insults_queue')
        r.sadd('insults_set', insult)
        r.incr('processed_insults')

# Redis InsultFilter worker
def redis_filter_worker():
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    while True:
        _, text = r.blpop('texts_queue')
        filtered = "CENSORED" if text.lower() in ["insult1", "insult2"] else text
        r.rpush('filtered_list', filtered)
        r.incr('processed_texts')

# Test functions
def test_rabbitmq_insult():
    processed = multiprocessing.Value('i', 0)
    p = multiprocessing.Process(target=rabbitmq_insult_worker, args=(processed,))
    p.start()
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel()
    ch.queue_declare(queue='insults')
    start_time = time.time()
    for i in range(1000):
        ch.basic_publish(exchange='', routing_key='insults', body=f"insult{i}".encode())
    while processed.value < 1000:
        time.sleep(0.1)
    elapsed = time.time() - start_time
    p.terminate()
    conn.close()
    return 1000 / elapsed

def test_rabbitmq_filter():
    processed = multiprocessing.Value('i', 0)
    p = multiprocessing.Process(target=rabbitmq_filter_worker, args=(processed,))
    p.start()
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel()
    ch.queue_declare(queue='texts')
    start_time = time.time()
    for i in range(1000):
        ch.basic_publish(exchange='', routing_key='texts', body=f"text{i}".encode())
    while processed.value < 1000:
        time.sleep(0.1)
    elapsed = time.time() - start_time
    p.terminate()
    conn.close()
    return 1000 / elapsed

def test_redis_insult():
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    r.delete('insults_queue', 'insults_set', 'processed_insults')
    r.set('processed_insults', 0)
    p = multiprocessing.Process(target=redis_insult_worker)
    p.start()
    start_time = time.time()
    for i in range(1000):
        r.rpush('insults_queue', f"insult{i}")
    while int(r.get('processed_insults') or 0) < 1000:
        time.sleep(0.1)
    elapsed = time.time() - start_time
    p.terminate()
    return 1000 / elapsed

def test_redis_filter():
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    r.delete('texts_queue', 'filtered_list', 'processed_texts')
    r.set('processed_texts', 0)
    p = multiprocessing.Process(target=redis_filter_worker)
    p.start()
    start_time = time.time()
    for i in range(1000):
        r.rpush('texts_queue', f"text{i}")
    while int(r.get('processed_texts') or 0) < 1000:
        time.sleep(0.1)
    elapsed = time.time() - start_time
    p.terminate()
    return 1000 / elapsed

if __name__ == "__main__":
    rabbitmq_insult_reqs = test_rabbitmq_insult()
    rabbitmq_filter_reqs = test_rabbitmq_filter()
    redis_insult_reqs = test_redis_insult()
    redis_filter_reqs = test_redis_filter()
    print(f"RabbitMQ Insult: {rabbitmq_insult_reqs:.2f} req/s")
    print(f"RabbitMQ Filter: {rabbitmq_filter_reqs:.2f} req/s")
    print(f"Redis Insult: {redis_insult_reqs:.2f} req/s")
    print(f"Redis Filter: {redis_filter_reqs:.2f} req/s")
    plt.bar(['RabbitMQ Insult', 'Redis Insult', 'RabbitMQ Filter', 'Redis Filter'],
            [rabbitmq_insult_reqs, redis_insult_reqs, rabbitmq_filter_reqs, redis_filter_reqs])
    plt.ylabel('Requests per second')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('single_node.png')