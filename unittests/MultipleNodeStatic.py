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

# Test functions for multiple nodes
def test_rabbitmq_insult_multiple(N):
    processed = multiprocessing.Value('i', 0)
    workers = [multiprocessing.Process(target=rabbitmq_insult_worker, args=(processed,)) for _ in range(N)]
    for w in workers:
        w.start()
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel()
    ch.queue_declare(queue='insults')
    start_time = time.time()
    for i in range(1000):
        ch.basic_publish(exchange='', routing_key='insults', body=f"insult{i}".encode())
    while processed.value < 1000:
        time.sleep(0.1)
    elapsed = time.time() - start_time
    for w in workers:
        w.terminate()
    conn.close()
    return elapsed

def test_rabbitmq_filter_multiple(N):
    processed = multiprocessing.Value('i', 0)
    workers = [multiprocessing.Process(target=rabbitmq_filter_worker, args=(processed,)) for _ in range(N)]
    for w in workers:
        w.start()
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel()
    ch.queue_declare(queue='texts')
    start_time = time.time()
    for i in range(1000):
        ch.basic_publish(exchange='', routing_key='texts', body=f"text{i}".encode())
    while processed.value < 1000:
        time.sleep(0.1)
    elapsed = time.time() - start_time
    for w in workers:
        w.terminate()
    conn.close()
    return elapsed

def test_redis_insult_multiple(N):
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    r.delete('insults_queue', 'insults_set', 'processed_insults')
    r.set('processed_insults', 0)
    workers = [multiprocessing.Process(target=redis_insult_worker) for _ in range(N)]
    for w in workers:
        w.start()
    start_time = time.time()
    for i in range(1000):
        r.rpush('insults_queue', f"insult{i}")
    while int(r.get('processed_insults') or 0) < 1000:
        time.sleep(0.1)
    elapsed = time.time() - start_time
    for w in workers:
        w.terminate()
    return elapsed

def test_redis_filter_multiple(N):
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    r.delete('texts_queue', 'filtered_list', 'processed_texts')
    r.set('processed_texts', 0)
    workers = [multiprocessing.Process(target=redis_filter_worker) for _ in range(N)]
    for w in workers:
        w.start()
    start_time = time.time()
    for i in range(1000):
        r.rpush('texts_queue', f"text{i}")
    while int(r.get('processed_texts') or 0) < 1000:
        time.sleep(0.1)
    elapsed = time.time() - start_time
    for w in workers:
        w.terminate()
    return elapsed

if __name__ == "__main__":
    N_values = [1, 2, 3]
    rabbitmq_insult_times = [test_rabbitmq_insult_multiple(N) for N in N_values]
    rabbitmq_filter_times = [test_rabbitmq_filter_multiple(N) for N in N_values]
    redis_insult_times = [test_redis_insult_multiple(N) for N in N_values]
    redis_filter_times = [test_redis_filter_multiple(N) for N in N_values]
    
    rabbitmq_insult_speedups = [rabbitmq_insult_times[0] / t for t in rabbitmq_insult_times]
    rabbitmq_filter_speedups = [rabbitmq_filter_times[0] / t for t in rabbitmq_filter_times]
    redis_insult_speedups = [redis_insult_times[0] / t for t in redis_insult_times]
    redis_filter_speedups = [redis_filter_times[0] / t for t in redis_filter_times]
    
    print("RabbitMQ Insult Speedups:", rabbitmq_insult_speedups)
    print("RabbitMQ Filter Speedups:", rabbitmq_filter_speedups)
    print("Redis Insult Speedups:", redis_insult_speedups)
    print("Redis Filter Speedups:", redis_filter_speedups)
    
    plt.plot(N_values, rabbitmq_insult_speedups, label='RabbitMQ Insult')
    plt.plot(N_values, rabbitmq_filter_speedups, label='RabbitMQ Filter')
    plt.plot(N_values, redis_insult_speedups, label='Redis Insult')
    plt.plot(N_values, redis_filter_speedups, label='Redis Filter')
    plt.xlabel('Number of nodes')
    plt.ylabel('Speedup')
    plt.legend()
    plt.savefig('multiple_nodes.png')