# File: MultipleNodeStatic.py
import os
import sys
import pika
import redis
import multiprocessing
import time
import matplotlib.pyplot as plt
import random
from functools import partial

# Añadir la carpeta raíz al path para poder importar RabbitMQ/ y Redis/ como paquetes
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

# Importar servicios y filtros desde los paquetes
from RabbitMQ.InsultService import receive as rmq_receive, broadcast as rmq_broadcast, list_insults as rmq_list
from RabbitMQ.InsultFilter  import receive as rmq_filter_receive, list_filtered as rmq_filter_list

from Redis.InsultService import receive as rds_receive, broadcast as rds_broadcast, list_insults as rds_list
from Redis.InsultFilter  import receive as rds_filter_receive, list_filtered as rds_filter_list

# Parámetros de la prueba
BATCH_SIZE = 50    # Mensajes por lote
TOTAL_REQUESTS = 5000

# Worker RabbitMQ InsultService
def rabbitmq_worker_insult(worker_id, queue_name, num_requests):
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost', heartbeat=0, blocked_connection_timeout=300
    ))
    channel = connection.channel()
    exchange_name = 'insults_exchange'
    # Exchange in-memory (no durable) for benchmark
    channel.exchange_declare(exchange=exchange_name, exchange_type='fanout', durable=False)
    result = channel.queue_declare(queue='', exclusive=True)
    temp_queue = result.method.queue
    channel.queue_bind(exchange=exchange_name, queue=temp_queue)
    # No publisher confirms to maximize throughput

    start_time = time.time()
    batch = []
    messages_sent = 0
    # Enviar mensajes de prueba en lotes
    for i in range(num_requests):
        batch.append(f"insult{i}")
        if len(batch) >= BATCH_SIZE or i == num_requests - 1:
            for msg in batch:
                channel.basic_publish(
                    exchange=exchange_name,
                    routing_key='',
                    body=msg.encode(),
                    properties=pika.BasicProperties(delivery_mode=1),  # transient
                    mandatory=False
                )
                messages_sent += 1
            batch = []

    connection.close()
    elapsed = time.time() - start_time
    return elapsed, messages_sent

# Worker Redis InsultService
def redis_worker_insult(worker_id, queue_name, num_requests):
    client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True,
                         socket_timeout=5, socket_connect_timeout=10, socket_keepalive=True, health_check_interval=30)
    start_time = time.time()
    messages_sent = 0
    worker_queue = f"{queue_name}_{worker_id}"
    pipeline = client.pipeline(transaction=False)

    for i in range(num_requests):
        pipeline.rpush(worker_queue, f"insult{i}")
        if (i + 1) % BATCH_SIZE == 0 or i == num_requests - 1:
            pipeline.execute()
            messages_sent += min(BATCH_SIZE, num_requests - i + (i % BATCH_SIZE))
            pipeline = client.pipeline(transaction=False)

    # Mover muestra al canal principal
    if worker_id == 0:
        sample = client.lrange(worker_queue, 0, min(10, num_requests)-1)
        for item in sample:
            client.rpush(queue_name, item)

    elapsed = time.time() - start_time
    return elapsed, messages_sent

# Funciones de setup/cleanup

def setup_rabbitmq():
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel()
    try:
        ch.exchange_delete(exchange='insults_exchange')
    except:
        pass
    # Exchange in-memory (no durable)
    ch.exchange_declare(exchange='insults_exchange', exchange_type='fanout', durable=False)
    for i in range(3):
        try:
            ch.queue_delete(queue=f'insults_queue_{i}')
        except:
            pass
        # Queues non-durable for benchmark
        ch.queue_declare(queue=f'insults_queue_{i}', durable=False)
        ch.queue_bind(exchange='insults_exchange', queue=f'insults_queue_{i}')
    conn.close()


def cleanup_rabbitmq():
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel()
    for i in range(3):
        try:
            ch.queue_delete(queue=f'insults_queue_{i}')
        except:
            pass
    try:
        ch.exchange_delete(exchange='insults_exchange')
    except:
        pass
    conn.close()


def setup_redis():
    client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    for key in client.scan_iter("task_queue*"):
        client.delete(key)
    try:
        client.config_set('timeout', '0')
    except:
        pass


def cleanup_redis():
    client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    for key in client.scan_iter("task_queue*"):
        client.delete(key)

# Función de ejecución de prueba

def run_test(num_nodes, worker_func, queue_name):
    num_requests = TOTAL_REQUESTS // num_nodes
    task = partial(worker_func, queue_name=queue_name, num_requests=num_requests)
    with multiprocessing.Pool(processes=num_nodes) as pool:
        results = pool.map(task, range(num_nodes))
    max_time = max(r[0] for r in results)
    total_msgs = sum(r[1] for r in results)
    print(f"Total messages: {total_msgs}")
    return max_time

# Bloque principal
if __name__ == "__main__":
    nodes = [1, 2, 3]
    rabbit_times = []
    redis_times = []

    setup_rabbitmq()
    setup_redis()
    try:
        print("Starting performance tests...")
        for n in nodes:
            print(f"Testing with {n} nodes...")
            t1 = run_test(n, rabbitmq_worker_insult, 'insults_input')
            rabbit_times.append(t1)
            print(f" RabbitMQ: {t1:.2f}s")
            t2 = run_test(n, redis_worker_insult, 'task_queue')
            redis_times.append(t2)
            print(f" Redis: {t2:.2f}s")

        # Velocidades relativas
        speed_rmq = [rabbit_times[0] / t for t in rabbit_times]
        speed_rds = [redis_times[0] / t for t in redis_times]
        plt.figure(figsize=(10, 6))
        plt.plot(nodes, speed_rmq, 'o-', label='RabbitMQ', linewidth=2)
        plt.plot(nodes, speed_rds, 's-', label='Redis', linewidth=2)
        plt.plot(nodes, nodes, '--', label='Ideal', alpha=0.7)
        plt.xlabel('Nodes')
        plt.ylabel('Speedup')
        plt.title('Static Scaling')
        plt.legend()
        plt.grid(alpha=0.3)
        plt.savefig('static_scaling.png', dpi=300, bbox_inches='tight')
        plt.show()
    finally:
        cleanup_rabbitmq()
        cleanup_redis()

