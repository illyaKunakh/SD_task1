import os
import sys
import pika
import redis
import multiprocessing
import time
import matplotlib.pyplot as plt
from functools import partial

# Add root directory to path
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

# Import services and filters
from RabbitMQ.InsultService import receive as rmq_receive, broadcast as rmq_broadcast, list_insults as rmq_list
from RabbitMQ.InsultFilter  import receive as rmq_filter_receive, list_filtered as rmq_filter_list
from Redis.InsultService import receive as rds_receive, broadcast as rds_broadcast, list_insults as rds_list
from Redis.InsultFilter  import receive as rds_filter_receive, list_filtered as rds_filter_list

BATCH_SIZE = 50    # Messages / batch
TOTAL_REQUESTS = 5000

# Worker RabbitMQ InsultService
def rabbitmq_worker_insult(worker_id, queue_name, num_requests):
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost', heartbeat=0, blocked_connection_timeout=300
    ))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=False)

    start_time = time.time()
    batch = []
    messages_sent = 0
    for i in range(num_requests):
        batch.append(f"insult{i}")
        if len(batch) >= BATCH_SIZE or i == num_requests - 1:
            for msg in batch:
                channel.basic_publish(
                    exchange='',
                    routing_key=queue_name,
                    body=msg.encode(),
                    properties=pika.BasicProperties(delivery_mode=2),
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
    pipeline = client.pipeline(transaction=False)

    for i in range(num_requests):
        pipeline.rpush(queue_name, f"insult{i}")
        if (i + 1) % BATCH_SIZE == 0 or i == num_requests - 1:
            pipeline.execute()
            messages_sent += min(BATCH_SIZE, num_requests - i + (i % BATCH_SIZE))
            pipeline = client.pipeline(transaction=False)

    elapsed = time.time() - start_time
    return elapsed, messages_sent

# Setup and cleanup functions
def setup_rabbitmq():
    global rmq_receive_process, rmq_broadcast_process, rmq_list_process
    rmq_receive_process = multiprocessing.Process(target=rmq_receive)
    rmq_broadcast_process = multiprocessing.Process(target=rmq_broadcast)
    rmq_list_process = multiprocessing.Process(target=rmq_list)
    rmq_receive_process.start()
    rmq_broadcast_process.start()
    rmq_list_process.start()
    time.sleep(1)  # Allow time for processes to start

def cleanup_rabbitmq():
    rmq_receive_process.terminate()
    rmq_broadcast_process.terminate()
    rmq_list_process.terminate()

def setup_redis():
    global rds_receive_process, rds_broadcast_process, rds_list_process
    rds_receive_process = multiprocessing.Process(target=rds_receive)
    rds_broadcast_process = multiprocessing.Process(target=rds_broadcast)
    rds_list_process = multiprocessing.Process(target=rds_list)
    rds_receive_process.start()
    rds_broadcast_process.start()
    rds_list_process.start()
    time.sleep(1)  # Allow time for processes to start

def cleanup_redis():
    rds_receive_process.terminate()
    rds_broadcast_process.terminate()
    rds_list_process.terminate()

# Test execution function
def run_test(num_nodes, worker_func, queue_name):
    num_requests = TOTAL_REQUESTS // num_nodes
    task = partial(worker_func, queue_name=queue_name, num_requests=num_requests)
    with multiprocessing.Pool(processes=num_nodes) as pool:
        results = pool.map(task, range(num_nodes))
    max_time = max(r[0] for r in results)
    total_msgs = sum(r[1] for r in results)
    print(f"Total messages: {total_msgs}")
    return max_time

# Main block
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
            t1 = run_test(n, rabbitmq_worker_insult, 'insults')
            rabbit_times.append(t1)
            print(f" RabbitMQ: {t1:.2f}s")
            t2 = run_test(n, redis_worker_insult, 'insults_queue')
            redis_times.append(t2)
            print(f" Redis: {t2:.2f}s")

        # Relative speeds
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