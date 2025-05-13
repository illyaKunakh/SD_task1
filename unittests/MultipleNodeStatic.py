import os
import pika
import redis
import multiprocessing
import time
import matplotlib.pyplot as plt
import sys
import random
from functools import partial

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import insults
import text


BATCH_SIZE = 50  # Every batch has X messages
TOTAL_REQUESTS = 5000

def rabbitmq_worker_insult(worker_id, queue_name, num_requests):
    """Worker that sends insults in batches"""
    # Create connection for this worker
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost',
        heartbeat=0,  # Improves performance
        blocked_connection_timeout=300
    ))
    
    channel = connection.channel()
    
    #Declare an exchange for broadcasting messages
    exchange_name = 'insults_exchange'
    channel.exchange_declare(exchange=exchange_name, exchange_type='fanout', durable=True)
    
    # Declare a temporary queue for this worker
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    
    # Bind the queue to the exchange
    channel.queue_bind(exchange=exchange_name, queue=queue_name)
    
    # Enable publisher confirmation for better performance
    channel.confirm_delivery()
    
    start_time = time.time()
    
    batch = []
    messages_sent = 0
    
    # Use a local copy of insults to avoid constant access to the module
    local_insults = insults.insults.copy()
    random.shuffle(local_insults)  # Take random insults
    for i in range(num_requests):
        # Select an insult from the list
        insult_index = i % len(local_insults)
        message = local_insults[insult_index]
        
        # Add the message to the batch
        batch.append(message)
        
        # When the batch is full or we reach the last message, send it
        if len(batch) >= BATCH_SIZE or i == num_requests - 1:
            for msg in batch:
                try:
                    # Use mandatory=True to ensure the message is routed
                    channel.basic_publish(
                        exchange=exchange_name,
                        routing_key='',  # Not needed for fanout exchange
                        body=msg.encode(),
                        properties=pika.BasicProperties(
                            delivery_mode=2,  # Persistent message
                        ),
                        mandatory=True
                    )
                    messages_sent += 1
                except pika.exceptions.UnroutableError:
                    print(f"Worker {worker_id}: Message was returned undeliverable")
            
            # Clear the batch
            batch = []
    
    elapsed = time.time() - start_time
    
    # Assure all messages are sent
    connection.close()
    
    return elapsed, messages_sent

def redis_worker_insult(worker_id, queue_name, num_requests):
    """Worker that sends insults in batches to Redis"""
    # Create Redis client for this worker
    client = redis.Redis(
        host='localhost', 
        port=6379, 
        db=0, 
        decode_responses=True,
        socket_timeout=5,
        socket_connect_timeout=10,
        socket_keepalive=True,
        health_check_interval=30
    )
    
    start_time = time.time()
    messages_sent = 0
    
    # Use a local copy of insults to avoid constant access to the module
    local_insults = insults.insults.copy()
    random.shuffle(local_insults)  # Take random insults
    
    # Each worker has its own queue to avoid colision
    worker_queue = f"{queue_name}_{worker_id}"
    
    # Create a pipeline for batch processing
    pipeline = client.pipeline(transaction=False)
    
    for i in range(num_requests):
        insult_index = i % len(local_insults)
        message = local_insults[insult_index]
        pipeline.rpush(worker_queue, message)
        
        # Execute the pipeline every BATCH_SIZE messages or at the end
        if (i + 1) % BATCH_SIZE == 0 or i == num_requests - 1:
            pipeline.execute()
            messages_sent += min(BATCH_SIZE, num_requests - i + (i % BATCH_SIZE))
            pipeline = client.pipeline(transaction=False)
    
    # At the end, transfer the messages to the main queue if needed
    pipeline = client.pipeline(transaction=False)
    # Transfer only a sample to the main queue if needed for verification
    if worker_id == 0:
        sample_size = min(10, num_requests)
        sample = client.lrange(worker_queue, 0, sample_size - 1)
        for item in sample:
            pipeline.rpush(queue_name, item)
    
    pipeline.execute()
    
    elapsed = time.time() - start_time
    return elapsed, messages_sent

def setup_rabbitmq():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    
    # Clean up any existing queues and exchanges
    try:
        channel.exchange_delete(exchange='insults_exchange')
    except:
        pass
    
    # Declare an exchange for broadcasting messages
    exchange_name = 'insults_exchange'
    channel.exchange_declare(exchange=exchange_name, exchange_type='fanout', durable=True)
    
    # Declare temporary queues for each worker
    for i in range(3):  # Configure 3 queues for 3 workers
        try:
            channel.queue_delete(queue=f'insults_queue_{i}')
        except:
            pass
        result = channel.queue_declare(queue=f'insults_queue_{i}', durable=True)
        channel.queue_bind(exchange=exchange_name, queue=f'insults_queue_{i}')
    
    connection.close()

def cleanup_rabbitmq():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    
    # Remove the temporary queues
    for i in range(3):
        try:
            channel.queue_delete(queue=f'insults_queue_{i}')
        except:
            pass
    
    # Remove the exchange
    try:
        channel.exchange_delete(exchange='insults_exchange')
    except:
        pass
    
    connection.close()

def setup_redis():
    client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    
    # Clean up any existing keys
    keys_to_delete = []
    for key in client.scan_iter("task_queue*"):
        keys_to_delete.append(key)
    
    if keys_to_delete:
        client.delete(*keys_to_delete)
    
    # Configure Redis for testing
    try:
        client.config_set('timeout', '0')  # Deactivate timeout
    except:
        print("Could not set Redis timeout to 0, continuing anyway")

def cleanup_redis():
    client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    
    # Clean up any existing keys
    keys_to_delete = []
    for key in client.scan_iter("task_queue*"):
        keys_to_delete.append(key)
    
    if keys_to_delete:
        client.delete(*keys_to_delete)

def run_test(num_nodes, test_func, queue_name):
    """Ejecutar prueba con el número especificado de nodos"""
    num_requests = TOTAL_REQUESTS // num_nodes
    
    # For both RabbitMQ and Redis, we will use multiprocessing to simulate multiple nodes
    with multiprocessing.Pool(processes=num_nodes) as pool:
        # Use partial to pass additional arguments to the worker function
        worker_func = partial(test_func, queue_name=queue_name, num_requests=num_requests)
        # ID of the worker is passed automatically by the pool
        results = pool.map(worker_func, range(num_nodes))
    
    # Calculate the maximum time taken by any worker
    max_time = max(result[0] for result in results)
    total_messages = sum(result[1] for result in results)
    
    print(f"    Total messages processed: {total_messages}")
    return max_time

if __name__ == "__main__":
    nodes = [1, 2, 3]
    rabbitmq_insult_times = []
    redis_insult_times = []
    
    setup_rabbitmq()
    setup_redis()
    
    try:
        print("Starting performance tests...")
        for n in nodes:
            print(f"Testing with {n} nodes...")
            
            # Execute RabbitMQ test
            print(f"  - RabbitMQ test with {n} nodes...")
            t_rabbitmq = run_test(n, rabbitmq_worker_insult, 'insults_input')
            rabbitmq_insult_times.append(t_rabbitmq)
            print(f"    Completed in {t_rabbitmq:.2f} seconds")
            
            # Execute Redis test
            print(f"  - Redis test with {n} nodes...")
            t_redis = run_test(n, redis_worker_insult, 'task_queue')
            redis_insult_times.append(t_redis)
            print(f"    Completed in {t_redis:.2f} seconds")
        
        # Calculate speedup
        rabbitmq_speedups = [rabbitmq_insult_times[0] / t for t in rabbitmq_insult_times]
        redis_speedups = [redis_insult_times[0] / t for t in redis_insult_times]
        
        print("\nResults:")
        print("RabbitMQ Speedups:", [f"{s:.2f}" for s in rabbitmq_speedups])
        print("Redis Speedups:", [f"{s:.2f}" for s in redis_speedups])
        
        # Create a plot for the results
        plt.figure(figsize=(10, 6))
        plt.plot(nodes, rabbitmq_speedups, 'o-', label='RabbitMQ InsultService', linewidth=2)
        plt.plot(nodes, redis_speedups, 's-', label='Redis InsultService', linewidth=2)
        
        # Add ideal speedup line
        plt.plot(nodes, nodes, '--', label='Ideal Speedup', color='gray', alpha=0.7)
        
        plt.xlabel('Number of Nodes')
        plt.ylabel('Speedup (S = T1 / TN)')
        plt.title('Static Scaling Performance')
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.savefig('static_scaling.png', dpi=300, bbox_inches='tight')
        plt.show()
        
    finally:
        # Cleanup resources
        cleanup_rabbitmq()
        cleanup_redis()