import random
import redis
import time

import sys
sys.path.append("..")  # Adds the parent directory to the module search path
import insults


# Connect to Redis
client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)


queue_name = "task_queue"


# Send multiple messages
while True:
    task = random.choice(insults.insults)
    client.rpush(queue_name, task)
    print(f"Produced: {task}")
    time.sleep(3)  # Simulating a delay in task production

