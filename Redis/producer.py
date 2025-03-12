import redis
import time

# Connect to Redis
client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

queue_name = "task_queue"

# Send multiple messages
tasks = ["Task 1", "Task 2", "Task 3"]

for task in tasks:
    client.rpush(queue_name, task)
    print(f"Produced: {task}")
    time.sleep(1)  # Simulating a delay in task production
