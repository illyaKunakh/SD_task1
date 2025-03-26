# InsultFilter
import redis
import sys
sys.path.append("..")  # Adds the parent directory to the module search path
import insults


# Connect to Redis
client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)


queue_name = "task_queue"
processed_set = "processed_tasks"

print("Consumer is waiting for tasks...")

while True:
    task = client.blpop(queue_name, timeout=0)  # Blocks indefinitely until a task is available
    if task:
        if client.sismember(processed_set, task[1]):
            print("Duplicate ignored: " + task[1])
        else:
            if task[1] in insults.insults:
                client.sadd(processed_set, "CENSORED")
                print("Consumed: 'CENSORED'")
            else:
                client.sadd(processed_set, task[1])
                print("Consumed: " + task[1])

