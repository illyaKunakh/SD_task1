import random
import redis
import time

import sys
sys.path.append("..")  # Adds the parent directory to the module search path
import insults


# Connect to Redis
client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

channel_name = "news_channel"

while True:
    message = random.choice(insults.insults)
    client.publish(channel_name, message)
    print(f"Published: {message}")
    time.sleep(2)  # Simulating delay between messages

