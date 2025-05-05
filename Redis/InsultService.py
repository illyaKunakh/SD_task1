import redis
import multiprocessing
import random
import time

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

def receive():
    while True:
        r.sadd('insults', r.blpop('insults_queue')[1])

def broadcast():
    while True:
        if ins := r.smembers('insults'):
            r.publish('insults_channel', random.choice(list(ins)))
        time.sleep(5)

def list_insults():
    ps = r.pubsub()
    ps.subscribe('insults_list')
    for msg in ps.listen():
        if msg['type'] == 'message':
            r.publish('insults_response', ';'.join(r.smembers('insults')) or 'empty')

if __name__ == "__main__":
    r.delete('insults', 'insults_queue')
    multiprocessing.Process(target=receive).start()
    multiprocessing.Process(target=broadcast).start()
    multiprocessing.Process(target=list_insults).start()