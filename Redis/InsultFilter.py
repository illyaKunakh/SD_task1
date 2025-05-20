import redis
import multiprocessing

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

def filter_text(text):
    return "CENSORED" if text.lower() in ["insult1", "insult2"] else text

def receive():
    while True:
        r.rpush('filtered', filter_text(r.blpop('texts_queue')[1]))

def list_filtered():
    ps = r.pubsub()
    ps.subscribe('texts_list')
    for msg in ps.listen():
        if msg['type'] == 'message':
            r.publish('texts_response', ';'.join(r.lrange('filtered', 0, -1)) or 'empty')

if __name__ == "__main__":
    r.delete('filtered', 'texts_queue')
    multiprocessing.Process(target=receive).start()
    multiprocessing.Process(target=list_filtered).start()