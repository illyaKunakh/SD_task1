import redis
import threading
import time
import random

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

def broadcaster():
    while True:
        insults = r.smembers('insults')
        if insults:
            insult = random.choice(list(insults))
            r.publish('insults_channel', insult)
        time.sleep(5)

if __name__ == "__main__":
    # Iniciar broadcaster en un hilo
    threading.Thread(target=broadcaster, daemon=True).start()
    print("Redis InsultService broadcaster iniciado.")
    # Mantener el servicio corriendo
    while True:
        time.sleep(1)