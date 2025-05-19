import redis
import threading
import time

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

def worker():
    while True:
        _, text = r.brpop('texts_queue')
        insults = r.smembers('insults') or set()
        filtered = "CENSORED" if text.lower() in insults else text
        r.rpush('filtered_texts', filtered)

if __name__ == "__main__":
    # Iniciar worker en un hilo
    threading.Thread(target=worker, daemon=True).start()
    print("Redis InsultFilter worker iniciado.")
    # Mantener el servicio corriendo
    while True:
        time.sleep(1)