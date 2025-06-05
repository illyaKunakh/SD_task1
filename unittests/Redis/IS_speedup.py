#!/usr/bin/env python3

import subprocess
import time
import sys
import json
import multiprocessing
from pathlib import Path

import redis
import matplotlib.pyplot as plt

# Every producer will push n requests to the Redis queue
def producer_push(n_requests: int, queue_name: str):
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    pid = multiprocessing.current_process().pid
    for i in range(n_requests):
        payload = {"operation": "ADD_INSULT", "data": f"speedup_insult_{pid}_{i}"}
        r.lpush(queue_name, json.dumps(payload))


def main():
    n_producers = 4    
    requests_per_producer = 10000                 
    total_requests = n_producers * requests_per_producer

    # Redis keys used by InsultService.py
    queue_name = "client_messages_service"
    insult_list_key = "redis_insult_list"

    svc_path = Path(__file__).parents[2] / "Redis" / "InsultService.py"
    if not svc_path.exists():
        print(f"ERROR: no he encontrado {svc_path}", file=sys.stderr)
        sys.exit(1)

    client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

    # Save the elapsed times for each number of services
    elapsed_times = {}

    for num_services in [1, 2, 3]:
        print(f"\n[Test Redis] → Iniciando {num_services} instancia(s) de InsultService…")

        # 1) Clean Redis keys before each test
        client.delete(queue_name)
        client.delete(insult_list_key)

        # 2) Start num_services instances of InsultService.py in parallel
        procs_service = []
        for _ in range(num_services):
            p = subprocess.Popen(
                [sys.executable, str(svc_path)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            procs_service.append(p)
            # Breve pausa para que cada instancia comience a hacer BRPOP
            time.sleep(0.5)

        # 3) Launch n producers, each sending requests_per_producer requests
        print(f"  → Lanzando {n_producers} productores, cada uno enviando {requests_per_producer} peticiones…")
        start_time = time.time()
        producers = []
        for _ in range(n_producers):
            p = multiprocessing.Process(
                target=producer_push,
                args=(requests_per_producer, queue_name)
            )
            producers.append(p)
            p.start()

        for p in producers:
            p.join()

        # 4) Wait until all messages are processed:
        #    each InsultService does BRPOP + LPUSH on insult_list_key.
        print("  → Esperando a que las instancias procesen todos los mensajes…")
        while True:
            count = client.llen(insult_list_key)
            if count >= total_requests:
                break
            time.sleep(0.01)

        elapsed = time.time() - start_time
        elapsed_times[num_services] = elapsed
        throughput = total_requests / elapsed
        print(f"  → Instancias: {num_services} | Tiempo total: {elapsed:.2f}s | Throughput: {throughput:.2f} msg/s")

        for p in procs_service:
            p.terminate()
            try:
                p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                p.kill()

    nodes = [1, 2, 3]
    ideal = [1.0, 2.0, 3.0]
    real = [ elapsed_times[1] / elapsed_times[n] for n in nodes ]

    plt.figure(figsize=(8, 4))
    plt.plot(nodes, ideal, 'r--', marker='o', label='Ideal (N)')
    plt.plot(nodes, real, 'b-o', label='Real')
    plt.xlabel('Número de instancias de InsultService')
    plt.ylabel('Speedup (elapsed₁ / elapsedₙ)')
    plt.title('SpeedUp Redis → InsultService')
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()
