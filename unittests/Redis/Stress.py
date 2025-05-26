import redis
import time
import multiprocessing
import subprocess
import sys
from pathlib import Path
import json
import random
from itertools import cycle
import matplotlib.pyplot as plt

# Número máximo de procesos concurrentes (según CPU)
max_cpu = 8

# Peticiones a probar
number_petitions = [1000, 2000, 5000, 10000, 50000, 100000, 500000]

def spam__void_petitions(number_petitions):
    petition = {
        "operation": "Y",
        "data": ""
    }
    for _ in range(number_petitions):
        client.lpush(service_queue, json.dumps(petition))
        client.incr("number_pushes")

def run_tests(number_petitions, number_process):
    processes = []
    start = time.time()

    for _ in range(number_process):
        p = multiprocessing.Process(target=spam__void_petitions, args=(number_petitions,))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    while int(client.get("number_pushes")) < number_petitions * number_process:
        time.sleep(0.01)

    elapsed = time.time() - start
    rate = number_petitions * number_process / elapsed
    print(f"  {number_petitions*number_process} msgs en {elapsed:.2f}s → {rate:.2f} msg/s")
    return rate

if __name__ == "__main__":
    # Conexión a Redis
    client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    service_queue = "client_messages_service"
    client.delete("client_messages_service")      # limpiamos
    client.delete("number_pushes")

    # Arrancamos 1 instancia de InsultFilter.py
    filter_script = Path(__file__).parent.parent / ".." / "Redis" / "MultipleNode" / "InsultFilter.py"
    if not filter_script.exists():
        print(f"ERROR: no he encontrado {filter_script}", file=sys.stderr)
        sys.exit(1)

    proc = subprocess.Popen(
        [sys.executable, str(filter_script)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    time.sleep(2)  # esperamos que el filtro esté listo

    # Ejecutamos pruebas de estrés
    rates = []
    for req in number_petitions:
        client.set("number_pushes", 0)
        print(f"\n--- Stress test con {req} peticiones ---")
        rate = run_tests(req, max_cpu)
        rates.append(rate)

    # Gráfico de resultados
    plt.figure(figsize=(8, 4))
    plt.plot(number_petitions, rates, 'b-o', label='msg/s')
    plt.xlabel('Total de peticiones')
    plt.ylabel('Mensajes procesados por segundo')
    plt.title('Stress Test Redis')
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.show()

    # Cerramos el filtro
    proc.terminate()
    proc.wait()
