import redis
import time
import multiprocessing
import subprocess
import sys
from pathlib import Path
import json
import random
from itertools import cycle

# Chose depending on cpu cores number
n_producers = 4

# List of insults to be used in the tests
# 1. This list can be extended with more insults.
# 2. The insults are used to populate the Redis list 'redis_insult_list'.
def initialize_insults():
    client.ltrim("redis_insult_list", 1, 0)
    for insult in insult_list:
        petition = {
            "operation": "SUBMIT_TEXT",
            "data": insult
        }
        client.lpush(service_queues[0], json.dumps(petition))

# Creates a list of service queues based on the number of nodes.
def petition_queues(nodes):
    global service_queues
    service_queues = []
    for i in range(nodes):
        service_queues.append("client_messages_service" + str(i + 1))
    print("Service queues:", service_queues)

# Spam of void petitions to the service queues.
def spam__void_petitions(number_petitions):
    service_rr = cycle(service_queues)
    petition = {
        "operation": "PROCESS_ONE",
        "data": ""
    }
    for _ in range(number_petitions):
        client.lpush(next(service_rr), json.dumps(petition))
        client.incr("number_pushes")

# 1. Start the spam of void petitions to the service queues.
# 2. Create the service queues based on the number of nodes.
# 3. Initialize the insults in Redis.
# 4. Run the tests with the specified number of petitions and processes.
# 5. Collect the elapsed time for each test.
# 6. Plot the speedup graph.
def run_tests(number_petitions, number_process):
    processes = []
    start = time.time()

    for _ in range(number_process):
        p = multiprocessing.Process(target=spam__void_petitions, args=(number_petitions,))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    # Wait until all petitions are processed
    while int(client.get("number_pushes")) < number_petitions * n_producers:
        time.sleep(0.01)

    elapsed = time.time() - start
    rate = (n_producers * number_petitions) / elapsed
    print(f"  Time: {elapsed:.2f}s — Rate: {rate:.2f} msg/s")
    return elapsed


if __name__ == "__main__":
    client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    client.set("insult_service_instance_id", 0)

    filter_script = Path(__file__).parent.parent.parent / "Redis" / "InsultFilter.py"
    if not filter_script.exists():
        print(f"ERROR: no he encontrado {filter_script}", file=sys.stderr)
        sys.exit(1)

    insult_list = ['insult1', 'insult2', 'insult3', 'insult4', 'insult5', 'insult6', 'insult7', 'insult8', 'insult9']
    number_petitions = 10000
    nodes = 3

    # Clean previous data in Redis
    for i in range(nodes):
        client.delete(f"client_messages_service{i + 1}")

    results = {}

    # For 1, 2 and 3 nodes of IF
    for service in range(nodes):
        procs = []
        for _ in range(service + 1):
            p = subprocess.Popen(
                [sys.executable, str(filter_script)],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
            procs.append(p)
            time.sleep(1)  # Dejamos que arranque

        # Set the service queues and initialize insults
        petition_queues(service + 1)
        initialize_insults()
        client.set("number_pushes", 0)
        print(f"\n=== Test con {service + 1} nodo(s) y {number_petitions} peticiones ===")
        elapsed = run_tests(number_petitions, n_producers)
        results[service] = elapsed

        for p in procs:
            p.terminate()
            p.wait()

    # Cálculo de speedup y gráfico
    import matplotlib.pyplot as plt

    speedup = [1] + [results[0] / results[i] for i in range(1, nodes)]
    workers = [1, 2, 3]

    plt.figure(figsize=(8, 4))
    plt.plot(workers, workers, 'r--', label='Ideal')
    plt.plot(workers, speedup, 'b-o', label='Medido')
    plt.xlabel('(InsultFilter) Nodes')
    plt.ylabel('Speedup')
    plt.title('SpeedUp Redis')
    plt.legend()
    plt.grid(True)
    plt.show()