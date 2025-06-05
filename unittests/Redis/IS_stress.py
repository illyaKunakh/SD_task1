#!/usr/bin/env python3

import subprocess
import time
import sys
import json
import multiprocessing
import matplotlib.pyplot as plt
from pathlib import Path

import redis

class StressTestService:
    def __init__(self):
        self.number_process = 8
        self.requests = [1000, 2000, 5000, 10000]
        # Save throughput results for each test
        self.consumer_rate = []

    # This function is executed by each producer process.
    # 1) Connects to Redis.
    # 2) Pushes `requests_per_producer` ADD_INSULT messages (in JSON format) to the 'client_messages_service' list.
    # 3) Does not wait for explicit responses: the total count is measured from the launcher (main).
    # 4) Each message is a unique insult based on the process PID and an index.
    # 5) The InsultService.py service will BRPOP and LPUSH in parallel to process these messages.
    def send_insult(self, requests_per_producer, idx):
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        pid = multiprocessing.current_process().pid
        for i in range(requests_per_producer):
            payload = {"operation": "ADD_INSULT", "data": f"stress_insult_{pid}_{i}"}
            r.lpush("client_messages_service", json.dumps(payload))

    # 1. Runs a test with a specified number of producers and requests.
    # 2. Creates multiple processes that execute the send_insult function.
    # 3. Starts all processes and waits for them to finish.
    # 4. Calculates the total messages sent and the throughput.
    # 5. Appends the throughput to the consumer_rate list.
    # 6. Prints the total time taken and the throughput.
    # 7. Cleans up the 'client_messages_service' and 'redis_insult_list' keys before each test.
    def run_test(self, per_producer_count):
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        total_msgs = self.number_process * per_producer_count

        r.delete("client_messages_service")
        r.delete("redis_insult_list")

        service_script = Path(__file__).parents[2] / "Redis" / "InsultService.py"
        if not service_script.exists():
            print(f"ERROR: no he encontrado {service_script}", file=sys.stderr)
            sys.exit(1)

        service_proc = subprocess.Popen(
            [sys.executable, str(service_script)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        time.sleep(1)

        print(f"Test → {per_producer_count} peticiones por productor "
              f"(total {total_msgs})")

        producers = []
        start_time = time.time()
        for i in range(self.number_process):
            p = multiprocessing.Process(
                target=self.send_insult,
                args=(per_producer_count, i)
            )
            producers.append(p)
            p.start()

        for p in producers:
            p.join()

        while True:
            processed = r.llen("redis_insult_list")
            if processed >= total_msgs:
                break
            time.sleep(0.01)

        elapsed = time.time() - start_time
        rate = total_msgs / elapsed
        self.consumer_rate.append(rate)
        print(f"  Tiempo total: {elapsed:.2f}s — Throughput: {rate:.2f} msg/s")

        service_proc.terminate()
        service_proc.wait(timeout=5)

    def do_tests(self):
        for req in self.requests:
            self.run_test(req)

if __name__ == '__main__':
    test = StressTestService()
    test.do_tests()

    total_requests = [r * test.number_process for r in test.requests]
    plt.figure(figsize=(8, 4))
    plt.plot(total_requests, test.consumer_rate, 'b-o', label='Messages/s')
    plt.xlabel('Total Requests')
    plt.ylabel('Throughput (msg/s)')
    plt.title('Stress Test Redis InsultService')
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.show()
