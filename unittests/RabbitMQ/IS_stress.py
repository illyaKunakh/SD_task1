#!/usr/bin/env python3

import pika
import multiprocessing
import time
import matplotlib.pyplot as plt
import subprocess
import sys
from pathlib import Path

class StressTestService:
    def __init__(self):
        self.number_process = 8
        self.requests = [1000, 2000, 5000, 10000]
        self.consumer_rate = []
    
    # 1. Sends insults to the InsultService via RabbitMQ.
    # 2. Connects to RabbitMQ server.
    # 3. Declares a response queue for receiving replies.
    # 4. Publishes 'requests_per_producer' number of messages to the 'request_queue'.
    # 5. Waits for all responses to be received.
    # 6. Closes the connection.
    def send_insult(self, requests_per_producer, idx):
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()

        # Ensure the request queue exists
        queue = "request_queue"
        channel.queue_declare(queue=queue)

        # Create a temporary queue for replies
        result = channel.queue_declare(queue='', exclusive=True)
        reply_queue = result.method.queue

        # Publish requests to the InsultService
        for _ in range(requests_per_producer):
            channel.basic_publish(
                exchange='',
                routing_key=queue,
                properties=pika.BasicProperties(reply_to=reply_queue),
                body='RANDOM_INSULT'
            )

        # Read the responses from the temporary queue
        count = 0
        while count < requests_per_producer:
            method, header, body = channel.basic_get(queue=reply_queue, auto_ack=True)
            if method:
                count += 1

        connection.close()

    # 1. Runs a test with a specified number of producers and requests.
    # 2. Creates multiple processes that execute the send_insult function.
    # 3. Starts all processes and waits for them to finish.
    # 4. Calculates the total messages sent and the throughput.
    # 5. Appends the throughput to the consumer_rate list.
    # 6. Prints the total time taken and the throughput.
    # 7. Cleans up the 'request_queue' after the tests.
    def run_test(self, per_producer_count):
        print(f"Test → {per_producer_count} peticiones por productor "
              f"(total {per_producer_count * self.number_process})")
        procs = []
        for i in range(self.number_process):
            p = multiprocessing.Process(
                target=self.send_insult,
                args=(per_producer_count, i)
            )
            procs.append(p)

        start = time.time()
        for p in procs:
            p.start()
        for p in procs:
            p.join()
        elapsed = time.time() - start

        total_msgs = self.number_process * per_producer_count
        rate = total_msgs / elapsed
        self.consumer_rate.append(rate)
        print(f"  Tiempo total: {elapsed:.2f}s — Throughput: {rate:.2f} msg/s")

    # 1. Executes the producers for each load scenario defined in self.requests.
    # 2. Cleans the RabbitMQ queue to leave it ready for the next test.
    # 3. Closes the connection to RabbitMQ.
    def do_tests(self):
        # Executes each load scenario defined in self.requests
        for req in self.requests:
            self.run_test(req)

        # Clean up the RabbitMQ queue
        conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        ch = conn.channel()
        ch.queue_purge(queue='request_queue')
        conn.close()

if __name__ == '__main__':
    # 1) Start the InsultService in the background
    service_script = Path(__file__).parent.parent.parent / 'RabbitMQ' / 'InsultService.py'
    if not service_script.exists():
        print(f"ERROR: no encontrado {service_script}", file=sys.stderr)
        sys.exit(1)

    service_proc = subprocess.Popen(
        [sys.executable, str(service_script)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    time.sleep(2)

    # 2) Execute the stress test
    test = StressTestService()
    test.do_tests()

    total_requests = [r * test.number_process for r in test.requests]
    plt.figure(figsize=(8, 4))
    plt.plot(total_requests, test.consumer_rate, 'b-o', label='Messages/s')
    plt.xlabel('Total Requests')
    plt.ylabel('Throughput (msg/s)')
    plt.title('Stress Test RabbitMQ InsultService')
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.show()

    service_proc.terminate()
    service_proc.wait()
