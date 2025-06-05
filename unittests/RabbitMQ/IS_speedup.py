#!/usr/bin/env python3
import subprocess
import time
import sys
import multiprocessing
from pathlib import Path

import pika
import redis
import matplotlib.pyplot as plt

class SpeedupTestRabbitMQService:
    def __init__(self):
        self.number_process = 4  # normalmente 8 o similar
        self.requests_per_producer = 10000
        # List where we will store the throughput for each test
        self.consumer_rate = []
        self.path_service = Path(__file__).parents[2] / 'RabbitMQ' / 'InsultService.py'

        # REdis client to prepare the insult list
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        self.queue_name = 'request_queue'
        self.exchange_type = ''  # we publis directly to the queue without an exchange

        # BEfore running the tests, we populate the insult list in Redis
        self._populate_insult_list()

    # This method populates the Redis list 'insult_list' with dummy insults.
    def _populate_insult_list(self):
        self.redis_client.delete('insult_list')
        for i in range(1000):
            self.redis_client.lpush('insult_list', f'dummy_insult_{i}')

    # This method sends n_requests messages to the InsultService via RabbitMQ.
    # 1. Connects to RabbitMQ server.
    # 2. Declares a response queue for receiving replies.
    # 3. Publishes 'n_requests' number of messages to the 'request_queue'.
    # 4. Waits for all responses to be received.
    # 5. Closes the connection.
    def _send_random_insults(self, n_requests: int):
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()

        # Assure the queue exists
        channel.queue_declare(queue=self.queue_name)

        # Create temporary queue for replies
        result = channel.queue_declare(queue='', exclusive=True)
        reply_queue = result.method.queue

        # Publish n_requests messages to the InsultService
        for _ in range(n_requests):
            channel.basic_publish(
                exchange=self.exchange_type,
                routing_key=self.queue_name,
                properties=pika.BasicProperties(reply_to=reply_queue),
                body='RANDOM_INSULT'
            )

        # Read the responses from the temporary queue
        count = 0
        while count < n_requests:
            method, header, body = channel.basic_get(queue=reply_queue, auto_ack=True)
            if method:
                count += 1

        connection.close()

    # 1. Runs a test with a specified number of InsultService instances.
    # 2. For each instance, it launches a subprocess that runs InsultService.
    # 3. Starts `self.number_process` producers that send insults to the service.
    # 4. Measures the total time taken and calculates the throughput.
    # 5. Terminates the InsultService instances after the test.
    # 6. Appends the throughput to self.consumer_rate for later analysis.
    # 7. Prints the results of the test.
    def run_test(self, num_servers: int):
        print(f"\n[Test RabbitMQ] → Iniciando {num_servers + 1} nodo(s) de InsultService...")
        procs_service = []
        # For each n servers, we launch an instance of InsultService
        for _ in range(num_servers + 1):
            p = subprocess.Popen(
                [sys.executable, str(self.path_service)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            procs_service.append(p)
            time.sleep(2)

        # Start producers that will send insults to the service
        producers = []
        start_time = time.time()
        for _ in range(self.number_process):
            p = multiprocessing.Process(
                target=self._send_random_insults,
                args=(self.requests_per_producer,)
            )
            producers.append(p)
            p.start()

        for p in producers:
            p.join()

        elapsed = time.time() - start_time
        total_messages = self.number_process * self.requests_per_producer
        throughput = total_messages / elapsed
        self.consumer_rate.append(throughput)
        print(f"  → Servidores: {num_servers + 1} | Tiempo total: {elapsed:.2f}s | "
              f"Throughput agregado: {throughput:.2f} msg/s")

        for p in procs_service:
            p.terminate()
            try:
                p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                p.kill()

    def do_speedup(self):
        """
        Ejecuta run_test() para 1, 2 y 3 instancias de servicio. Luego grafica speedup.
        """
        # For each 1, 2 and 3 servers, we run the test
        for servers in range(3):
            self.run_test(servers)

        nodes = [1, 2, 3]
        ideal = [1.0, 2.0, 3.0]
        real = [self.consumer_rate[i] / self.consumer_rate[0] for i in range(3)]

        plt.figure(figsize=(8, 4))
        plt.plot(nodes, ideal, 'r--', marker='o', label='Ideal (N)')
        plt.plot(nodes, real, 'b-o', label='Real')
        plt.xlabel('Número de instancias de InsultService')
        plt.ylabel('Speedup (throughput_relativo)')
        plt.title('SpeedUp RabbitMQ → InsultService')
        plt.grid(True)
        plt.legend()
        plt.tight_layout()
        plt.show()


if __name__ == "__main__":
    tester = SpeedupTestRabbitMQService()
    tester.do_speedup()
