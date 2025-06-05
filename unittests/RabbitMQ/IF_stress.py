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
        self.consumer_rate = []
        self.requests = [1000, 2000, 5000, 10000]

    # Sends insults to the InsultFilter service via RabbitMQ.
    # 1. Connects to RabbitMQ server.
    # 2. Declares a response queue for receiving replies.
    # 3. Publishes 'requests' number of messages to the 'insult_filter_queue'.
    # 4. Waits for all responses to be received.
    # 5. Closes the connection. 
    def send_insult(self, requests, i):
        # Conectar a RabbitMQ
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()

        channel.queue_declare(queue='insult_filter_queue')

        # Creamos cola temporal para respuestas
        response = channel.queue_declare(queue='', exclusive=True)
        response_queue = response.method.queue

        # Publicamos solicitudes
        for _ in range(requests):
            channel.basic_publish(
                exchange='',
                routing_key='insult_filter_queue',
                properties=pika.BasicProperties(reply_to=response_queue),
                body='A'
            )

        # Recibimos todas las respuestas
        count = 0
        while count < requests:
            method, header, body = channel.basic_get(queue=response_queue, auto_ack=True)
            if method:
                count += 1

        connection.close()

    # Runs test with specified number of producers and requests.
    def run_test(self, request):
        print(f"Test → {request} peticiones")
        procs = []
        # Start n producers that execute the send_insult function
        for i in range(self.number_process):
            p = multiprocessing.Process(target=self.send_insult, args=(request, i))
            procs.append(p)

        start = time.time()
        for p in procs: p.start()
        for p in procs: p.join()
        elapsed = time.time() - start

        rate = self.number_process * request / elapsed
        self.consumer_rate.append(rate)
        print(f"  Tiempo total: {elapsed:.2f}s — Throughput: {rate:.2f} msg/s")

    # Executes the producers 
    # 1. Runs each load scenario defined in self.requests.
    # 2. Cleans the RabbitMQ queue to leave it ready for the next test.
    # 3. Closes the connection.
    # 4. Displays the throughput graph.
    # 5. Terminates the InsultFilter service process.
    # 6. Waits for the process to finish.
    # 7. Cleans up the RabbitMQ connection.
    # 8. Plots the speedup graph.
    def do_tests(self):
        # Ejecuta cada escenario de carga
        for req in self.requests:
            self.run_test(req)

        # Limpiamos la cola para dejarla lista
        conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        ch = conn.channel()
        ch.queue_purge(queue='insult_filter_queue')
        conn.close()

# Executes the stress test for the InsultFilter service.
# 1. Starts the InsultFilter service in the background.
# 2. Runs the stress test.
# 3. Displays the throughput graph.
# 4. Terminates the InsultFilter service process.
# 5. Waits for the process to finish.
# 6. Cleans up the RabbitMQ connection.
if __name__ == '__main__':
    # 1) Arrancar el filtro en background
    filter_script = Path(__file__).parent.parent.parent / 'RabbitMQ' / 'InsultFilter.py'
    if not filter_script.exists():
        print(f"ERROR: no encontrado {filter_script}", file=sys.stderr)
        sys.exit(1)

    filter_proc = subprocess.Popen(
        [sys.executable, str(filter_script)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    time.sleep(2)  # esperamos a que el filtro esté listo

    # 2) Ejecutar el test
    test = StressTestService()
    test.do_tests()

    # 3) Mostrar gráfico
    total_requests = [r * test.number_process for r in test.requests]
    plt.figure(figsize=(8, 4))
    plt.plot(total_requests, test.consumer_rate, 'b-o', label='Messages/s')
    plt.xlabel('Total Requests')
    plt.ylabel('Throughput (msg/s)')
    plt.title('Stress Test RabbitMQ')
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.show()

    # 4) Detener el filtro
    filter_proc.terminate()
    filter_proc.wait()