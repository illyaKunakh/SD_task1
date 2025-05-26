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

    def send_insult(self, requests, i):
        # Conectar a RabbitMQ
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()

        # Aseguramos la cola de filtros
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

    def run_test(self, request):
        print(f"Test → {request} peticiones")
        procs = []
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

    def do_tests(self):
        # Ejecuta cada escenario de carga
        for req in self.requests:
            self.run_test(req)

        # Limpiamos la cola para dejarla lista
        conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        ch = conn.channel()
        ch.queue_purge(queue='insult_filter_queue')
        conn.close()

if __name__ == '__main__':
    # 1) Arrancar el filtro en background
    filter_script = Path(__file__).parent.parent / ".." / 'RabbitMQ' / 'InsultFilter.py'
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
