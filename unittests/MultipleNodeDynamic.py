import pika
import multiprocessing
import matplotlib.pyplot as plt
import time
import random
from pathlib import Path
import subprocess

class DynamicWorkerManager:
    def __init__(self, min_workers=1, max_workers=10, duration=60):
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.duration = duration
        self.active_workers = []
        self.manager = multiprocessing.Manager()
        self.message_rates = self.manager.list()
        self.worker_counts = self.manager.list()
        self.interval = 1  # Interval to adjust workers (in seconds)
        self.worker_capacity = 200  # Maximum messages per second a worker can handle
        self.path_worker = Path(__file__).parent.parent / 'RabbitMQ' / 'InsultFilter.py'

    def start_worker(self):
        """Launches a real worker process from InsultFilter.py."""
        return subprocess.Popen(['python3', str(self.path_worker)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def adjust_workers(self, message_rate):
        """Adjusts the number of workers based on current message rate."""
        desired_workers = int(max(self.min_workers, min(self.max_workers, (message_rate + self.worker_capacity - 1) // self.worker_capacity)))
        current_workers = len(self.active_workers)

        if desired_workers > current_workers:
            for _ in range(desired_workers - current_workers):
                p = self.start_worker()
                self.active_workers.append(p)

        elif desired_workers < current_workers:
            for _ in range(current_workers - desired_workers):
                worker_to_stop = self.active_workers.pop()
                worker_to_stop.terminate()
                worker_to_stop.wait()

    def run_simulation(self):
        """Runs the simulation for dynamic scaling of workers."""
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        queue = 'dynamic_queue'
        channel.queue_declare(queue=queue)

        start_time = time.time()
        elapsed_time = 0

        while elapsed_time < self.duration:
            if elapsed_time < self.duration / 3:
                message_rate = random.randint(80, 120)  # Low load
            elif elapsed_time < 2 * self.duration / 3:
                message_rate = random.randint(400, 500)  # High load
            else:
                message_rate = random.randint(250, 300)  # Medium load

            total_messages = message_rate * self.interval
            for _ in range(total_messages):
                channel.basic_publish(exchange='', routing_key=queue, body='Message')

            self.message_rates.append(message_rate)
            self.adjust_workers(message_rate)
            self.worker_counts.append(len(self.active_workers))

            time.sleep(self.interval)
            elapsed_time = time.time() - start_time

        for worker in self.active_workers:
            worker.terminate()
            worker.wait()

        self.plot_results()

    def plot_results(self):
        """Plots the results of the simulation."""
        intervals = range(len(self.worker_counts))

        fig, ax1 = plt.subplots(figsize=(10, 5))

        ax1.set_xlabel('Time Interval (1 second each)')
        ax1.set_ylabel('Message Rate (msgs/s)', color='tab:blue')
        ax1.plot(intervals, self.message_rates, label='Message Rate', color='tab:blue')
        ax1.tick_params(axis='y', labelcolor='tab:blue')

        ax2 = ax1.twinx()
        ax2.set_ylabel('Number of Active Workers', color='tab:red')
        ax2.plot(intervals, self.worker_counts, label='Active Workers', linestyle='--', color='tab:red')
        ax2.tick_params(axis='y', labelcolor='tab:red')

        fig.tight_layout()
        plt.title('Dynamic Worker Scaling Based on Message Rate')
        plt.grid(True)
        plt.show()

if __name__ == '__main__':
    manager = DynamicWorkerManager()
    manager.run_simulation()