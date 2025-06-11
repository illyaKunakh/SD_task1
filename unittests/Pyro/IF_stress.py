import Pyro4
import time
import multiprocessing
import subprocess
import sys
from pathlib import Path
import matplotlib.pyplot as plt
import random

class StressTestService:
    def __init__(self):
        self.number_process = 8
        self.requests = [1000, 2000, 5000, 10000]
        self.consumer_rate = []
        self.service_uri = "PYRO:insult.filter@localhost:9091"
    
    # Sends insults to the InsultFilter service via Pyro4.
    # 1. Connects to Pyro4 service using the service URI.
    # 2. Sends 'requests_per_producer' number of filter requests to the service.
    # 3. Each request calls the submit_text method with a test message containing insults.
    # 4. Also queries service status to simulate mixed workload.
    def send_insult(self, requests_per_producer, idx):
        try:
            # Connect to Pyro4 service
            filter_service = Pyro4.Proxy(self.service_uri)
            
            # Mix of submit_text and status queries
            submit_count = requests_per_producer // 2
            query_count = requests_per_producer - submit_count
            
            # Submit texts for filtering
            for i in range(submit_count):
                # Create test text with insults
                insult_num = (i % 9) + 1
                text = f"Producer {idx} message {i} with insult{insult_num} content"
                result = filter_service.submit_text(text)
            
            # Query service status
            for i in range(query_count):
                if i % 3 == 0:
                    result = filter_service.get_queue_size()
                elif i % 3 == 1:
                    result = filter_service.get_results_count()
                else:
                    result = filter_service.get_results()
                    
        except Exception as e:
            print(f"Error in producer {idx}: {e}")

    # Runs a test with a specified number of producers and requests.
    # 1. Creates multiple processes that execute the send_insult function.
    # 2. Starts all processes and waits for them to finish.
    # 3. Calculates the total messages sent and the throughput.
    # 4. Appends the throughput to the consumer_rate list.
    # 5. Prints the total time taken and the throughput.
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

    # Executes the producers for each load scenario defined in self.requests.
    # 1. Runs each test scenario with different request loads.
    # 2. Clears service results between tests to ensure clean state.
    def do_tests(self):
        for req in self.requests:
            # Clear previous results before each test
            try:
                filter_service = Pyro4.Proxy(self.service_uri)
                filter_service.clear_results()
                time.sleep(0.5)  # Brief pause between tests
            except Exception as e:
                print(f"Warning: Could not clear results: {e}")
            
            self.run_test(req)

if __name__ == '__main__':
    # 1) Start the Pyro4 InsultFilter service in the background
    filter_script = Path(__file__).parent.parent.parent / 'Pyro' / 'InsultFilter.py'
    if not filter_script.exists():
        print(f"ERROR: no encontrado {filter_script}", file=sys.stderr)
        sys.exit(1)

    # Start the filter service with specific port
    filter_proc = subprocess.Popen(
        [sys.executable, str(filter_script), "9091"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    time.sleep(3)  # Wait for Pyro4 service to start

    # 2) Test service connectivity
    test = StressTestService()
    try:
        filter_service = Pyro4.Proxy(test.service_uri)
        result = filter_service.submit_text("Connection test")
        print("Pyro4 service connection successful")
    except Exception as e:
        print(f"Error connecting to Pyro4 service: {e}")
        filter_proc.terminate()
        filter_proc.wait()
        sys.exit(1)

    # 3) Execute the stress test
    print(f"\n--- Iniciando stress test Pyro4 con {test.number_process} procesos ---")
    test.do_tests()

    # 4) Show results graph
    total_requests = [r * test.number_process for r in test.requests]
    plt.figure(figsize=(8, 4))
    plt.plot(total_requests, test.consumer_rate, 'b-o', label='Messages/s')
    plt.xlabel('Total Requests')
    plt.ylabel('Throughput (msg/s)')
    plt.title('Stress Test Pyro4 InsultFilter')
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.show()
    plt.savefig('pyro4_stress_test_results.png')
    
    # 5) Stop the filter service
    print("\nDeteniendo el servicio Pyro4...")
    try:
        filter_service = Pyro4.Proxy(test.service_uri)
        filter_service.stop_processing()
    except:
        pass
    
    filter_proc.terminate()
    filter_proc.wait()
    print("Stress test completado.")