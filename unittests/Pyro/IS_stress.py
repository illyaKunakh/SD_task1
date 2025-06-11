#!/usr/bin/env python3
import Pyro4
import subprocess
import time
import sys
import multiprocessing
import matplotlib.pyplot as plt
from pathlib import Path

class StressTestService:
    def __init__(self):
        self.number_process = 8
        self.requests = [1000, 2000, 5000, 10000]
        # Save throughput results for each test
        self.consumer_rate = []
        self.service_uri = "PYRO:insult.service@localhost:9090"
    
    # This function is executed by each producer process.
    # 1. Connects to Pyro4 InsultService.
    # 2. Sends `requests_per_producer` store_insult requests to add insults.
    # 3. Also calls get_random_insult to simulate mixed workload.
    # 4. Each insult is unique based on the process PID and an index.
    def send_insult(self, requests_per_producer, idx):
        try:
            # Connect to Pyro4 service
            service = Pyro4.Proxy(self.service_uri)
            pid = multiprocessing.current_process().pid
            
            # Mix of store_insult and get_random_insult operations
            store_count = requests_per_producer // 2
            get_count = requests_per_producer - store_count
            
            # Store insults
            for i in range(store_count):
                insult = f"stress_insult_{pid}_{idx}_{i}"
                result = service.store_insult(insult)
            
            # Get random insults and check service status
            for i in range(get_count):
                if i % 3 == 0:
                    result = service.get_random_insult()
                elif i % 3 == 1:
                    result = service.get_insults_count()
                else:
                    result = service.get_subscribers_count()
                    
        except Exception as e:
            print(f"Error in producer {idx}: {e}")

    # Runs a test with a specified number of producers and requests.
    # 1. Starts the InsultService process.
    # 2. Creates multiple processes that execute the send_insult function.
    # 3. Starts all processes and waits for them to finish.
    # 4. Calculates the total messages sent and the throughput.
    # 5. Appends the throughput to the consumer_rate list.
    # 6. Prints the total time taken and the throughput.
    # 7. Terminates the service process after each test.
    def run_test(self, per_producer_count):
        total_msgs = self.number_process * per_producer_count
        
        # Start the InsultService
        service_script = Path(__file__).parents[2] / "Pyro" / "InsultService.py"
        if not service_script.exists():
            print(f"ERROR: no he encontrado {service_script}", file=sys.stderr)
            sys.exit(1)

        service_proc = subprocess.Popen(
            [sys.executable, str(service_script), "9090"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        time.sleep(3)  # Wait for Pyro4 service to start
        
        # Test service connectivity
        try:
            service = Pyro4.Proxy(self.service_uri)
            # Initialize with a test insult
            service.store_insult("test_insult_initial")
            print(f"Pyro4 service ready on port 9090")
        except Exception as e:
            print(f"Error connecting to Pyro4 service: {e}")
            service_proc.terminate()
            service_proc.wait()
            return

        print(f"Test → {per_producer_count} peticiones por productor "
              f"(total {total_msgs})")
        
        producers = []
        start_time = time.time()
        
        # Start all producer processes
        for i in range(self.number_process):
            p = multiprocessing.Process(
                target=self.send_insult,
                args=(per_producer_count, i)
            )
            producers.append(p)
            p.start()
        
        # Wait for all producers to finish
        for p in producers:
            p.join()
        
        elapsed = time.time() - start_time
        rate = total_msgs / elapsed
        self.consumer_rate.append(rate)
        print(f"  Tiempo total: {elapsed:.2f}s — Throughput: {rate:.2f} msg/s")
        
        # Show final service statistics
        try:
            service = Pyro4.Proxy(self.service_uri)
            insults_count = service.get_insults_count()
            print(f"  Total insults stored: {insults_count}")
        except Exception as e:
            print(f"  Could not get final statistics: {e}")
        
        # Stop the service
        service_proc.terminate()
        service_proc.wait(timeout=5)
        if service_proc.poll() is None:
            service_proc.kill()
            service_proc.wait()

    def do_tests(self):
        print(f"\n--- Iniciando stress test Pyro4 InsultService con {self.number_process} procesos ---")
        for req in self.requests:
            self.run_test(req)
            time.sleep(1)  # Brief pause between tests

if __name__ == '__main__':
    test = StressTestService()
    test.do_tests()
    
    # Show results graph
    total_requests = [r * test.number_process for r in test.requests]
    plt.figure(figsize=(8, 4))
    plt.plot(total_requests, test.consumer_rate, 'b-o', label='Messages/s')
    plt.xlabel('Total Requests')
    plt.ylabel('Throughput (msg/s)')
    plt.title('Stress Test Pyro4 InsultService')
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig('pyro4_insult_service_stress_test_results.png')
    plt.show()
    
    print("\nStress test completado.")