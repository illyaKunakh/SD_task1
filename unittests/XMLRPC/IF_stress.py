import xmlrpc.client
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
        self.server_url = "http://localhost:8001"
    
    # Sends insults to the InsultFilter service via XML-RPC.
    # 1. Connects to XML-RPC server.
    # 2. Sends 'requests_per_producer' number of filter requests to the server.
    # 3. Each request calls the submit_text method with a test message.
    # 4. Measures the time taken to process all requests.
    def send_insult(self, requests_per_producer, idx):
        try:
            proxy = xmlrpc.client.ServerProxy(self.server_url)
            
            for i in range(requests_per_producer):
                # Send a test message to be filtered
                test_message = f"Test message {idx}_{i}"
                result = proxy.submit_text(test_message)
                
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
    def do_tests(self):
        for req in self.requests:
            self.run_test(req)

if __name__ == '__main__':
    # 1) Start the InsultFilter XML-RPC service in the background
    filter_script = Path(__file__).parent.parent.parent / 'XMLRPC' / 'InsultFilter.py'
    if not filter_script.exists():
        print(f"ERROR: no encontrado {filter_script}", file=sys.stderr)
        sys.exit(1)

    filter_proc = subprocess.Popen(
        [sys.executable, str(filter_script)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    time.sleep(3)  # Wait for the XML-RPC server to start

    # 2) Execute the stress test
    test = StressTestService()
    
    # Test server connectivity first
    try:
        proxy = xmlrpc.client.ServerProxy(test.server_url)
        proxy.submit_text("test")
        print("XML-RPC server connection successful")
    except Exception as e:
        print(f"Error connecting to XML-RPC server: {e}")
        filter_proc.terminate()
        filter_proc.wait()
        sys.exit(1)
    
    test.do_tests()

    # 3) Show results graph
    total_requests = [r * test.number_process for r in test.requests]
    plt.figure(figsize=(8, 4))
    plt.plot(total_requests, test.consumer_rate, 'b-o', label='Messages/s')
    plt.xlabel('Total Requests')
    plt.ylabel('Throughput (msg/s)')
    plt.title('Stress Test XML-RPC InsultFilter')
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.show()

    # 4) Stop the filter service
    filter_proc.terminate()
    filter_proc.wait()