#!/usr/bin/env python3

import Pyro4
import multiprocessing
import time
import matplotlib.pyplot as plt
import subprocess
import sys
from pathlib import Path

class SpeedupTestPyro4Service:
    def __init__(self):
        self.number_process = 4  # number of producers
        self.requests_per_producer = 10000
        self.consumer_rate = []
        self.base_port = 9090    # starting port for Pyro4 servers

    # Sends requests to multiple InsultService instances via Pyro4.
    # Uses round-robin to distribute requests across servers.
    def _send_requests(self, n_requests: int, num_servers: int):
        try:
            # Create connections to all available servers
            servers = []
            for i in range(num_servers):
                port = self.base_port + i
                uri = f"PYRO:insult.service@localhost:{port}"
                proxy = Pyro4.Proxy(uri)
                servers.append(proxy)
            
            # Send requests using round-robin
            store_count = n_requests // 2
            get_count = n_requests - store_count
            
            # Add insults to servers
            for i in range(store_count):
                server_idx = i % num_servers
                proxy = servers[server_idx]
                insult = f"Speedup insult {i}"
                result = proxy.store_insult(insult)
            
            # Get random insults from servers
            for i in range(get_count):
                server_idx = i % num_servers
                proxy = servers[server_idx]
                result = proxy.get_random_insult()
                
        except Exception as e:
            print(f"Error in producer: {e}")

    # Runs a test with a specified number of InsultService instances.
    # 1. Starts `self.number_process` producers that send requests to the services.
    # 2. Measures the total time taken and calculates the throughput.
    # 3. Appends the throughput to self.consumer_rate for later analysis.
    # 4. Prints the results of the test.
    def run_test(self, num_servers: int):
        print(f"\n[Test Pyro4] → Testing with {num_servers} InsultService node(s)...")
        
        # Start producers that will send requests to the services
        producers = []
        start_time = time.time()
        for _ in range(self.number_process):
            p = multiprocessing.Process(
                target=self._send_requests,
                args=(self.requests_per_producer, num_servers)
            )
            producers.append(p)
            p.start()

        for p in producers:
            p.join()

        elapsed = time.time() - start_time
        total_messages = self.number_process * self.requests_per_producer
        throughput = total_messages / elapsed
        self.consumer_rate.append(throughput)
        print(f"  → Servers: {num_servers} | Total time: {elapsed:.2f}s | "
              f"Throughput: {throughput:.2f} msg/s")

    def do_speedup(self):
        """
        Runs tests for 1, 2 and 3 service instances. Then plots speedup graph.
        """
        service_script = Path(__file__).parent.parent.parent / 'Pyro' / 'InsultService.py'
        if not service_script.exists():
            print(f"ERROR: no encontrado {service_script}", file=sys.stderr)
            sys.exit(1)

        procs_service = []
        max_servers = 3
        
        # Start servers on different ports and run tests
        for servers in range(max_servers):
            port = self.base_port + servers
            # Start server with specific port
            p = subprocess.Popen(
                [sys.executable, str(service_script), str(port)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            procs_service.append(p)
            time.sleep(3)  # Wait for Pyro4 server to start (longer than XML-RPC)
            
            # Initialize servers with some insults for testing
            try:
                uri = f"PYRO:insult.service@localhost:{port}"
                proxy = Pyro4.Proxy(uri)
                for i in range(10):
                    proxy.store_insult(f"Initial insult {i}")
            except Exception as e:
                print(f"Warning: Could not initialize server on port {port}: {e}")
            
            # Run test with current number of servers
            self.run_test(servers + 1)

        # Terminate all service processes
        for p in procs_service:
            p.terminate()
            try:
                p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                p.kill()

        # Plot speedup graph
        nodes = [1, 2, 3]
        ideal = [1.0, 2.0, 3.0]
        real = [self.consumer_rate[i] / self.consumer_rate[0] for i in range(3)]

        plt.figure(figsize=(8, 4))
        plt.plot(nodes, ideal, 'r--', marker='o', label='Ideal (N)')
        plt.plot(nodes, real, 'b-o', label='Real')
        plt.xlabel('Number of InsultService instances')
        plt.ylabel('Speedup (relative_throughput)')
        plt.title('SpeedUp Pyro4 → InsultService')
        plt.grid(True)
        plt.legend()
        plt.tight_layout()
        plt.savefig('ISPyroSpeed.png')
        plt.show()


if __name__ == "__main__":
    tester = SpeedupTestPyro4Service()
    tester.do_speedup()