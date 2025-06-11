#!/usr/bin/env python3

import Pyro4
import multiprocessing
import time
import matplotlib.pyplot as plt
import subprocess
import sys
from pathlib import Path

class SpeedupTestService:
    def __init__(self):
        self.number_process = 4  # number of producers
        self.requests = 10000    # messages per producer
        self.consumer_rate = []
        self.base_port = 9091    # starting port for Pyro4 servers
    
    # Sends insults to the InsultFilter service via Pyro4.
    # Uses round-robin to distribute requests across multiple servers.
    def send_insult(self, requests, num_servers):
        try:
            # Create connections to all available servers
            servers = []
            for i in range(num_servers):
                port = self.base_port + i
                uri = f"PYRO:insult.filter@localhost:{port}"
                proxy = Pyro4.Proxy(uri)
                servers.append(proxy)
            
            # Send requests using round-robin
            for i in range(requests):
                server_idx = i % num_servers
                proxy = servers[server_idx]
                test_message = f"Test message {i} with insult1"
                result = proxy.submit_text(test_message)
                
        except Exception as e:
            print(f"Error in producer: {e}")

    # Runs a test with a specified number of servers and requests.
    def run_test(self, num_servers):
        print(f"Test-> {num_servers} servers: {self.requests} requests per producer")
        
        procs = []
        # Create n producers that execute the send_insult function
        for _ in range(self.number_process):
            p = multiprocessing.Process(target=self.send_insult, args=(self.requests, num_servers))
            procs.append(p)

        start = time.time()
        for proc in procs:
            proc.start()
        for proc in procs:
            proc.join()
        fin = time.time()

        all_time = fin - start
        consum = self.number_process * self.requests / all_time
        self.consumer_rate.append(consum)
        print(f"  Total Time: {all_time:.2f}s")
        print(f"  Throughput: {consum:.2f} msg/s")

    # Starts multiple instances of the InsultFilter service and runs tests.
    # 1. Starts up to 3 InsultFilter servers on different ports.
    # 2. For each number of servers (1, 2, 3), runs a test.
    # 3. Terminates all processes after tests are done.
    # 4. Plots the speedup graph.
    def do_tests(self):
        filter_script = Path(__file__).parent.parent.parent / 'Pyro' / 'InsultFilter.py'
        if not filter_script.exists():
            print(f"ERROR: no encontrado {filter_script}", file=sys.stderr)
            sys.exit(1)

        procs = []
        max_servers = 3
        
        # Start servers on different ports
        for servers in range(max_servers):
            port = self.base_port + servers
            # Start server with specific port
            proc = subprocess.Popen(
                [sys.executable, str(filter_script), str(port)],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
            procs.append(proc)
            time.sleep(3)  # Wait for Pyro4 server to start (longer than XML-RPC)
            
            # Run test with current number of servers
            self.run_test(servers + 1)
        
        # Terminate all processes when tests are done
        for proc in procs:
            proc.terminate()
            proc.wait()

if __name__ == '__main__':
    test = SpeedupTestService()
    test.do_tests()

    # Plot speedup graph
    plt.figure(figsize=(8, 4))
    num_servers = [1, 2, 3]
    plt.plot(num_servers, num_servers, 'r-o', label='Ideal')
    speed_up = [val / test.consumer_rate[0] for val in test.consumer_rate]
    plt.plot(num_servers, speed_up, 'b-o', label='Real')
    plt.xlabel('(InsultFilter) Nodes')
    plt.ylabel('Speedup')
    plt.legend()
    plt.grid(True)
    plt.title("SpeedUp Pyro4 InsultFilter")
    plt.savefig('IFPyroSpeed.png')
    plt.show()