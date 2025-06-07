import xmlrpc.server
import threading
import time
import random
import sys
from multiprocessing import Process, Event

class InsultService:
    def __init__(self):
        self.insult_list = []
        self.broadcast_process = None
        self.stop_broadcast = Event()
        self.subscribers = []  # Lista de URLs de suscriptores
        self.lock = threading.Lock()  # Para operaciones thread-safe
        
    # Stores an insult remotely only if it's not already in the list
    # 1. Acquires thread lock for safe operation
    # 2. Checks if insult already exists in the list
    # 3. Adds insult to list if not present
    # 4. Returns True if added, False if already exists
    def store_insult(self, insult):
        with self.lock:
            if insult not in self.insult_list:
                self.insult_list.append(insult)
                print(f"Stored insult: {insult}")
                return True
            else:
                print(f"Insult '{insult}' already exists")
                return False
    
    # Retrieves the entire list of insults
    # 1. Acquires thread lock for safe operation
    # 2. Returns a copy of the current insult list
    def get_insult_list(self):
        with self.lock:
            return self.insult_list.copy()
    
    # Returns a random insult from the stored list
    # 1. Acquires thread lock for safe operation
    # 2. Randomly selects an insult if list is not empty
    # 3. Returns the selected insult or a default message
    def get_random_insult(self):
        with self.lock:
            if self.insult_list:
                return random.choice(self.insult_list)
            return "No insults available"
    
    # Registers a subscriber for broadcast notifications
    # 1. Acquires thread lock for safe operation
    # 2. Adds subscriber URL to list if not already present
    # 3. Returns True if added, False if already subscribed
    def subscribe(self, subscriber_url):
        with self.lock:
            if subscriber_url not in self.subscribers:
                self.subscribers.append(subscriber_url)
                print(f"Subscriber registered: {subscriber_url}")
                return True
            return False
    
    # Removes a subscriber from broadcast notifications
    # 1. Acquires thread lock for safe operation
    # 2. Removes subscriber URL from list if present
    # 3. Returns True if removed, False if not found
    def unsubscribe(self, subscriber_url):
        with self.lock:
            if subscriber_url in self.subscribers:
                self.subscribers.remove(subscriber_url)
                print(f"Subscriber unregistered: {subscriber_url}")
                return True
            return False
    
    def get_subscribers_count(self):
        with self.lock:
            return len(self.subscribers)
    
    def get_insults_count(self):
        with self.lock:
            return len(self.insult_list)
    
    # Starts the broadcast process that sends random insults every 5 seconds
    # 1. Checks if broadcast process is not already running
    # 2. Clears the stop event and creates new process
    # 3. Starts the broadcast worker process
    # 4. Returns True if started, False if already active
    def start_broadcast(self):
        if self.broadcast_process is None or not self.broadcast_process.is_alive():
            self.stop_broadcast.clear()
            self.broadcast_process = Process(target=self._broadcast_worker)
            self.broadcast_process.start()
            print("Broadcast started")
            return True
        else:
            print("Broadcast already active")
            return False
    
    # Stops the broadcast process
    # 1. Sets the stop event to signal worker to stop
    # 2. Waits for process to join or terminates if timeout
    # 3. Returns True if stopped, False if not running
    def stop_broadcast_service(self):
        if self.broadcast_process and self.broadcast_process.is_alive():
            self.stop_broadcast.set()
            self.broadcast_process.join(timeout=2)
            if self.broadcast_process.is_alive():
                self.broadcast_process.terminate()
            print("Broadcast stopped")
            return True
        else:
            print("No broadcast active")
            return False
    
    # Broadcast worker that runs in separate process
    # 1. Continuously runs until stop event is set
    # 2. Gets current insults and subscribers safely
    # 3. Selects random insult and sends to all subscribers
    # 4. Sleeps for 5 seconds before next broadcast
    def _broadcast_worker(self):
        import xmlrpc.client
        while not self.stop_broadcast.is_set():
            with self.lock:
                current_insults = self.insult_list.copy()
                current_subscribers = self.subscribers.copy()
            
            if current_insults and current_subscribers:
                insult = random.choice(current_insults)
                for subscriber_url in current_subscribers:
                    try:
                        client = xmlrpc.client.ServerProxy(subscriber_url)
                        client.receive_insult(insult)
                        print(f"Broadcasted to {subscriber_url}: {insult}")
                    except Exception as e:
                        print(f"Failed to broadcast to {subscriber_url}: {e}")
            time.sleep(5)

def run_server(host='localhost', port=8000):
    service = InsultService()
    
    # Crear servidor XMLRPC
    server = xmlrpc.server.SimpleXMLRPCServer((host, port), allow_none=True)
    server.register_instance(service)
    server.register_introspection_functions()
    
    print(f"InsultService started on {host}:{port}")
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(f"\nShutting down server on port {port}...")
        service.stop_broadcast_service()
        server.shutdown()

if __name__ == "__main__":
    # Permitir especificar el puerto como argumento
    port = 8000
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            print("Error: Port must be a number")
            sys.exit(1)
    
    run_server(port=port)