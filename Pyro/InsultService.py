import Pyro4
import threading
import time
import random
import sys
from multiprocessing import Process, Event

@Pyro4.expose
class InsultService:
    def __init__(self):
        self.insult_list = []
        self.broadcast_process = None
        self.stop_broadcast = Event()
        self.subscribers = []  # Lista de URIs de suscriptores
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
                insult = random.choice(self.insult_list)
                print(f"Sent random insult: {insult}")
                return insult
            return "No insults available"
    
    # Registers a subscriber for broadcast notifications
    # 1. Acquires thread lock for safe operation
    # 2. Adds subscriber URI to list if not already present
    # 3. Returns True if added, False if already subscribed
    def subscribe(self, subscriber_uri):
        with self.lock:
            if subscriber_uri not in self.subscribers:
                self.subscribers.append(subscriber_uri)
                print(f"Subscriber registered: {subscriber_uri}")
                return True
            else:
                print(f"Subscriber already registered: {subscriber_uri}")
                return False
    
    # Removes a subscriber from broadcast notifications
    # 1. Acquires thread lock for safe operation
    # 2. Removes subscriber URI from list if present
    # 3. Returns True if removed, False if not found
    def unsubscribe(self, subscriber_uri):
        with self.lock:
            if subscriber_uri in self.subscribers:
                self.subscribers.remove(subscriber_uri)
                print(f"Subscriber unregistered: {subscriber_uri}")
                return True
            else:
                print(f"Subscriber not found: {subscriber_uri}")
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
        while not self.stop_broadcast.is_set():
            with self.lock:
                current_insults = self.insult_list.copy()
                current_subscribers = self.subscribers.copy()
            
            if current_insults and current_subscribers:
                insult = random.choice(current_insults)
                for subscriber_uri in current_subscribers:
                    try:
                        subscriber = Pyro4.Proxy(subscriber_uri)
                        subscriber.receive_insult(insult)
                        print(f"Broadcasted to {subscriber_uri}: {insult}")
                    except Exception as e:
                        print(f"Failed to broadcast to {subscriber_uri}: {e}")
            time.sleep(5)

def run_server(host='localhost', port=9090):
    # Configurar Pyro4
    Pyro4.config.HOST = host
    
    # Crear el servicio
    service = InsultService()
    
    # Crear daemon y registrar el servicio
    daemon = Pyro4.Daemon(host=host, port=port)
    uri = daemon.register(service, "insult.service")
    
    print(f"InsultService started on {host}:{port}")
    print(f"Service URI: {uri}")
    
    try:
        daemon.requestLoop()
    except KeyboardInterrupt:
        print(f"\nShutting down server on port {port}...")
        service.stop_broadcast_service()
        daemon.close()

if __name__ == "__main__":
    # Permitir especificar el puerto como argumento
    port = 9090
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            print("Error: Port must be a number")
            sys.exit(1)
    
    run_server(port=port)