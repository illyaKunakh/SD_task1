import Pyro4
import threading
import sys
import time

@Pyro4.expose
class Subscriber:
    def __init__(self):
        self.received_insults = []
        self.lock = threading.Lock()
    
    # Called by the InsultService to deliver broadcast insults
    # 1. Acquires thread lock for safe operation
    # 2. Adds received insult to the list
    # 3. Prints the received insult to console
    # 4. Returns True to confirm receipt
    def receive_insult(self, insult):
        with self.lock:
            self.received_insults.append(insult)
            print(f"Received broadcast: {insult}")
        return True
    
    # Retrieves all received insults
    # 1. Acquires thread lock for safe operation
    # 2. Returns a copy of the received insults list
    def get_received_insults(self):
        with self.lock:
            return self.received_insults.copy()
    
    # Returns the number of insults received
    def get_insults_count(self):
        with self.lock:
            return len(self.received_insults)
    
    # Clears the list of received insults
    # 1. Acquires thread lock for safe operation
    # 2. Empties the received insults list
    def clear_insults(self):
        with self.lock:
            self.received_insults.clear()
        return True

def run_subscriber(host='localhost', port=9092, service_host='localhost', service_port=9090):
    # Configurar Pyro4
    Pyro4.config.HOST = host
    
    # Crear subscriber
    subscriber = Subscriber()
    
    # Crear daemon y registrar el subscriber
    daemon = Pyro4.Daemon(host=host, port=port)
    subscriber_uri = daemon.register(subscriber, "insult.subscriber")
    
    print(f"Subscriber started on {host}:{port}")
    print(f"Subscriber URI: {subscriber_uri}")
    
    # Registrarse en el servicio de insultos
    try:
        service_uri = f"PYRO:insult.service@{service_host}:{service_port}"
        service = Pyro4.Proxy(service_uri)
        success = service.subscribe(str(subscriber_uri))
        if success:
            print(f"Successfully subscribed to InsultService at {service_uri}")
        else:
            print(f"Already subscribed to InsultService at {service_uri}")
    except Exception as e:
        print(f"Failed to subscribe: {e}")
        daemon.close()
        return
    
    print("Available methods:")
    print("- get_received_insults()")
    print("- get_insults_count()")
    print("- clear_insults()")
    print("Waiting for broadcasts...")
    
    try:
        daemon.requestLoop()
    except KeyboardInterrupt:
        print(f"\nShutting down subscriber on port {port}...")
        try:
            service.unsubscribe(str(subscriber_uri))
            print("Unsubscribed from service")
        except:
            pass
        daemon.close()

if __name__ == "__main__":
    # Permitir especificar puerto y host del servicio como argumentos
    port = 9092
    service_host = 'localhost'
    service_port = 9090
    
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            print("Error: Port must be a number")
            sys.exit(1)
    
    if len(sys.argv) > 2:
        service_host = sys.argv[2]
    
    if len(sys.argv) > 3:
        try:
            service_port = int(sys.argv[3])
        except ValueError:
            print("Error: Service port must be a number")
            sys.exit(1)
    
    run_subscriber(port=port, service_host=service_host, service_port=service_port)