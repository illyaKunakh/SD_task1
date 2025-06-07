import xmlrpc.server
import xmlrpc.client
import sys
import threading

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

def run_subscriber(host='localhost', port=8002, service_url='http://localhost:8000'):
    subscriber = Subscriber()
    
    # Crear servidor para recibir broadcasts
    server = xmlrpc.server.SimpleXMLRPCServer((host, port), allow_none=True)
    server.register_instance(subscriber)
    
    # Registrarse en el servicio de insultos
    try:
        service = xmlrpc.client.ServerProxy(service_url)
        subscriber_url = f"http://{host}:{port}"
        success = service.subscribe(subscriber_url)
        if success:
            print(f"Successfully subscribed to InsultService at {service_url}")
        else:
            print(f"Already subscribed to InsultService at {service_url}")
    except Exception as e:
        print(f"Failed to subscribe: {e}")
        return
    
    print(f"Subscriber started on {host}:{port}")
    print("Available methods:")
    print("- get_received_insults()")
    print("- get_insults_count()")
    print("- clear_insults()")
    print("Waiting for broadcasts...")
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(f"\nShutting down subscriber on port {port}...")
        try:
            subscriber_url = f"http://{host}:{port}"
            service.unsubscribe(subscriber_url)
            print("Unsubscribed from service")
        except:
            pass
        server.shutdown()

if __name__ == "__main__":
    # Permitir especificar puerto y URL del servicio como argumentos
    port = 8002
    service_url = 'http://localhost:8000'
    
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            print("Error: Port must be a number")
            sys.exit(1)
    
    if len(sys.argv) > 2:
        service_url = sys.argv[2]
    
    run_subscriber(port=port, service_url=service_url)