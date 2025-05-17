from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import random
import threading
import time
import sys
import xmlrpc.client

# Restrict to a particular path
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

class InsultBroadcaster:
    def __init__(self, insult_provider, subscribers_manager):
        """
        Inicializa el broadcaster de insultos.
        
        Args:
            insult_provider: Objeto que provee acceso a los insultos (debe tener método get_random_insult)
            subscribers_manager: Objeto que gestiona los suscriptores (debe tener método get_subscribers)
        """
        self.insult_provider = insult_provider
        self.subscribers_manager = subscribers_manager
        self.running = False
        self.thread = None
    
    def start(self):
        """Inicia el broadcaster en un hilo separado"""
        if not self.running:
            self.running = True
            self.thread = threading.Thread(target=self._broadcast_loop)
            self.thread.daemon = True
            self.thread.start()
            print("Broadcaster iniciado correctamente")
    
    def stop(self):
        """Detiene el broadcaster"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=1)
            print("Broadcaster detenido")
    
    def _broadcast_loop(self):
        """Bucle principal que envía insultos aleatorios cada 5 segundos"""
        while self.running:
            subscribers = self.subscribers_manager.get_subscribers()
            if subscribers:
                try:
                    # Obtener un insulto aleatorio
                    insult = self.insult_provider.get_random_insult()
                    if insult:
                        print(f"Broadcasting insulto aleatorio: {insult}")
                        # Notificar a todos los suscriptores
                        self._notify_subscribers(subscribers, insult)
                except Exception as e:
                    print(f"Error en el broadcaster: {e}")
                
            # Esperar 5 segundos antes de la próxima transmisión
            time.sleep(5)
    
    def _notify_subscribers(self, subscribers, insult):
        """Notifica a todos los suscriptores sobre un nuevo insulto"""
        for subscriber_url in list(subscribers):
            try:
                with xmlrpc.client.ServerProxy(subscriber_url) as subscriber:
                    subscriber.notify(insult)
                    print(f"Notificado a {subscriber_url}: {insult}")
            except Exception as e:
                print(f"Error al notificar a {subscriber_url}: {e}")

class InsultService:
    def __init__(self):
        # Lista para almacenar insultos
        self.insults = []
        # Lista para almacenar suscriptores
        self.subscribers = []
        # Inicializar el broadcaster
        self.broadcaster = InsultBroadcaster(self, self)
        self.broadcaster.start()
    
    def add_insult(self, insult):
        """Añade un insulto si no existe ya en la lista"""
        if insult not in self.insults:
            self.insults.append(insult)
            print(f"Nuevo insulto añadido: {insult}")
            return True
        print(f"El insulto ya existe: {insult}")
        return False
    
    def get_insults(self):
        """Devuelve la lista completa de insultos"""
        return self.insults
    
    def get_random_insult(self):
        """Devuelve un insulto aleatorio de la lista"""
        if self.insults:
            return random.choice(self.insults)
        return "No hay insultos disponibles."
    
    def add_subscriber(self, subscriber_url):
        """Añade un suscriptor a la lista"""
        if subscriber_url not in self.subscribers:
            self.subscribers.append(subscriber_url)
            print(f"Nuevo suscriptor añadido: {subscriber_url}")
            return True
        return False
    
    def remove_subscriber(self, subscriber_url):
        """Elimina un suscriptor de la lista"""
        if subscriber_url in self.subscribers:
            self.subscribers.remove(subscriber_url)
            print(f"Suscriptor eliminado: {subscriber_url}")
            return True
        return False
    
    def get_subscribers(self):
        """Método requerido por el broadcaster para obtener los suscriptores"""
        return self.subscribers

def start_server(host='localhost', port=8000):
    server = SimpleXMLRPCServer((host, port), requestHandler=RequestHandler, allow_none=True)
    server.register_introspection_functions()
    
    # Crear la instancia del servicio
    service = InsultService()
    
    # Registrar la instancia
    server.register_instance(service)
    
    # Añadir algunos insultos de ejemplo
    service.add_insult("insult1")
    service.add_insult("insult2")
    service.add_insult("insult3")
    
    print(f"Servidor InsultService iniciado en http://{host}:{port}")
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServidor detenido por el usuario")
        service.broadcaster.stop()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    else:
        port = 8000
    
    start_server(port=port)