import Pyro4
import random
import threading
import time
import sys

# Configure Pyro4 to accept pickle serialization
Pyro4.config.SERIALIZER = 'pickle'
Pyro4.config.SERIALIZERS_ACCEPTED.add('pickle')

@Pyro4.expose
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
    
    def add_subscriber(self, subscriber_uri):
        """Añade un suscriptor a la lista"""
        if subscriber_uri not in self.subscribers:
            self.subscribers.append(subscriber_uri)
            print(f"Nuevo suscriptor añadido: {subscriber_uri}")
            return True
        return False
    
    def remove_subscriber(self, subscriber_uri):
        """Elimina un suscriptor de la lista"""
        if subscriber_uri in self.subscribers:
            self.subscribers.remove(subscriber_uri)
            print(f"Suscriptor eliminado: {subscriber_uri}")
            return True
        return False
    
    def get_subscribers(self):
        """Método requerido por el broadcaster para obtener los suscriptores"""
        return self.subscribers

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
        for subscriber_uri in list(subscribers):
            try:
                proxy = Pyro4.Proxy(subscriber_uri)
                proxy.notify(insult)
                print(f"Notificado a {subscriber_uri}: {insult}")
            except Exception as e:
                print(f"Error al notificar a {subscriber_uri}: {e}")

def start_server(host='localhost', port=9090):
    # Crear la instancia del servicio
    service = InsultService()
    
    # Añadir algunos insultos de ejemplo
    service.add_insult("insult1")
    service.add_insult("insult2")
    service.add_insult("insult3")
    
    # Inicializar el servidor Pyro4
    with Pyro4.Daemon(host=host, port=port) as daemon:
        uri = daemon.register(service, objectId="insult.service")
        
        print(f"URI del servicio: {uri}")
        print(f"Servidor InsultService iniciado en {host}:{port}")
        
        try:
            daemon.requestLoop()
        except KeyboardInterrupt:
            print("\nServidor detenido por el usuario")
            service.broadcaster.stop()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    else:
        port = 9090

    start_server(port=port)