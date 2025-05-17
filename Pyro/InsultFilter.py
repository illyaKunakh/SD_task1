import Pyro4
import queue
import threading
import time
import sys

@Pyro4.expose
class InsultFilter:
    """
    Servicio de filtrado de insultos implementado con Pyro4.
    Permite filtrar textos que contienen insultos.
    """
    def __init__(self, insult_service_uri=None):
        """
        Inicializa el servicio de filtrado de insultos.
        
        Args:
            insult_service_uri: URI opcional del servicio de insultos para obtener la lista de insultos
        """
        self.filtered_texts = []
        self.insults = ["insult1", "insult2", "insult3"]  # insultos predeterminados
        
        # Iniciar workers para procesar las solicitudes
        self.task_queue = []
        self.queue_lock = threading.Lock()
        self.workers = []
        self.running = True
        
        # Iniciar un único worker por defecto
        self._start_worker()
        
        # Si se proporciona una URI del servicio de insultos, actualizar la lista
        if insult_service_uri:
            self._update_insults_from_service(insult_service_uri)
        
        print("Servicio de filtrado iniciado")
    
    def _start_worker(self):
        """Inicia un nuevo worker para procesar textos"""
        worker = threading.Thread(target=self._process_queue)
        worker.daemon = True
        worker.start()
        self.workers.append(worker)
        print(f"Worker iniciado. Total workers: {len(self.workers)}")
        return True
    
    def _process_queue(self):
        """Proceso en segundo plano para filtrar textos de la cola"""
        while self.running:
            text_to_process = None
            
            # Obtener una tarea de la cola de forma segura
            with self.queue_lock:
                if self.task_queue:
                    text_to_process = self.task_queue.pop(0)
            
            if text_to_process:
                # Procesar el texto
                filtered_text = self.filter_text(text_to_process)
                
                # Guardar el resultado
                self.filtered_texts.append(filtered_text)
                print(f"Texto procesado: {filtered_text[:30]}...")
            else:
                # No hay tareas, esperar un poco
                time.sleep(0.1)
    
    def _update_insults_from_service(self, insult_service_uri):
        """Actualiza la lista de insultos desde el servicio remoto"""
        try:
            insult_service = Pyro4.Proxy(insult_service_uri)
            self.insults = insult_service.get_insults()
            print(f"Lista de insultos actualizada: {len(self.insults)} insultos.")
            return True
        except Exception as e:
            print(f"Error al actualizar insultos desde el servicio: {e}")
            return False
    
    def submit_text(self, text):
        """
        Añade un texto a la cola para ser filtrado
        
        Args:
            text: Texto que puede contener insultos
        
        Returns:
            bool: True si se añadió correctamente a la cola
        """
        try:
            with self.queue_lock:
                self.task_queue.append(text)
            print(f"Texto añadido a la cola: {text[:20]}...")
            
            # Comprobar si necesitamos escalar dinámicamente
            self._check_scaling()
            
            return True
        except Exception as e:
            print(f"Error al añadir texto a la cola: {e}")
            return False
    
    def filter_text(self, text):
        """
        Filtra un texto reemplazando los insultos con 'CENSORED'
        
        Args:
            text: Texto que puede contener insultos
            
        Returns:
            str: Texto con insultos censurados
        """
        filtered = text
        for insult in self.insults:
            # Reemplazar los insultos con CENSORED (case insensitive)
            filtered = filtered.replace(insult.lower(), "CENSORED")
            filtered = filtered.replace(insult.upper(), "CENSORED")
            filtered = filtered.replace(insult.capitalize(), "CENSORED")
        
        return filtered
    
    def get_filtered_texts(self):
        """Devuelve la lista de textos filtrados"""
        return self.filtered_texts
    
    def get_queue_size(self):
        """Devuelve el tamaño actual de la cola de trabajo"""
        with self.queue_lock:
            return len(self.task_queue)
    
    def get_num_workers(self):
        """Devuelve el número actual de workers"""
        return len(self.workers)
    
    def _check_scaling(self):
        """
        Comprueba si es necesario escalar dinámicamente los workers
        basado en el tamaño de la cola y los workers actuales
        """
        with self.queue_lock:
            queue_size = len(self.task_queue)
        
        num_workers = len(self.workers)
        
        # Escalado simple: si la cola tiene más de 5 elementos por worker,
        # añadir un nuevo worker (hasta un máximo de 5 workers)
        if queue_size > num_workers * 5 and num_workers < 5:
            self._start_worker()
        # Si la cola está casi vacía y tenemos más de un worker, no hacemos nada
        # (los workers se mantendrán para futuras cargas)
    
    def shutdown(self):
        """Detiene el servicio"""
        self.running = False
        for worker in self.workers:
            if worker.is_alive():
                worker.join(timeout=1)
        print("Servicio de filtrado detenido")

def start_filter_server(host='localhost', port=9091, insult_service_uri=None):
    """Inicia el servicio de filtrado Pyro4"""
    # Configurar el servidor Pyro4
    daemon = Pyro4.Daemon(host=host, port=port)
    
    # Registrar el servicio
    service = InsultFilter(insult_service_uri)
    uri = daemon.register(service, "filter.service")
    
    print(f"URI del servicio de filtrado: {uri}")
    print(f"Servidor de filtrado iniciado en {host}:{port}")
    
    try:
        daemon.requestLoop()
    except KeyboardInterrupt:
        print("\nServidor detenido por el usuario")
        service.shutdown()
        daemon.shutdown()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    else:
        port = 9091
    
    # Para Pyro4 es necesario configurar serialización
    Pyro4.config.SERIALIZER = 'pickle'  # Usar pickle para objetos más complejos
    Pyro4.config.SERIALIZERS_ACCEPTED.add('pickle')
    
    # URI del servicio de insultos (se puede pasar como segundo argumento)
    insult_service_uri = None
    if len(sys.argv) > 2:
        insult_service_uri = sys.argv[2]
    
    start_filter_server(port=port, insult_service_uri=insult_service_uri)