from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import threading
import multiprocessing
import sys
import queue
import time

# Restrict to a particular path
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

class InsultFilter:
    def __init__(self, insult_service_url=None):
        """
        Inicializa el servicio de filtrado de insultos.
        
        Args:
            insult_service_url: URL opcional del servicio de insultos para obtener la lista de insultos
        """
        self.filtered_texts = []
        self.insult_service_url = insult_service_url
        self.insults = ["insult1", "insult2"]  # insultos predeterminados
        
        # Cola de trabajo para el procesamiento en segundo plano
        self.task_queue = queue.Queue()
        
        # Iniciar workers para procesar las solicitudes
        self.workers = []
        self.running = True
        
        # Iniciar un único worker por defecto
        self._start_worker()
        
        # Si se proporciona una URL del servicio de insultos, actualizar la lista
        if insult_service_url:
            self._update_insults_from_service()
    
    def _start_worker(self):
        """Inicia un nuevo worker para procesar textos"""
        worker = threading.Thread(target=self._process_queue)
        worker.daemon = True
        worker.start()
        self.workers.append(worker)
        print(f"Worker iniciado. Total workers: {len(self.workers)}")
        return True
    
    def _stop_worker(self):
        """Detiene un worker si hay más de uno"""
        if len(self.workers) > 1:
            # Marcamos un worker para detener - se detendrá en su próxima iteración
            worker = self.workers.pop()
            print(f"Worker marcado para detener. Workers restantes: {len(self.workers)}")
            return True
        return False
    
    def _process_queue(self):
        """Proceso en segundo plano para filtrar textos de la cola"""
        while self.running:
            try:
                # Intenta obtener una tarea con un timeout para poder verificar si debe seguir ejecutándose
                text = self.task_queue.get(timeout=1)
                
                # Procesar el texto
                filtered_text = self.filter_text(text)
                
                # Guardar el resultado
                self.filtered_texts.append(filtered_text)
                
                # Marcar la tarea como completada
                self.task_queue.task_done()
            except queue.Empty:
                # No hay tareas en la cola, el worker espera
                continue
            except Exception as e:
                print(f"Error en worker: {e}")
    
    def _update_insults_from_service(self):
        """Actualiza la lista de insultos desde el servicio remoto"""
        try:
            import xmlrpc.client
            with xmlrpc.client.ServerProxy(self.insult_service_url) as proxy:
                self.insults = proxy.get_insults()
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
            self.task_queue.put(text)
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
        return self.task_queue.qsize()
    
    def get_num_workers(self):
        """Devuelve el número actual de workers"""
        return len(self.workers)
    
    def _check_scaling(self):
        """
        Comprueba si es necesario escalar dinámicamente los workers
        basado en el tamaño de la cola y los workers actuales
        """
        queue_size = self.task_queue.qsize()
        num_workers = len(self.workers)
        
        # Escalado simple: si la cola tiene más de 5 elementos por worker,
        # añadir un nuevo worker (hasta un máximo de 5 workers)
        if queue_size > num_workers * 5 and num_workers < 5:
            self._start_worker()
        # Si la cola está casi vacía y tenemos más de un worker, eliminar uno
        elif queue_size < 2 and num_workers > 1:
            self._stop_worker()

def start_server(host='localhost', port=8001, insult_service_url=None):
    """
    Inicia el servidor XMLRPC para el servicio InsultFilter
    
    Args:
        host: Host donde iniciar el servidor
        port: Puerto donde escuchar
        insult_service_url: URL opcional del servicio de insultos
    """
    server = SimpleXMLRPCServer((host, port), requestHandler=RequestHandler, allow_none=True)
    server.register_introspection_functions()
    
    # Crear la instancia del servicio
    service = InsultFilter(insult_service_url)
    
    # Registrar la instancia y los métodos disponibles
    server.register_instance(service)
    
    print(f"Servidor InsultFilter iniciado en http://{host}:{port}")
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServidor detenido por el usuario")
        service.running = False

if __name__ == "__main__":
    # Configuración por defecto
    port = 8001
    insult_service_url = None
    
    # Procesar argumentos de línea de comandos
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    if len(sys.argv) > 2:
        insult_service_url = sys.argv[2]
    
    start_server(port=port, insult_service_url=insult_service_url)