import Pyro4
import queue
import threading
import time
import sys

@Pyro4.expose
class InsultFilter:
    def __init__(self):
        self.insults = ['insult1', 'insult2', 'insult3', 'insult4', 'insult5', 
                       'insult6', 'insult7', 'insult8', 'insult9']
        self.work_queue = queue.Queue()
        self.result_list = []
        self.processing = True
        self.lock = threading.Lock()
        
        # Iniciar worker thread
        self.worker_thread = threading.Thread(target=self._process_queue, daemon=True)
        self.worker_thread.start()
    
    # Submits text for filtering using Work Queue pattern
    # 1. Adds text to the work queue for processing
    # 2. Returns True to confirm submission
    def submit_text(self, text):
        self.work_queue.put(text)
        print(f"Received text for filtering: {text}")
        return True
    
    # Retrieves the list of filtered texts
    # 1. Acquires thread lock for safe operation
    # 2. Returns a copy of the result list
    def get_results(self):
        with self.lock:
            return self.result_list.copy()
    
    # Returns the current size of the work queue
    # 1. Gets the number of items waiting to be processed
    def get_queue_size(self):
        return self.work_queue.qsize()
    
    # Worker thread that processes the work queue continuously
    # 1. Continuously retrieves text from work queue
    # 2. Filters insults from the text
    # 3. Stores filtered text in result list
    # 4. Simulates processing delay
    # 5. Prints the filtered text to console
    def _process_queue(self):
        while self.processing:
            try:
                text = self.work_queue.get(timeout=1)
                filtered_text = self._filter_text(text)
                
                with self.lock:
                    self.result_list.append(filtered_text)
                
                self.work_queue.task_done()
                time.sleep(0.0005)  # Simulate processing delay
                print(f"Filtered: {filtered_text}")
                
            except queue.Empty:
                continue
    
    # Filters insults from text by replacing them with "CENSORED"
    # 1. Iterates through all known insults
    # 2. Replaces any found insults with "CENSORED"
    # 3. Returns the filtered text
    def _filter_text(self, text):
        filtered = text
        for insult in self.insults:
            if insult in filtered:
                filtered = filtered.replace(insult, "CENSORED")
        return filtered
    
    # Stops the processing worker thread
    # 1. Sets processing flag to False
    # 2. Worker thread will exit on next iteration
    def stop_processing(self):
        self.processing = False
        return True
    
    # Returns the number of processed results
    def get_results_count(self):
        with self.lock:
            return len(self.result_list)
    
    # Clears the result list
    def clear_results(self):
        with self.lock:
            self.result_list.clear()
        return True

def run_server(host='localhost', port=9091):
    # Configurar Pyro4
    Pyro4.config.HOST = host
    
    # Crear el servicio
    filter_service = InsultFilter()
    
    # Crear daemon y registrar el servicio
    daemon = Pyro4.Daemon(host=host, port=port)
    uri = daemon.register(filter_service, "insult.filter")
    
    print(f"InsultFilter started on {host}:{port}")
    print(f"Filter URI: {uri}")
    
    try:
        daemon.requestLoop()
    except KeyboardInterrupt:
        print(f"\nShutting down server on port {port}...")
        filter_service.stop_processing()
        daemon.close()

if __name__ == "__main__":
    # Permitir especificar el puerto como argumento
    port = 9091
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            print("Error: Port must be a number")
            sys.exit(1)
    
    run_server(port=port)