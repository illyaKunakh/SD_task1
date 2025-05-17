# File: MultipleNodeStatic.py
import os
import sys
import pika
import redis
import multiprocessing
import time
import matplotlib.pyplot as plt
import random
from functools import partial
import xmlrpc.client
import xmlrpc.server
import threading
import socket
import queue
import Pyro4

# Añadir la carpeta raíz al path para poder importar como paquetes
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

# Importar servicios y filtros desde los paquetes (RabbitMQ y Redis)
from RabbitMQ.InsultService import receive as rmq_receive, broadcast as rmq_broadcast, list_insults as rmq_list
from RabbitMQ.InsultFilter  import receive as rmq_filter_receive, list_filtered as rmq_filter_list

from Redis.InsultService import receive as rds_receive, broadcast as rds_broadcast, list_insults as rds_list
from Redis.InsultFilter  import receive as rds_filter_receive, list_filtered as rds_filter_list

# Parámetros de la prueba
BATCH_SIZE = 50    # Mensajes por lote
TOTAL_REQUESTS = 5000

# Worker RabbitMQ InsultService
def rabbitmq_worker_insult(worker_id, queue_name, num_requests):
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost', heartbeat=0, blocked_connection_timeout=300
    ))
    channel = connection.channel()
    exchange_name = 'insults_exchange'
    # Exchange in-memory (no durable) for benchmark
    channel.exchange_declare(exchange=exchange_name, exchange_type='fanout', durable=False)
    result = channel.queue_declare(queue='', exclusive=True)
    temp_queue = result.method.queue
    channel.queue_bind(exchange=exchange_name, queue=temp_queue)
    # No publisher confirms to maximize throughput

    start_time = time.time()
    batch = []
    messages_sent = 0
    # Enviar mensajes de prueba en lotes
    for i in range(num_requests):
        batch.append(f"insult{i}")
        if len(batch) >= BATCH_SIZE or i == num_requests - 1:
            for msg in batch:
                channel.basic_publish(
                    exchange=exchange_name,
                    routing_key='',
                    body=msg.encode(),
                    properties=pika.BasicProperties(delivery_mode=1),  # transient
                    mandatory=False
                )
                messages_sent += 1
            batch = []

    connection.close()
    elapsed = time.time() - start_time
    return elapsed, messages_sent

# Worker Redis InsultService
def redis_worker_insult(worker_id, queue_name, num_requests):
    client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True,
                         socket_timeout=5, socket_connect_timeout=10, socket_keepalive=True, health_check_interval=30)
    start_time = time.time()
    messages_sent = 0
    worker_queue = f"{queue_name}_{worker_id}"
    pipeline = client.pipeline(transaction=False)

    for i in range(num_requests):
        pipeline.rpush(worker_queue, f"insult{i}")
        if (i + 1) % BATCH_SIZE == 0 or i == num_requests - 1:
            pipeline.execute()
            messages_sent += min(BATCH_SIZE, num_requests - i + (i % BATCH_SIZE))
            pipeline = client.pipeline(transaction=False)

    # Mover muestra al canal principal
    if worker_id == 0:
        sample = client.lrange(worker_queue, 0, min(10, num_requests)-1)
        for item in sample:
            client.rpush(queue_name, item)

    elapsed = time.time() - start_time
    return elapsed, messages_sent

# Worker XMLRPC InsultService
def xmlrpc_worker_insult(worker_id, queue_name, num_requests):
    # Para XMLRPC, utilizamos load balancing simulado
    # Configuración de servidores (en un caso real, serían diferentes máquinas)
    base_port = 8000
    num_servers = 3
    
    # Seleccionar servidor basado en worker_id (para distribuir carga)
    server_port = base_port + (worker_id % num_servers)
    server_url = f"http://localhost:{server_port}"
    
    try:
        # Crear cliente XMLRPC
        proxy = xmlrpc.client.ServerProxy(server_url)
        
        start_time = time.time()
        messages_sent = 0
        batch = []
        
        # Enviar mensajes en lotes
        for i in range(num_requests):
            batch.append(f"insult{i}_{worker_id}")
            if len(batch) >= BATCH_SIZE or i == num_requests - 1:
                for msg in batch:
                    try:
                        # Añadir insulto al servidor
                        proxy.add_insult(msg)
                        messages_sent += 1
                    except Exception as e:
                        print(f"Error enviando mensaje a {server_url}: {e}")
                batch = []
        
        elapsed = time.time() - start_time
        return elapsed, messages_sent
    except Exception as e:
        print(f"Error al conectar con servidor XMLRPC {server_url}: {e}")
        return 999, 0  # Retornar tiempo muy alto para indicar error

# Worker Pyro InsultService
def pyro_worker_insult(worker_id, queue_name, num_requests):
    # Configuración para Pyro
    Pyro4.config.SERIALIZER = 'pickle'
    Pyro4.config.SERIALIZERS_ACCEPTED.add('pickle')
    
    # Configuración de servidores (en un caso real, serían diferentes máquinas)
    base_port = 9090
    num_servers = 3
    
    # Seleccionar servidor basado en worker_id (para distribuir carga)
    server_port = base_port + (worker_id % num_servers)
    uri = f"PYRO:insult.service@localhost:{server_port}"
    
    try:
        # Crear cliente Pyro
        proxy = Pyro4.Proxy(uri)
        
        start_time = time.time()
        messages_sent = 0
        batch = []
        
        # Enviar mensajes en lotes
        for i in range(num_requests):
            batch.append(f"insult{i}_{worker_id}")
            if len(batch) >= BATCH_SIZE or i == num_requests - 1:
                for msg in batch:
                    try:
                        # Añadir insulto al servidor
                        proxy.add_insult(msg)
                        messages_sent += 1
                    except Exception as e:
                        print(f"Error enviando mensaje a {uri}: {e}")
                batch = []
        
        elapsed = time.time() - start_time
        return elapsed, messages_sent
    except Exception as e:
        print(f"Error al conectar con servidor Pyro {uri}: {e}")
        return 999, 0  # Retornar tiempo muy alto para indicar error

# Clase para servidor XMLRPC (para iniciar los servidores en el setup)
class XMLRPCInsultServer(threading.Thread):
    def __init__(self, port):
        threading.Thread.__init__(self)
        self.port = port
        self.server = None
        self.running = True
        self.daemon = True
        self.insults = []
        self.subscribers = []
    
    def run(self):
        from xmlrpc.server import SimpleXMLRPCServer
        from xmlrpc.server import SimpleXMLRPCRequestHandler
        
        class RequestHandler(SimpleXMLRPCRequestHandler):
            rpc_paths = ('/RPC2',)
        
        self.server = SimpleXMLRPCServer(
            ('localhost', self.port), 
            requestHandler=RequestHandler, 
            allow_none=True,
            logRequests=False
        )
        
        self.server.register_introspection_functions()
        self.server.register_instance(self)
        
        # Iniciar servidor
        print(f"XMLRPC Server iniciado en puerto {self.port}")
        while self.running:
            self.server.handle_request()
    
    def stop(self):
        self.running = False
        # Conectar para cerrar
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(('localhost', self.port))
        except:
            pass
    
    # Métodos del servicio InsultService
    def add_insult(self, insult):
        if insult not in self.insults:
            self.insults.append(insult)
            return True
        return False
    
    def get_insults(self):
        return self.insults
    
    def get_random_insult(self):
        if self.insults:
            return random.choice(self.insults)
        return "No hay insultos disponibles."
    
    def add_subscriber(self, subscriber_url):
        if subscriber_url not in self.subscribers:
            self.subscribers.append(subscriber_url)
            return True
        return False
    
    def remove_subscriber(self, subscriber_url):
        if subscriber_url in self.subscribers:
            self.subscribers.remove(subscriber_url)
            return True
        return False
    
    def get_subscribers(self):
        return self.subscribers

# Clase para servidor Pyro (para iniciar los servidores en el setup)
@Pyro4.expose
class PyroInsultServer(threading.Thread):
    def __init__(self, port):
        threading.Thread.__init__(self)
        self.port = port
        self.daemon = True
        self.running = True
        self.insults = []
        self.subscribers = []
        self.daemon_obj = None
    
    def run(self):
        # Configuración para Pyro
        Pyro4.config.SERIALIZER = 'pickle'
        Pyro4.config.SERIALIZERS_ACCEPTED.add('pickle')
        
        # Iniciar daemon Pyro
        self.daemon_obj = Pyro4.Daemon(host='localhost', port=self.port)
        uri = self.daemon_obj.register(self, objectId="insult.service")
        
        print(f"Pyro Server iniciado en puerto {self.port}, URI: {uri}")
        
        # Iniciar el requestloop
        self.daemon_obj.requestLoop(loopCondition=lambda: self.running)
    
    def stop(self):
        self.running = False
        if self.daemon_obj:
            self.daemon_obj.shutdown()
    
    # Métodos del servicio InsultService
    def add_insult(self, insult):
        if insult not in self.insults:
            self.insults.append(insult)
            return True
        return False
    
    def get_insults(self):
        return self.insults
    
    def get_random_insult(self):
        if self.insults:
            return random.choice(self.insults)
        return "No hay insultos disponibles."
    
    def add_subscriber(self, subscriber_uri):
        if subscriber_uri not in self.subscribers:
            self.subscribers.append(subscriber_uri)
            return True
        return False
    
    def remove_subscriber(self, subscriber_uri):
        if subscriber_uri in self.subscribers:
            self.subscribers.remove(subscriber_uri)
            return True
        return False
    
    def get_subscribers(self):
        return self.subscribers

# Funciones de setup/cleanup

def setup_rabbitmq():
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel()
    try:
        ch.exchange_delete(exchange='insults_exchange')
    except:
        pass
    # Exchange in-memory (no durable)
    ch.exchange_declare(exchange='insults_exchange', exchange_type='fanout', durable=False)
    for i in range(3):
        try:
            ch.queue_delete(queue=f'insults_queue_{i}')
        except:
            pass
        # Queues non-durable for benchmark
        ch.queue_declare(queue=f'insults_queue_{i}', durable=False)
        ch.queue_bind(exchange='insults_exchange', queue=f'insults_queue_{i}')
    conn.close()


def cleanup_rabbitmq():
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel()
    for i in range(3):
        try:
            ch.queue_delete(queue=f'insults_queue_{i}')
        except:
            pass
    try:
        ch.exchange_delete(exchange='insults_exchange')
    except:
        pass
    conn.close()


def setup_redis():
    client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    for key in client.scan_iter("task_queue*"):
        client.delete(key)
    try:
        client.config_set('timeout', '0')
    except:
        pass


def cleanup_redis():
    client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    for key in client.scan_iter("task_queue*"):
        client.delete(key)

# Variables globales para los servidores XMLRPC y Pyro
xmlrpc_servers = []
pyro_servers = []

def setup_xmlrpc():
    global xmlrpc_servers
    # Cerrar servidores existentes si los hay
    cleanup_xmlrpc()
    
    # Iniciar 3 servidores XMLRPC en puertos 8000-8002
    for i in range(3):
        port = 8000 + i
        server = XMLRPCInsultServer(port)
        xmlrpc_servers.append(server)
        server.start()
        time.sleep(0.5)  # Esperar a que inicie


def cleanup_xmlrpc():
    global xmlrpc_servers
    for server in xmlrpc_servers:
        try:
            server.stop()
        except:
            pass
    xmlrpc_servers = []


def setup_pyro():
    global pyro_servers
    # Cerrar servidores existentes si los hay
    cleanup_pyro()
    
    # Iniciar 3 servidores Pyro en puertos 9090-9092
    for i in range(3):
        port = 9090 + i
        server = PyroInsultServer(port)
        pyro_servers.append(server)
        server.start()
        time.sleep(0.5)  # Esperar a que inicie


def cleanup_pyro():
    global pyro_servers
    for server in pyro_servers:
        try:
            server.stop()
        except:
            pass
    pyro_servers = []

# Función de ejecución de prueba
def run_test(num_nodes, worker_func, queue_name):
    num_requests = TOTAL_REQUESTS // num_nodes
    task = partial(worker_func, queue_name=queue_name, num_requests=num_requests)
    with multiprocessing.Pool(processes=num_nodes) as pool:
        results = pool.map(task, range(num_nodes))
    max_time = max(r[0] for r in results)
    total_msgs = sum(r[1] for r in results)
    print(f"Total messages: {total_msgs}")
    return max_time

# Bloque principal
if __name__ == "__main__":
    nodes = [1, 2, 3]
    rabbit_times = []
    redis_times = []
    xmlrpc_times = []
    pyro_times = []

    try:
        print("Setting up services...")
        setup_rabbitmq()
        setup_redis()
        setup_xmlrpc()
        setup_pyro()
        
        print("Starting performance tests...")
        for n in nodes:
            print(f"\nTesting with {n} nodes...")
            
            print("Testing RabbitMQ...")
            t1 = run_test(n, rabbitmq_worker_insult, 'insults_input')
            rabbit_times.append(t1)
            print(f" RabbitMQ: {t1:.2f}s, throughput: {TOTAL_REQUESTS/t1:.2f} req/s")
            
            print("Testing Redis...")
            t2 = run_test(n, redis_worker_insult, 'task_queue')
            redis_times.append(t2)
            print(f" Redis: {t2:.2f}s, throughput: {TOTAL_REQUESTS/t2:.2f} req/s")
            
            print("Testing XMLRPC...")
            t3 = run_test(n, xmlrpc_worker_insult, None)  # XMLRPC no usa cola específica
            xmlrpc_times.append(t3)
            print(f" XMLRPC: {t3:.2f}s, throughput: {TOTAL_REQUESTS/t3:.2f} req/s")
            
            print("Testing Pyro...")
            t4 = run_test(n, pyro_worker_insult, None)  # Pyro no usa cola específica
            pyro_times.append(t4)
            print(f" Pyro: {t4:.2f}s, throughput: {TOTAL_REQUESTS/t4:.2f} req/s")

        # Velocidades relativas
        speed_rmq = [rabbit_times[0] / t for t in rabbit_times]
        speed_rds = [redis_times[0] / t for t in redis_times]
        speed_xml = [xmlrpc_times[0] / t for t in xmlrpc_times]
        speed_pyro = [pyro_times[0] / t for t in pyro_times]
        
        plt.figure(figsize=(12, 8))
        plt.plot(nodes, speed_rmq, 'o-', label='RabbitMQ', linewidth=2)
        plt.plot(nodes, speed_rds, 's-', label='Redis', linewidth=2)
        plt.plot(nodes, speed_xml, '^-', label='XMLRPC', linewidth=2)
        plt.plot(nodes, speed_pyro, 'v-', label='Pyro', linewidth=2)
        plt.plot(nodes, nodes, '--', label='Ideal', alpha=0.7)
        plt.xlabel('Number of Nodes')
        plt.ylabel('Speedup (relative to single node)')
        plt.title('Static Scaling Performance Analysis')
        plt.legend()
        plt.grid(alpha=0.3)
        plt.savefig('static_scaling_all_technologies.png', dpi=300, bbox_inches='tight')
        
        # Gráfica de tiempo absoluto
        plt.figure(figsize=(12, 8))
        plt.bar(range(len(nodes)), rabbit_times, width=0.2, label='RabbitMQ', align='center')
        plt.bar([x + 0.2 for x in range(len(nodes))], redis_times, width=0.2, label='Redis', align='center')
        plt.bar([x + 0.4 for x in range(len(nodes))], xmlrpc_times, width=0.2, label='XMLRPC', align='center')
        plt.bar([x + 0.6 for x in range(len(nodes))], pyro_times, width=0.2, label='Pyro', align='center')
        plt.xlabel('Number of Nodes')
        plt.ylabel('Time (seconds)')
        plt.title('Execution Time by Technology and Number of Nodes')
        plt.xticks([x + 0.3 for x in range(len(nodes))], nodes)
        plt.legend()
        plt.grid(alpha=0.3, axis='y')
        plt.savefig('execution_time_comparison.png', dpi=300, bbox_inches='tight')
        
        # Imprimir resumen de speedups para facilitar el análisis
        print("\nSpeedup Summary:")
        print(f"Nodes:\t\t{nodes}")
        print(f"RabbitMQ:\t{[f'{s:.2f}x' for s in speed_rmq]}")
        print(f"Redis:\t\t{[f'{s:.2f}x' for s in speed_rds]}")
        print(f"XMLRPC:\t\t{[f'{s:.2f}x' for s in speed_xml]}")
        print(f"Pyro:\t\t{[f'{s:.2f}x' for s in speed_pyro]}")
        
    finally:
        print("\nCleaning up...")
        cleanup_rabbitmq()
        cleanup_redis()
        cleanup_xmlrpc()
        cleanup_pyro()