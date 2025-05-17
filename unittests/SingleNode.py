import os
import sys
import time
import multiprocessing
import pika
import redis
import Pyro4
import xmlrpc.client
import matplotlib.pyplot as plt

# Añadir la carpeta raíz al path para poder importar filtros
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

from RabbitMQ.InsultFilter import filter_text as rmq_filter_text
from Redis.InsultFilter import filter_text as rds_filter_text

# Worker RabbitMQ InsultService para test de throughput
def rabbitmq_insult_worker(processed):
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel()
    ch.queue_declare(queue='insults', durable=False)
    def callback(ch, method, props, body):
        with processed.get_lock():
            processed.value += 1
        ch.basic_ack(delivery_tag=method.delivery_tag)
    ch.basic_consume(queue='insults', on_message_callback=callback)
    ch.start_consuming()

# Worker RabbitMQ InsultFilter para test de throughput
def rabbitmq_filter_worker(processed):
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel()
    ch.queue_declare(queue='texts', durable=False)
    def callback(ch, method, props, body):
        # aplicar filtro importado
        _ = rmq_filter_text(body.decode())
        with processed.get_lock():
            processed.value += 1
        ch.basic_ack(delivery_tag=method.delivery_tag)
    ch.basic_consume(queue='texts', on_message_callback=callback)
    ch.start_consuming()

# Worker Redis InsultService para test
def redis_insult_worker(processed):
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    while True:
        _, insult = r.blpop('insults_queue')
        with processed.get_lock():
            processed.value += 1

# Worker XMLRPC InsultService para test
def xmlrpc_insult_worker(processed, host='localhost', port=8000):
    proxy = xmlrpc.client.ServerProxy(f"http://{host}:{port}")
    
    # Bucle principal de consumo
    while True:
        try:
            # Esperar a que haya trabajo
            with processed.get_lock():
                if processed.value >= 1000:
                    break
            time.sleep(0.01)
        except Exception as e:
            print(f"Error en worker XMLRPC Insult: {e}")

# Worker Redis InsultFilter para test
def redis_filter_worker(processed):
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    while True:
        _, text = r.blpop('texts_queue')
        _ = rds_filter_text(text)
        with processed.get_lock():
            processed.value += 1

# Worker XMLRPC InsultFilter para test
def xmlrpc_filter_worker(processed, host='localhost', port=8001):
    proxy = xmlrpc.client.ServerProxy(f"http://{host}:{port}")
    
    # Bucle principal de consumo
    while True:
        try:
            # Esperar a que haya trabajo
            with processed.get_lock():
                if processed.value >= 1000:
                    break
            time.sleep(0.01)
        except Exception as e:
            print(f"Error en worker XMLRPC Filter: {e}")

# Worker Pyro InsultService para test
def pyro_insult_worker(processed, uri="PYRO:insult.service@localhost:9090"):
    proxy = Pyro4.Proxy(uri)
    
    # Bucle principal de consumo
    while True:
        try:
            # Esperar a que haya trabajo
            with processed.get_lock():
                if processed.value >= 1000:
                    break
            time.sleep(0.01)
        except Exception as e:
            print(f"Error en worker Pyro Insult: {e}")

# Worker Pyro InsultFilter para test
def pyro_filter_worker(processed, uri="PYRO:filter.service@localhost:9091"):
    proxy = Pyro4.Proxy(uri)
    
    # Bucle principal de consumo
    while True:
        try:
            # Esperar a que haya trabajo
            with processed.get_lock():
                if processed.value >= 1000:
                    break
            time.sleep(0.01)
        except Exception as e:
            print(f"Error en worker Pyro Filter: {e}")

# Test RabbitMQ InsultService
def test_rabbitmq_insult():
    processed = multiprocessing.Value('i', 0)
    p = multiprocessing.Process(target=rabbitmq_insult_worker, args=(processed,))
    p.start()
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel()
    ch.queue_declare(queue='insults', durable=False)
    start = time.time()
    for i in range(1000):
        ch.basic_publish(exchange='', routing_key='insults', body=f"insult{i}".encode())
    while processed.value < 1000:
        time.sleep(0.01)
    elapsed = time.time() - start
    p.terminate()
    conn.close()
    return 1000 / elapsed

# Test RabbitMQ InsultFilter
def test_rabbitmq_filter():
    processed = multiprocessing.Value('i', 0)
    p = multiprocessing.Process(target=rabbitmq_filter_worker, args=(processed,))
    p.start()
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel()
    ch.queue_declare(queue='texts', durable=False)
    start = time.time()
    for i in range(1000):
        ch.basic_publish(exchange='', routing_key='texts', body=f"text{i}".encode())
    while processed.value < 1000:
        time.sleep(0.01)
    elapsed = time.time() - start
    p.terminate()
    conn.close()
    return 1000 / elapsed

# Test Redis InsultService
def test_redis_insult():
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    r.delete('insults_queue')
    processed = multiprocessing.Value('i', 0)
    p = multiprocessing.Process(target=redis_insult_worker, args=(processed,))
    p.start()
    start = time.time()
    for i in range(1000):
        r.rpush('insults_queue', f"insult{i}")
    while processed.value < 1000:
        time.sleep(0.01)
    elapsed = time.time() - start
    p.terminate()
    return 1000 / elapsed

# Test Redis InsultFilter
def test_redis_filter():
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    r.delete('texts_queue')
    processed = multiprocessing.Value('i', 0)
    p = multiprocessing.Process(target=redis_filter_worker, args=(processed,))
    p.start()
    start = time.time()
    for i in range(1000):
        r.rpush('texts_queue', f"text{i}")
    while processed.value < 1000:
        time.sleep(0.01)
    elapsed = time.time() - start
    p.terminate()
    return 1000 / elapsed

# Test XMLRPC InsultService
def test_xmlrpc_insult():
    processed = multiprocessing.Value('i', 0)
    host = 'localhost'
    port = 8000
    
    # Iniciamos un worker en segundo plano (para simular un balanceador de carga)
    p = multiprocessing.Process(target=xmlrpc_insult_worker, args=(processed, host, port))
    p.start()
    
    # Creamos un cliente XMLRPC
    proxy = xmlrpc.client.ServerProxy(f"http://{host}:{port}")
    
    start = time.time()
    for i in range(1000):
        proxy.add_insult(f"insult{i}")
        with processed.get_lock():
            processed.value += 1
    
    elapsed = time.time() - start
    p.terminate()
    
    return 1000 / elapsed

# Test XMLRPC InsultFilter
def test_xmlrpc_filter():
    processed = multiprocessing.Value('i', 0)
    host = 'localhost'
    port = 8001
    
    # Iniciamos un worker en segundo plano (para simular un balanceador de carga)
    p = multiprocessing.Process(target=xmlrpc_filter_worker, args=(processed, host, port))
    p.start()
    
    # Creamos un cliente XMLRPC
    proxy = xmlrpc.client.ServerProxy(f"http://{host}:{port}")
    
    start = time.time()
    for i in range(1000):
        proxy.submit_text(f"text with insult1 and insult2 number {i}")
        with processed.get_lock():
            processed.value += 1
    
    elapsed = time.time() - start
    p.terminate()
    
    return 1000 / elapsed

# Test Pyro InsultService
def test_pyro_insult():
    # Configurar Pyro
    Pyro4.config.SERIALIZER = 'pickle'
    Pyro4.config.SERIALIZERS_ACCEPTED.add('pickle')
    
    processed = multiprocessing.Value('i', 0)
    uri = "PYRO:insult.service@localhost:9090"
    
    # Iniciamos un worker en segundo plano
    p = multiprocessing.Process(target=pyro_insult_worker, args=(processed, uri))
    p.start()
    
    # Creamos un cliente Pyro
    proxy = Pyro4.Proxy(uri)
    
    start = time.time()
    for i in range(1000):
        proxy.add_insult(f"insult{i}")
        with processed.get_lock():
            processed.value += 1
    
    elapsed = time.time() - start
    p.terminate()
    
    return 1000 / elapsed

# Test Pyro InsultFilter
def test_pyro_filter():
    # Configurar Pyro
    Pyro4.config.SERIALIZER = 'pickle'
    Pyro4.config.SERIALIZERS_ACCEPTED.add('pickle')
    
    processed = multiprocessing.Value('i', 0)
    uri = "PYRO:filter.service@localhost:9091"
    
    # Iniciamos un worker en segundo plano
    p = multiprocessing.Process(target=pyro_filter_worker, args=(processed, uri))
    p.start()
    
    # Creamos un cliente Pyro
    proxy = Pyro4.Proxy(uri)
    
    start = time.time()
    for i in range(1000):
        proxy.submit_text(f"text with insult1 and insult2 number {i}")
        with processed.get_lock():
            processed.value += 1
    
    elapsed = time.time() - start
    p.terminate()
    
    return 1000 / elapsed

# Bloque principal
if __name__ == "__main__":
    # Definir todas las pruebas en un diccionario
    test_functions = {
        "RabbitMQ Insult": test_rabbitmq_insult,
        "RabbitMQ Filter": test_rabbitmq_filter,
        "Redis Insult": test_redis_insult,
        "Redis Filter": test_redis_filter,
        "Pyro Insult": test_pyro_insult,
        "Pyro Filter": test_pyro_filter,
        "XMLRPC Insult": test_xmlrpc_insult,
        "XMLRPC Filter": test_xmlrpc_filter
    }
    
    results = {}
    
    # Ejecutar las pruebas
    print("Ejecutando pruebas...")
    for label, test_func in test_functions.items():
        try:
            rate = test_func()
            results[label] = rate
            print(f"{label}: {rate:.2f} req/s")
        except Exception as e:
            results[label] = 0
            print(f"Error en {label}: {e}")
    
    # Extraer datos para las gráficas
    insult_labels = [label for label in results if 'Insult' in label]
    filter_labels = [label for label in results if 'Filter' in label]
    
    insult_rates = [results[label] for label in insult_labels]
    filter_rates = [results[label] for label in filter_labels]
    
    # Crear gráfica con todos los resultados
    plt.figure(figsize=(14, 7))
    x = range(len(insult_labels))
    width = 0.35
    
    plt.bar([i - width/2 for i in x], insult_rates, width, label='InsultService')
    plt.bar([i + width/2 for i in x], filter_rates, width, label='InsultFilter')
    
    plt.xlabel('Tecnología')
    plt.ylabel('Peticiones por segundo')
    plt.title('Comparativa de rendimiento entre tecnologías (Insult vs Filter)')
    plt.xticks(x, [label.split()[0] for label in insult_labels])  # Solo el nombre de la tecnología (sin "Insult" ni "Filter")
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    
    # Guardar la gráfica en un archivo
    plt.savefig('single_node_performance_comparison.png')
    print("Gráfica guardada como 'single_node_performance_comparison.png'")
