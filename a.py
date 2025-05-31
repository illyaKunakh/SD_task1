Tengo un proyecto con esta estructura (ver imagen). 
/RabbitMQ/MultipleNode/InsultBroadcaster.py: "import pika
import multiprocessing
import redis
import random
import time

def startBroadcast():
    global broadcast_active
    if broadcast_active is None:
        broadcast_active = multiprocessing.Process(target=notify)
        broadcast_active.start()
        print(" Started Broadcast")
    else:
        print (" Broadcast already active")

# Send random insults every 5 seconds to subscribers
def notify():
    insults = ['insult1', 'insult2', 'insult3', 'insult4', 'insult5', 'insult6', 'insult7', 'insult8', 'insult9']
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    while True:
        insult = random.choice(insults)
        channel.basic_publish(exchange='notify_subs', routing_key='', body=insult)
        print(f" Broadcaster sent: '{insult}'")
        time.sleep(5)

def stopBroadcast():
    global broadcast_active
    if broadcast_active is not None:
        broadcast_active.terminate()
        broadcast_active.join()
        broadcast_active = None
        print(" Stopping broadcast ")
    else:
        print (" No Broadcast active ")

def callback(ch, method, properties, body):
    value = body.decode()
    print(f" Received: {value}")
    if value == 'start':
        startBroadcast()
    elif value == 'stop':
        stopBroadcast()

if __name__ == "__main__":
    broadcast_active = None

    # Connect to RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare("broadcast_queue")

    # Declare a fanout exchange
    channel.exchange_declare(exchange='notify_subs', exchange_type='fanout')

    channel.basic_consume(queue="broadcast_queue", on_message_callback=callback, auto_ack=True)

    channel.start_consuming()", 
/RabbitMQ/MultipleNode/InsultService.py: "import pika
import multiprocessing
import random
import time
import redis

def startBroadcast():
    channel.basic_publish(exchange='', routing_key='broadcast_queue', body='start')

def stopBroadcast():
    channel.basic_publish(exchange='', routing_key='broadcast_queue', body='stop')

def storeInsult(insult):
    insults = client_redis.lrange(insult_list, 0, -1)
    if insult not in insults:
        client_redis.lpush(insult_list, insult)
        print(f"Saving: {insult}")
    else:
        print(f"{insult} is already in the list")

def getInsult(ch, method, properties, body):
    insults = client_redis.lrange(insult_list, 0, -1)
    response = random.choice(insults)
    print(f"Sent {response}")
    
    # Return response to client
    ch.basic_publish(
        exchange='',
        routing_key=properties.reply_to,
        body=str(response),
    )

# Define the callback function
def callback(ch, method, properties, body):
    value = body.decode()
    print(f" [x] Received {value}")
    if value == 'RANDOM_INSULT':
        getInsult(ch, method, properties, body)
    elif value == 'BCAST_START':
        startBroadcast()
    elif value == 'BCAST_STOP':
        stopBroadcast()
    else:
        storeInsult(value)

if __name__ == "__main__":
    insult_list = "insult_list"

    # Connect to RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    client_redis = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

    # Declare a queue (ensure it exists)
    channel.queue_declare(queue='request_queue_n')

    # Declare a queue
    channel.queue_declare(queue='broadcast_queue')

    # Consume messages
    channel.basic_consume(queue='request_queue_n', on_message_callback=callback, auto_ack=True)

    print("Consumer")
    print(' [*] Waiting for messages. To exit, press CTRL+C')
    channel.start_consuming()", 
/RabbitMQ/SingleNode/InsultService.py: "import pika
import multiprocessing
import random
import time

def storeInsult(insult):
    if insult not in insult_list:
        insult_list.append(insult)
        print(f"Save insult: {insult}")
    else:
        print(f"Insult ({insult}) is already in the list")

def getInsult(ch, method, properties, body):
    response = random.choice(insult_list)
    print(f"Send {response}")
    
    # Answer to the client
    ch.basic_publish(exchange='', routing_key=properties.reply_to, body=str(response),)

def notify(list):
    print(f"Notify subscribers...")
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    while True:
        insult = random.choice(list)
        channel.basic_publish(exchange='notify_subs', routing_key='', body=insult)
        print(f" [x] Send '{insult}'")
        time.sleep(5)

def startBroadcast():
    global broadcast_active
    if broadcast_active is None:
        broadcast_active = multiprocessing.Process(target=notify, args=(insult_list,))
        broadcast_active.start()
        print ("Broadcast Actived")
    else:
        print ("Broadcast is also Actived")

def stopBroadcast():
    global broadcast_active
    if broadcast_active is not None:
        broadcast_active.terminate()
        broadcast_active.join()
        broadcast_active = None
        print ("Broadcast Desactived")
    else:
        print ("Broadcast is also Desactived")

def getInsultList(ch, method, properties, body):
    print(f"Send {insult_list}")
    
    # Return response to client
    ch.basic_publish(
        exchange='',
        routing_key=properties.reply_to,
        body=str(insult_list),
    )

# Define the callback function
def callback(ch, method, properties, body):
    value = body.decode()
    print(f" [x] Received {value}")
    if value == 'RANDOM_INSULT':
        getInsult(ch, method, properties, body)
    elif value == 'BCAST_START':
        startBroadcast()
    elif value == 'BCAST_STOP':
        stopBroadcast()
    elif value == 'X':
        getInsultList(ch, method, properties, body)
    else:
        storeInsult(value)

if __name__ == "__main__":
    insult_list = []
    broadcast_active = None

    # Connect to RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declare a queue (ensure it exists)
    channel.queue_declare(queue='request_queue')

    # Declare a fanout exchange
    channel.exchange_declare(exchange='notify_subs', exchange_type='fanout')

    # Consume messages
    channel.basic_consume(queue='request_queue', on_message_callback=callback, auto_ack=True)

    print(' Waiting for messages')
    channel.start_consuming()",
/RabbitMQ/InsultFilter.py: "import pika
import argparse
import time


insults = ['insult1', 'insult2', 'insult3', 'insult4', 'insult5', 'insult6', 'insult7', 'insult8', 'insult9']

# Connect to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
# Parse command line arguments
parser = argparse.ArgumentParser()

# Add an argument for the queue name
parser.add_argument('--queue', type=str, help='Name of the RabbitMQ queue to consume from')
args = parser.parse_args()  
if args.queue is None:
    queue_name = 'insult_filter_queue'
else:
    queue_name = args.queue

# Declare new rabbit queue
channel.queue_declare(queue=queue_name)

def callback(ch, method, properties, body):
    text = body.decode()
    for insult in insults:
            if insult in text:
                text = text.replace(insult, "CENSORED")
    time.sleep(0.0005)

    # Answer to the client
    ch.basic_publish(exchange='', routing_key=properties.reply_to, body=str(text),)

# Consume messages from the queue
channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

print(f'Consumer: {queue_name}')
print(' Waiting for messages')
channel.start_consuming()",
/RabbitMQ/Subscriber.py: "import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Exchange declaration
channel.exchange_declare(exchange='notify_subs', exchange_type='fanout')

# New queue declaration, autodelete when user disconnects
result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue

# Bind the queue to the exchange
channel.queue_bind(exchange='notify_subs', queue=queue_name)

print("Subscriber")
print(' Waiting messages')

def callback(ch, methos, property, body):
    print(f" Received {body.decode()}")

channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
channel.start_consuming()",
/Redis/MultipleNode/InsultFilter.py: "import redis
import json

class InsultFilter:
    
    def __init__(self):
        self.client = redis.Redis(host='localhost', port = 6379, db = 0, decode_responses = True)
        self.instance_id = self.client.incr("insult_service_instance_id")
        self.client_jsons = f"client_jsons{self.instance_id}"
        self.client_messages_filter = "client_messages_filter"
        self.redis_filtered_list = "redis_filtered_list"
    
    # Function to store filter petitions from clients
    def add_petition(self, petition):
        self.client.lpush(self.client_messages_filter, petition)
        print(f"Adding new petition: {petition}")

    # Resolve one petition
    def resolve_petition(self):
        _, petition = self.client.brpop(self.client_messages_filter, timeout=0)
        insults = self.client.lrange("insult_queue", 0, -1)
        for insult in insults:
            if insult in petition:
                petition = petition.replace(insult, "CENSORED")
        self.client.lpush(self.redis_filtered_list, petition)
        print(f"Resolved: {petition}")
    
    # Retrieve all resolutions
    def retrieve_resolutions(self, client):
        print("Retriving all resolutions")
        self.client.lpush(client, *self.get_resolve_queue())

    # Get all resolutions (private)
    def get_resolve_queue(self):
        return self.client.lrange(self.redis_filtered_list, 0, -1)
    
filter = InsultFilter()
print("Waiting for petitions to be filtered")
print(filter.client_jsons)
while True:
    _, raw_data = filter.client.brpop(filter.client_jsons, timeout=0)
    print("Petition recieved")
    petition = json.loads(raw_data)

    operation = petition["operation"]
    data = petition["data"]

    match operation:
        case "SUBMIT_TEXT":
            filter.add_petition(data)
        
        case "PROCESS_ONE":
            filter.resolve_petition()

        case "GET_RESULTS":
            filter.retrieve_resolutions(data)

        case _:
            print("Non exisiting operation")", 
/Redis/MultipleNode/InsultService.py: "import redis
import random
import time
import json
from multiprocessing import Process, Manager, Event


class InsultService:
    
    def __init__(self):
        self.client = redis.Redis(host='localhost', port = 6379, db = 0, decode_responses = True)
        self.instance_id = self.client.incr("insult_service_instance_id")
        self.redis_insult_list = "redis_insult_list"              
        self.broadcast_channel = "broadcast_channel"           
        self.client_messages_service = f"client_messages_service{self.instance_id}"        
        self.process = None

    def add_insult(self, insult):      
        if insult not in self.get_insults():                   
            self.client.lpush(self.redis_insult_list, insult)        
        else:
            pass

    def remove_insult(self, insult):
        if insult in self.get_insults():
            self.client.lrem(self.redis_insult_list, 0, insult)
            print(f"Removing {insult}")
        else:
            print(f"The {insult} has already been removed")

    # Retrieve all insults stored in redis
    def retrieve_insults(self, client):
        print("Retrieving all insults on redis")
        self.client.lpush(client, *self.get_insults())

    def random_insult(self):
        return random.choice(self.get_insults())
    
    # One random insult is sent to each master subscriber
    def random_insult(self, stop):
        insult_list = self.get_insults()
        while not stop.is_set():
            if insult_list:
                insult = self.random_insult()
                print(f"Publishing {insult}")
                self.client.publish(self.broadcast_channel, insult)
                time.sleep(5)
    
    # We define a function that returns all insults that are stored in our redis db 
    def get_insults(self):
        return self.client.lrange(self.redis_insult_list, 0, -1)
        
service = InsultService()
service.client.ltrim(service.client_messages_service, 1, 0)
print(service.client_messages_service)
while True:
    _, raw_data = service.client.brpop(service.client_messages_service, timeout=0)
    petition = json.loads(raw_data)

    operation = petition["operation"]
    data = petition["data"]

    match operation:
        case "ADD_INSULT":
            service.add_insult(data)
        
        case "LIST":
            service.retrieve_insults(data)

        case "BCAST_START":
            print("Activating broadcast")
            with Manager() as manager:
                stop = Event()
                list = manager.list(service.get_insults())
                service.process = Process(target=service.random_insult, args=(stop,))
                service.process.start()
        
        case "BCAST_STOP":
            if service.process is not None:
                stop.set()
                service.process.join()
                print("Stopped broadcast")
            else:
                print("No broadcast to stop")

        case _:
            pass
    ",
/Redis/SingleNode/InsultFilter.py: "import redis
import json

class InsultFilter:
    
    def __init__(self):
        self.client = redis.Redis(host='localhost', port = 6379, db = 0, decode_responses = True)
        self.client_jsons = f"client_jsons"
        self.client_messages_filter = "client_messages_filter"
        self.redis_filtered_list = "redis_filtered_list"
    
    def add_petition(self, petition):
        self.client.lpush(self.client_messages_filter, petition)
        print(f"Adding new petition: {petition}")

    def resolve_petition(self):
        _, petition = self.client.brpop(self.client_messages_filter, timeout=0)
        insults = self.client.lrange("insult_queue", 0, -1)
        for insult in insults:
            if insult in petition:
                petition = petition.replace(insult, "CENSORED")
        self.client.lpush(self.redis_filtered_list, petition)
        print(f"Resolved: {petition}")

    def retrieve_resolutions(self, client):
        self.client.lpush(client, *self.get_resolve_queue())

    # Get all resolutions (private)
    def get_resolve_queue(self):
        return self.client.lrange(self.redis_filtered_list, 0, -1)
    
filter = InsultFilter()
print("Waiting for petitions to be filtered")
while True:
    _, raw_data = filter.client.brpop(filter.client_jsons, timeout=0)
    print("Petition recieved")
    petition = json.loads(raw_data)

    operation = petition["operation"]
    data = petition["data"]

    match operation:
        case "SUBMIT_TEXT":
            filter.add_petition(data)
        
        case "PROCESS_ONE":
            filter.resolve_petition()

        case "GET_RESULTS":
            filter.retrieve_resolutions(data)

        case _:
            print("Non exisiting operation")", 
/Redis/SingleNode/InsultService.py: "import redis
import random
import time
import json
from multiprocessing import Process, Manager, Event


class InsultService:
    
    def __init__(self):
        self.client = redis.Redis(host='localhost', port = 6379, db = 0, decode_responses = True)
        self.redis_insult_list = "redis_insult_list"              
        self.broadcast_queue = "broadcast_queue"           
        self.client_messages_service = f"client_messages_service"        
        self.process = None

    def add_insult(self, insult):      
        if insult not in self.get_insults():                   
            self.client.lpush(self.redis_insult_list, insult)         
        else:
            pass     

    def remove_insult(self, insult):
        if insult in self.get_insults():
            self.client.lrem(self.redis_insult_list, 0, insult)
            print(f"Removing {insult}")
        else:
            print(f"The {insult} has already been removed")
    
    def retrieve_insults(self, client):
        print("Retrieving all insults on redis")
        self.client.lpush(client, *self.get_insults())

    def random_insult(self):
        return random.choice(self.get_insults())
    
    # Send random insults to each master subscriber
    def random_events(self, stop):
        insult_list = self.get_insults()
        while not stop.is_set():
            if insult_list:
                insult = self.random_insult()
                print(f"Publishing {insult}")
                self.client.publish(self.broadcast_queue, insult)
                time.sleep(5)
    
    # Get all insults stored in redis
    def get_insults(self):
        return self.client.lrange(self.redis_insult_list, 0, -1)
        
service = InsultService()
service.client.ltrim(service.client_messages_service, 1, 0)
print(service.client_messages_service)
print("Waiting for petitions...")
while True:
    _, raw_data = service.client.brpop(service.client_messages_service, timeout=0)
    petition = json.loads(raw_data)

    operation = petition["operation"]
    data = petition["data"]

    match operation:
        case "ADD_INSULT":
            service.add_insult(data)
        
        case "LIST":
            service.retrieve_insults(data)

        case "BCAST_START":
            print("Activating broadcast")
            with Manager() as manager:
                stop = Event()
                list = manager.list(service.get_insults())
                service.process = Process(target=service.random_events, args=(stop,))
                service.process.start()
        
        case "BCAST_STOP":
            if service.process is not None:
                stop.set()
                service.process.join()
                print("Stopping broadcast")
            else:
                print("Broadcast not active")

        case _:
            pass
    ", 
/unittests/RabbitMQ/Speedup.py: "import pika
import multiprocessing
import time
import matplotlib.pyplot as plt
import subprocess
from pathlib import Path
import random

# We need active (active in order):
# Any
class SpeedupTestService:
    def __init__(self):
        self.number_process = 8
        self.consumer_rate = []
        self.requests = 10000
        self.path_worker = Path(__file__).parent.parent.parent/'RabbitMQ'/'InsultFilter.py'

    def send_insult(self, requests):
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()

        # Declare response queue
        response = channel.queue_declare(queue='')
        response_queue = response.method.queue
        queue = 'insult_filter_queue'

        for _ in range(requests):
            channel.basic_publish(exchange='', 
                                routing_key=queue,
                                properties=pika.BasicProperties(reply_to=response_queue),
                                body='A')
        count = 0
        while True:
            method_frame, header_frame, body = channel.basic_get(queue=response_queue, auto_ack=True)
            if method_frame:
                count += 1
            if count >= requests: break
        connection.close()

    def run_test(self, request, num_servers):
        print(f"Test-> {num_servers+1} servers: {request} requests")
        procs = []
        
        for _ in range(self.number_process):
            p = multiprocessing.Process(target=self.send_insult, args=(request,))
            procs.append(p)

        start = time.time()
        for proc in procs:
            proc.start()
        for proc in procs:
            proc.join()
        fin = time.time()

        all_time = fin - start
        #self.time_stamp(time)
        consum = self.number_process*request / all_time
        self.consumer_rate.append(consum)
        print(f"  Total Time: {all_time}")
        print(f"  Consumer: {consum}")

    def do_tests(self):
        procs = []
        #queues = list(self.filter_queues)
        for servers in range(3):
            # queue = queues.pop(0)
            # print(queue)
            proc = subprocess.Popen(['python3', self.path_worker],#@, '--queue', queue]
                                    stdout = subprocess.DEVNULL)
            procs.append(proc)
            time.sleep(2)
            self.run_test(self.requests, servers)
        for proc in procs:
            proc.terminate()
            proc.wait()
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()

if __name__ == '__main__':
    test = SpeedupTestService()
    test.do_tests()

    plt.figure(figsize=(8, 4))
    num_servers = [1,2,3]
    plt.plot(num_servers, num_servers, 'r-o', label='Estimated')
    speed_up = [val/test.consumer_rate[0] for val in test.consumer_rate]
    #plt.plot(num_servers, test.message_process, 'b-', label='Real')
    plt.plot(num_servers, speed_up, 'b-o', label='Real')
    plt.xlabel('(InsultFilter) Nodes')
    plt.ylabel('Speedup')
    plt.legend()
    plt.grid(True)
    plt.title("SpeedUp RabbitMQ")
    plt.show()", 
/unittests/RabbitMQ/Stress.py: "import pika
import multiprocessing
import time
import matplotlib.pyplot as plt
import subprocess
import sys
from pathlib import Path

class StressTestService:
    def __init__(self):
        self.number_process = 8
        self.consumer_rate = []
        self.requests = [1000, 2000, 5000, 10000]

    def send_insult(self, requests, i):
        # Conectar a RabbitMQ
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()

        # Aseguramos la cola de filtros
        channel.queue_declare(queue='insult_filter_queue')

        # Creamos cola temporal para respuestas
        response = channel.queue_declare(queue='', exclusive=True)
        response_queue = response.method.queue

        # Publicamos solicitudes
        for _ in range(requests):
            channel.basic_publish(
                exchange='',
                routing_key='insult_filter_queue',
                properties=pika.BasicProperties(reply_to=response_queue),
                body='A'
            )

        # Recibimos todas las respuestas
        count = 0
        while count < requests:
            method, header, body = channel.basic_get(queue=response_queue, auto_ack=True)
            if method:
                count += 1

        connection.close()

    def run_test(self, request):
        print(f"Test → {request} peticiones")
        procs = []
        for i in range(self.number_process):
            p = multiprocessing.Process(target=self.send_insult, args=(request, i))
            procs.append(p)

        start = time.time()
        for p in procs: p.start()
        for p in procs: p.join()
        elapsed = time.time() - start

        rate = self.number_process * request / elapsed
        self.consumer_rate.append(rate)
        print(f"  Tiempo total: {elapsed:.2f}s — Throughput: {rate:.2f} msg/s")

    def do_tests(self):
        # Ejecuta cada escenario de carga
        for req in self.requests:
            self.run_test(req)

        # Limpiamos la cola para dejarla lista
        conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        ch = conn.channel()
        ch.queue_purge(queue='insult_filter_queue')
        conn.close()

if __name__ == '__main__':
    # 1) Arrancar el filtro en background
    filter_script = Path(__file__).parent.parent / ".." / 'RabbitMQ' / 'InsultFilter.py'
    if not filter_script.exists():
        print(f"ERROR: no encontrado {filter_script}", file=sys.stderr)
        sys.exit(1)

    filter_proc = subprocess.Popen(
        [sys.executable, str(filter_script)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    time.sleep(2)  # esperamos a que el filtro esté listo

    # 2) Ejecutar el test
    test = StressTestService()
    test.do_tests()

    # 3) Mostrar gráfico
    total_requests = [r * test.number_process for r in test.requests]
    plt.figure(figsize=(8, 4))
    plt.plot(total_requests, test.consumer_rate, 'b-o', label='Messages/s')
    plt.xlabel('Total Requests')
    plt.ylabel('Throughput (msg/s)')
    plt.title('Stress Test RabbitMQ')
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.show()

    # 4) Detener el filtro
    filter_proc.terminate()
    filter_proc.wait()
", 
/unittests/Redis/Speedup.py: "import redis
import time
import multiprocessing
import subprocess
import sys
from pathlib import Path
import json
import random
from itertools import cycle

# Chose depending on cpu cores number
n_producers = 8


def initialize_insults():
    client.ltrim("redis_insult_list", 1, 0)
    for insult in insult_list:
        petition = {
            "operation": "Z",
            "data": insult
        }
        client.lpush(service_queues[0], json.dumps(petition))

def petition_queues(nodes):
    global service_queues
    service_queues = []
    for i in range(nodes):
        service_queues.append("client_messages_service" + str(i + 1))
    print("Service queues:", service_queues)

# Spam de peticiones vacías
def spam__void_petitions(number_petitions):
    service_rr = cycle(service_queues)
    petition = {
        "operation": "Y",
        "data": ""
    }
    for _ in range(number_petitions):
        client.lpush(next(service_rr), json.dumps(petition))
        client.incr("number_pushes")

def run_tests(number_petitions, number_process):
    processes = []
    start = time.time()

    for _ in range(number_process):
        p = multiprocessing.Process(target=spam__void_petitions, args=(number_petitions,))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    # Esperamos a que todas las pushes hayan sido contabilizadas
    while int(client.get("number_pushes")) < number_petitions * n_producers:
        time.sleep(0.01)

    elapsed = time.time() - start
    rate = (n_producers * number_petitions) / elapsed
    print(f"  Time: {elapsed:.2f}s — Rate: {rate:.2f} msg/s")
    return elapsed

if __name__ == "__main__":
    # Conexión a Redis
    client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    client.set("insult_service_instance_id", 0)

    # Ruta al script InsultFilter.py
    filter_script = Path(__file__).parent.parent / ".." / "Redis" / "MultipleNode" / "InsultFilter.py"
    if not filter_script.exists():
        print(f"ERROR: no he encontrado {filter_script}", file=sys.stderr)
        sys.exit(1)

    # Parámetros de test
    insult_list = ['insult1', 'insult2', 'insult3', 'insult4', 'insult5', 'insult6', 'insult7', 'insult8', 'insult9']
    number_petitions = 100000
    nodes = 3

    # Limpiamos colas previas
    for i in range(nodes):
        client.delete(f"client_messages_service{i + 1}")

    results = {}

    # Para 1, 2 y 3 instancias de filtro
    for service in range(nodes):
        # Arrancamos (service+1) instancias de InsultFilter.py
        procs = []
        for _ in range(service + 1):
            p = subprocess.Popen(
                [sys.executable, str(filter_script)],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
            procs.append(p)
            time.sleep(1)  # Dejamos que arranque

        # Preparar colas y datos
        petition_queues(service + 1)
        initialize_insults()
        client.set("number_pushes", 0)
        print(f"\n=== Test con {service + 1} nodo(s) y {number_petitions} peticiones ===")
        elapsed = run_tests(number_petitions, n_producers)
        results[service] = elapsed

        # Detenemos los filtros
        for p in procs:
            p.terminate()
            p.wait()

    # Cálculo de speedup y gráfico
    import matplotlib.pyplot as plt

    speedup = [1] + [results[0] / results[i] for i in range(1, nodes)]
    workers = [1, 2, 3]

    plt.figure(figsize=(8, 4))
    plt.plot(workers, workers, 'r--', label='Ideal')
    plt.plot(workers, speedup, 'b-o', label='Medido')
    plt.xlabel('(InsultFilter) Nodes')
    plt.ylabel('Speedup')
    plt.title('SpeedUp Redis')
    plt.legend()
    plt.grid(True)
    plt.show()
", 
/unittests/Redis/Stress.py: "import redis
import time
import multiprocessing
import subprocess
import sys
from pathlib import Path
import json
import random
from itertools import cycle
import matplotlib.pyplot as plt

# Número máximo de procesos concurrentes (según CPU)
max_cpu = 8

# Peticiones a probar
number_petitions = [1000, 2000, 5000, 10000, 50000, 100000, 500000]

def spam__void_petitions(number_petitions):
    petition = {
        "operation": "Y",
        "data": ""
    }
    for _ in range(number_petitions):
        client.lpush(service_queue, json.dumps(petition))
        client.incr("number_pushes")

def run_tests(number_petitions, number_process):
    processes = []
    start = time.time()

    for _ in range(number_process):
        p = multiprocessing.Process(target=spam__void_petitions, args=(number_petitions,))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    while int(client.get("number_pushes")) < number_petitions * number_process:
        time.sleep(0.01)

    elapsed = time.time() - start
    rate = number_petitions * number_process / elapsed
    print(f"  {number_petitions*number_process} msgs en {elapsed:.2f}s → {rate:.2f} msg/s")
    return rate

if __name__ == "__main__":
    # Conexión a Redis
    client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    service_queue = "client_messages_service"
    client.delete("client_messages_service")      # limpiamos
    client.delete("number_pushes")

    # Arrancamos 1 instancia de InsultFilter.py
    filter_script = Path(__file__).parent.parent / ".." / "Redis" / "MultipleNode" / "InsultFilter.py"
    if not filter_script.exists():
        print(f"ERROR: no he encontrado {filter_script}", file=sys.stderr)
        sys.exit(1)

    proc = subprocess.Popen(
        [sys.executable, str(filter_script)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    time.sleep(2)  # esperamos que el filtro esté listo

    # Ejecutamos pruebas de estrés
    rates = []
    for req in number_petitions:
        client.set("number_pushes", 0)
        print(f"\n--- Stress test con {req} peticiones ---")
        rate = run_tests(req, max_cpu)
        rates.append(rate)

    # Gráfico de resultados
    plt.figure(figsize=(8, 4))
    plt.plot(number_petitions, rates, 'b-o', label='msg/s')
    plt.xlabel('Total de peticiones')
    plt.ylabel('Mensajes procesados por segundo')
    plt.title('Stress Test Redis')
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.show()

    # Cerramos el filtro
    proc.terminate()
    proc.wait()
".