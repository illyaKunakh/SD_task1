import subprocess, sys, time
from pathlib import Path
import pika

# Configuración
ROOT = Path(__file__).resolve().parents[2]
SVC_PATH = ROOT / 'RabbitMQ' / 'InsultService.py'
SUB_PATH = ROOT / 'RabbitMQ' / 'Subscriber.py'
N_SUBS    = 3      
TEST_SECS = 15     # seconds of broadcast

# Función para lanzar un proceso de InsultService o Subscriber
spawn = lambda path, *args: subprocess.Popen([sys.executable, str(path)] + list(args))

# Function for sending commands to the RabbitMQ service
# It connects to RabbitMQ, declares the queue, and sends the command.
# The connection is closed after sending the command.
def send(cmd: str):
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel(); ch.queue_declare(queue='request_queue')
    ch.basic_publish('', 'request_queue', cmd)
    conn.close()

print('[TEST] Launching InsultService with broadcast…')
broad = spawn(SVC_PATH, '--broadcast')

# Waiting for connection
time.sleep(1)

# Launching n subscribers
print(f'[TEST] Lanzando {N_SUBS} subscriptores…')
subs = [spawn(SUB_PATH) for _ in range(N_SUBS)]

time.sleep(1)
print('[TEST] → BCAST_START')
send('BCAST_START')

time.sleep(TEST_SECS)
print('[TEST] → BCAST_STOP')
send('BCAST_STOP')

# Shutdown 
time.sleep(1)
for p in subs:
    p.terminate(); p.wait()
broad.terminate(); broad.wait()

print('[TEST] Finalizado')