import subprocess, sys, time
from pathlib import Path
import pika

# Configuración
ROOT = Path(__file__).resolve().parents[2]
BC_PATH = ROOT / 'RabbitMQ' / 'MultipleNode' / 'InsultBroadcaster.py'
SUB_PATH = ROOT / 'RabbitMQ' / 'Subscriber.py'
N_SUBS    = 3      
TEST_SECS = 15     # sseconds of broadcast

spawn = lambda path: subprocess.Popen([sys.executable, str(path)])

def send(cmd: str):
    """Publica 'start' o 'stop' en broadcast_queue"""
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel(); ch.queue_declare(queue='broadcast_queue')
    ch.basic_publish('', 'broadcast_queue', cmd)
    conn.close()

print('[TEST] Launching broadcaster…')
broad = spawn(BC_PATH)

# Waiting for connection
time.sleep(2)

# Launching n sbscribers
print(f'[TEST] Lanzando {N_SUBS} subscriptores…')
subs = [spawn(SUB_PATH) for _ in range(N_SUBS)]

time.sleep(2)
print('[TEST] → start')
send('start')

time.sleep(TEST_SECS)
print('[TEST] → stop')
send('stop')

# Shutdown 
time.sleep(2)
for p in subs:
    p.terminate(); p.wait()
broad.terminate(); broad.wait()

print('[TEST] Finalizado')
