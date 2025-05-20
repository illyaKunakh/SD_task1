#!/usr/bin/env python3
"""
Static scaling test (1, 2, 3 nodes) for four techs.

Each worker sends blocking calls.  
We time how long and draw a speed-up graph.

It saves static_scaling.png
"""
import os, sys, time, subprocess, multiprocessing as mp, socket
import pika, redis, matplotlib.pyplot as plt

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(BASE); import insults   # simple list of insults

TOTAL_REQUESTS = 30_000    # total calls per tech
BATCH_SIZE     = 500       # Redis pipeline flush size

# ------ small helper -------------------------------------------------
def wait_port(host, port, t=5.0):
    """Wait until TCP <host:port> is ready."""
    start = time.time()
    while time.time() - start < t:
        try:
            with socket.create_connection((host, port), 1):
                return
        except OSError:
            time.sleep(0.2)
    raise RuntimeError(f"{host}:{port} not ready")

# ------ RabbitMQ / Redis clean-up -----------------------------------
def setup_rabbitmq(n_workers):
    """Create a fan-out exchange and n empty queues."""
    con = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch  = con.channel()

    try:
        ch.exchange_delete(exchange='insults_exchange')
    except pika.exceptions.ChannelClosedByBroker:
        ch = con.channel()     # exchange did not exist

    ch.exchange_declare('insults_exchange', 'fanout', durable=False)

    for i in range(n_workers):
        q = f'insults_queue_{i}'
        ch.queue_delete(queue=q)
        ch.queue_declare(queue=q, durable=False)
        ch.queue_bind(queue=q, exchange='insults_exchange')
    con.close()

def setup_redis():
    """Flush all keys for a fresh test."""
    redis.Redis().flushdb()

# ------ worker code for each tech -----------------------------------
def rabbit_worker(n_msgs, evt):
    con = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch  = con.channel()
    ch.exchange_declare('bench_ex', 'fanout')
    evt.wait()                      # start gun
    body = b'x'*20
    for _ in range(n_msgs):
        ch.basic_publish('bench_ex', '', body,
                         pika.BasicProperties(delivery_mode=1))
    con.close()

def redis_worker(n_msgs, evt):
    r = redis.Redis()
    pipe = r.pipeline(); q = "bench_q"
    evt.wait()
    for i in range(n_msgs):
        pipe.rpush(q, b'x'*20)
        if (i+1) % BATCH_SIZE == 0:
            pipe.execute()
    pipe.execute()

def xmlrpc_worker(n_msgs, evt):
    import xmlrpc.client
    pr = xmlrpc.client.ServerProxy("http://localhost:8000/")
    local = insults.insults
    evt.wait()
    for i in range(n_msgs):
        pr.add_insult(local[i % len(local)])

def pyro_worker(n_msgs, evt):
    import Pyro4
    ns = Pyro4.locateNS(host="localhost", port=9090, broadcast=False)
    pr = Pyro4.Proxy(ns.lookup("insult.service"))
    local = insults.insults
    evt.wait()
    for i in range(n_msgs):
        pr.add_insult(local[i % len(local)])

MAP = {
    'RabbitMQ': rabbit_worker,
    'Redis'   : redis_worker,
    'XML-RPC' : xmlrpc_worker,
    'Pyro'    : pyro_worker,
}

def run(kind, nodes):
    """Start <nodes> workers and time them."""
    per = TOTAL_REQUESTS // nodes
    evt = mp.Event()
    ws  = [mp.Process(target=MAP[kind], args=(per, evt)) for _ in range(nodes)]
    [p.start() for p in ws]
    time.sleep(0.2)      # give workers a moment
    t0 = time.time(); evt.set()   # run
    [p.join() for p in ws]
    return time.time() - t0

# ------ main program -------------------------------------------------
if __name__ == "__main__":
    mp.freeze_support()

    # start NameServer and two services once
    procs = []
    def _p(cmd):
        procs.append(subprocess.Popen(cmd,
                                      stdout=subprocess.DEVNULL,
                                      stderr=subprocess.DEVNULL))
    _p([sys.executable, "-m", "Pyro4.naming", "-n", "localhost", "-p", "9090"])
    wait_port("localhost", 9090)
    _p([sys.executable, os.path.join(BASE, "Pyro",   "InsultService.py")])
    _p([sys.executable, os.path.join(BASE, "XMLRPC", "InsultService.py")])
    wait_port("localhost", 8000)

    nodes = [1, 2, 3]
    times = {k: [] for k in MAP}

    for n in nodes:
        print(f"\n  {n} node(s)")
        setup_rabbitmq(n)
        setup_redis()

        for kind in MAP:
            t = run(kind, n)
            times[kind].append(t)
            print(f"   {kind:8}: {t:.2f}s")

    # speed-up = time with 1 node / time with N nodes
    speed = {k: [times[k][0]/x for x in times[k]] for k in MAP}

    # draw the lines
    plt.figure(figsize=(7,4))
    for k, v in speed.items():
        plt.plot(nodes, v, '-o', label=k)
    plt.plot(nodes, nodes, '--', alpha=.6, label='Ideal')
    plt.xlabel("Nodes"); plt.ylabel("Speed-up")
    plt.legend(); plt.grid(alpha=.3)
    plt.tight_layout(); plt.savefig("static_scaling.png", dpi=300)

    for p in procs: p.terminate(); p.wait()
