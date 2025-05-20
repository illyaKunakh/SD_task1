#!/usr/bin/env python3
#   • Stress test of a single node per technology
#   • Saves single_node.png
import os, sys, time, subprocess, multiprocessing as mp
import pika, redis, xmlrpc.client, Pyro4, matplotlib.pyplot as plt

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# -------------------------------------------------------------------------
#                         WORKERS 
# -------------------------------------------------------------------------

def rabbit_consumer(queue, counter):
    con = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch  = con.channel(); ch.queue_declare(queue=queue)
    def cb(ch, m, p, body):
        with counter.get_lock(): counter.value += 1
        ch.basic_ack(m.delivery_tag)
    ch.basic_consume(queue, cb); ch.start_consuming()

def redis_consumer(list_name, counter):
    r = redis.Redis(decode_responses=True)
    while True:
        _, _ = r.blpop(list_name)
        with counter.get_lock(): counter.value += 1

# -------------------------------------------------------------------------
#                         PUBLISHERS
# -------------------------------------------------------------------------

def rabbit_test(queue):
    done = mp.Value('i', 0)
    # assure the queue does not exist
    tmp = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    tmp.channel().queue_delete(queue=queue); tmp.close()

    p = mp.Process(target=rabbit_consumer, args=(queue, done), daemon=True); p.start()

    con = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch  = con.channel(); ch.queue_declare(queue=queue)

    t0 = time.time()
    for i in range(1000):
        ch.basic_publish('', queue, f"m{i}".encode(),
                         pika.BasicProperties(delivery_mode=1))
    while done.value < 1000: time.sleep(0.01)
    elapsed = time.time() - t0
    p.terminate(); con.close()
    return 1000/elapsed

def redis_test(key):
    r = redis.Redis(decode_responses=True); r.delete(key)
    done = mp.Value('i', 0)
    p = mp.Process(target=redis_consumer, args=(key, done), daemon=True); p.start()

    t0 = time.time()
    for i in range(1000):
        r.rpush(key, f"m{i}")
    while done.value < 1000: time.sleep(0.01)
    elapsed = time.time() - t0
    p.terminate(); return 1000/elapsed

def xmlrpc_test(proxy, method):
    t0 = time.time()
    for i in range(1000): getattr(proxy, method)(f"x{i}")
    return 1000/(time.time()-t0)

def pyro_test(proxy, method):
    t0 = time.time()
    for i in range(1000): getattr(proxy, method)(f"x{i}")
    return 1000/(time.time()-t0)

# -------------------------------------------------------------------------
#                          MAIN  
# -------------------------------------------------------------------------
if __name__ == "__main__":
    mp.freeze_support()     

    # launch services
    procs = []
    def _launch(cmd):
        procs.append(subprocess.Popen(cmd, stdout=subprocess.DEVNULL,
                                      stderr=subprocess.DEVNULL))
    _launch([sys.executable, "-m", "Pyro4.naming"])
    _launch([sys.executable, os.path.join(BASE,"Pyro","InsultService.py")])
    _launch([sys.executable, os.path.join(BASE,"Pyro","InsultFilter.py")])
    _launch([sys.executable, os.path.join(BASE,"XMLRPC","InsultService.py")])
    _launch([sys.executable, os.path.join(BASE,"XMLRPC","InsultFilter.py")])
    time.sleep(3)   # give time to listen

    # proxies
    xml_ins  = xmlrpc.client.ServerProxy("http://localhost:8000/")
    xml_filt = xmlrpc.client.ServerProxy("http://localhost:8001/")
    ns       = Pyro4.locateNS(host="localhost")
    pyro_ins  = Pyro4.Proxy(ns.lookup("insult.service"))
    pyro_filt = Pyro4.Proxy(ns.lookup("insult.filter"))

    # benchmarks
    results = {
        "RabbitMQ Insult": rabbit_test("insults"),
        "RabbitMQ Filter": rabbit_test("texts"),
        "Redis Insult"   : redis_test("ins_q"),
        "Redis Filter"   : redis_test("txt_q"),
        "XML-RPC Insult" : xmlrpc_test(xml_ins,  "add_insult"),
        "XML-RPC Filter" : xmlrpc_test(xml_filt, "filter_text"),
        "Pyro Insult"    : pyro_test(pyro_ins,  "add_insult"),
        "Pyro Filter"    : pyro_test(pyro_filt, "filter_text"),
    }

    for k,v in results.items():
        print(f"{k:16}: {v:7.1f} req/s")

    plt.bar(results.keys(), results.values())
    plt.xticks(rotation=45, ha="right"); plt.ylabel("req/s")
    plt.tight_layout(); plt.savefig("single_node.png", dpi=300)

    # --- cleaning ---
    for p in procs: p.terminate()
    for p in procs: p.wait()
