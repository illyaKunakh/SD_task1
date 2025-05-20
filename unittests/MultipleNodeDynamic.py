#!/usr/bin/env python3
"""
Dynamic scale test with RabbitMQ.

We change the message rate over one minute.
A small “scaler” adds or kills worker processes
so the queue stays small.

It saves a picture named dynamic_scaling.png.
"""

import multiprocessing as mp, time, math, random, ctypes
import pika, matplotlib.pyplot as plt, signal, sys

HOST   = "localhost"
QUEUE  = "insults_dynamic"

# How long and how big
RUNTIME_SEC   = 60          # run 60 seconds
MAX_WORKERS   = 8           # never more than 8 workers
MIN_WORKERS   = 1           # always at least 1
TARGET_RES_S  = 2           # we want each message handled in 2 s
PROC_RATE     = 330         # one worker can do ~330 msg/s

# ───────────────────────── worker ─────────────────────────
def worker(idx, counter):
    """One consumer. Counts every message it sees."""
    con = pika.BlockingConnection(pika.ConnectionParameters(HOST))
    ch  = con.channel()
    ch.queue_declare(queue=QUEUE, durable=False)

    def cb(ch, m, props, body):
        with counter.get_lock():
            counter.value += 1          # add to shared counter
        ch.basic_ack(m.delivery_tag)

    ch.basic_qos(prefetch_count=200)    # let Rabbit send 200 at once
    ch.basic_consume(queue=QUEUE, on_message_callback=cb)
    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        pass
    finally:
        con.close()

# ─────────────────────── publisher ───────────────────────
def publisher(profile):
    """
    profile = list of (second_limit, messages_per_second).

    We send messages for 60 s.
    After the last limit we keep the last rate.
    """
    con = pika.BlockingConnection(pika.ConnectionParameters(HOST))
    ch  = con.channel()
    ch.queue_declare(queue=QUEUE, durable=False)

    insults = [f"insult{i}" for i in range(20_000)]
    t0 = time.time(); i = 0
    try:
        while True:
            elapsed = time.time() - t0
            if elapsed >= RUNTIME_SEC:
                break

            # choose rate for this second
            for limit, r in profile:
                if elapsed < limit:
                    rate = r
                    break
            else:
                rate = profile[-1][1]   # after last limit

            # send the messages
            for _ in range(rate):
                ch.basic_publish('', QUEUE, insults[i % len(insults)].encode())
                i += 1
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        con.close()

# ───────────────────────── scaler ─────────────────────────
def scaler():
    """Every second decide how many workers we need."""
    con = pika.BlockingConnection(pika.ConnectionParameters(HOST))
    ch  = con.channel()
    ch.queue_declare(queue=QUEUE, durable=False)

    # counter the workers will increment
    processed_counter = mp.Value(ctypes.c_long, 0)

    workers = []           # list of Process objects
    backlog_hist = []      # messages waiting
    thrpt_hist   = []      # messages handled per second
    w_hist       = []      # how many workers we had

    def spawn():
        """Start one new worker."""
        p = mp.Process(target=worker, args=(len(workers), processed_counter),
                       daemon=True)
        p.start()
        workers.append(p)

    # start with MIN_WORKERS
    for _ in range(MIN_WORKERS):
        spawn()

    start = time.time()
    last_processed = 0
    while True:
        now = time.time()
        if now - start >= RUNTIME_SEC:
            break

        # how many messages are in the queue?
        q_now = ch.queue_declare(queue=QUEUE, passive=True).method.message_count

        # how many did we process this last second?
        with processed_counter.get_lock():
            done_now = processed_counter.value
        thrpt = done_now - last_processed
        last_processed = done_now

        # workers we think we need
        need = math.ceil((thrpt + q_now / TARGET_RES_S) / PROC_RATE)
        need = max(MIN_WORKERS, min(MAX_WORKERS, need))

        # scale up or down
        if need > len(workers):
            for _ in range(need - len(workers)):
                spawn()
        elif need < len(workers):
            for _ in range(len(workers) - need):
                workers.pop().terminate()

        # save numbers for the graph
        backlog_hist.append(q_now)
        thrpt_hist.append(thrpt)
        w_hist.append(len(workers))

        time.sleep(1)

    # stop everything
    for p in workers: p.terminate()
    con.close()

    # ─── make a picture ───
    t = range(len(thrpt_hist))
    fig, ax1 = plt.subplots()
    ax1.plot(t, thrpt_hist, label='msg/s done')
    ax1.set_xlabel('seconds')
    ax1.set_ylabel('msg/s')
    ax2 = ax1.twinx()
    ax2.plot(t, w_hist, 'r', label='workers')
    ax2.set_ylabel('workers')
    fig.legend(loc='upper right')
    plt.title('Dynamic Scaling')
    plt.tight_layout()
    plt.savefig('dynamic_scaling.png', dpi=300, bbox_inches='tight')
    print("Generating dynamic_scaling.png")

# ───────────────────────── main ──────────────────────────
if __name__ == "__main__":
    # start with clean queue
    con = pika.BlockingConnection(pika.ConnectionParameters(HOST))
    ch  = con.channel()
    ch.queue_delete(queue=QUEUE)
    ch.queue_declare(queue=QUEUE)
    con.close()

    # rate profile: 0-20 s → 200 msg/s, 20-40 s → 800 msg/s, rest 100 msg/s
    profile = [(20, 200), (40, 800), (RUNTIME_SEC, 100)]
    pub = mp.Process(target=publisher, args=(profile,), daemon=True)
    pub.start()
    scaler()
    pub.terminate()
