#!/usr/bin/env python3
import subprocess, time, sys, json
from pathlib import Path

import pika


def main() -> None:
    service_path = Path(__file__).parents[2] / "RabbitMQ" / "SingleNode" / "InsultService.py"
    proc = subprocess.Popen([sys.executable, str(service_path)],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        time.sleep(1)      

        conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        ch   = conn.channel()
        rq   = "request_queue"                       # queue that listens for insults
        ch.queue_declare(queue=rq)                   

        # Temporary reply queue
        res = ch.queue_declare(queue='', exclusive=True)
        reply_q = res.method.queue

        # ----- 1) Add insults
        for insult in ("foo", "bar"):
            ch.basic_publish(exchange='', routing_key=rq, body=insult)

        # ----- 2) Get the list of insults
        ch.basic_publish(exchange='',
                         routing_key=rq,
                         properties=pika.BasicProperties(reply_to=reply_q),
                         body='LIST')                   # 'LIST' = getInsultList

        # ----- 3) Reading the response
        insults = None
        for _ in range(10):                          # max 5 s
            method, header, body = ch.basic_get(reply_q, auto_ack=True)
            if method:
                insults = eval(body.decode())        # response arrives as str(list)
                break
            time.sleep(0.5)

        assert insults is not None,   "No response from service."
        assert "foo" in insults,      "'foo' is NOT on returned list."
        assert "bar" in insults,      "'bar' is not on returned list."
        print("✅ RabbitMQ-SingleNode InsultService OK")

    finally:
        proc.terminate(); proc.wait(timeout=5)
        try: conn.close()
        except Exception: pass


if __name__ == "__main__":
    main()
