#!/usr/bin/env python3
"""
Prueba funcional mínima para RabbitMQ/MultipleNode/InsultService.py
• Añade insultos 'foo', 'bar'
• Solicita un insulto aleatorio ('RANDOM_INSULT') y comprueba que sea uno de los añadidos
"""

import subprocess, time, sys, json, random
from pathlib import Path

import pika


def main() -> None:
    service_path = Path(__file__).parents[2] / "RabbitMQ" / "MultipleNode" / "InsultService.py"
    proc = subprocess.Popen([sys.executable, str(service_path)],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        time.sleep(2)

        conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        ch   = conn.channel()
        rq   = "request_queue_n"                    # queue for multi-node version
        ch.queue_declare(queue=rq)

        res = ch.queue_declare(queue='', exclusive=True)
        reply_q = res.method.queue

        # 1) Send insults
        for insult in ("foo", "bar"):
            ch.basic_publish(exchange='', routing_key=rq, body=insult)

        # 2) Demand a random insult
        ch.basic_publish(exchange='',
                         routing_key=rq,
                         properties=pika.BasicProperties(reply_to=reply_q),
                         body='RANDOM_INSULT')                  # 'RANDOM_INSULT' = getInsult

        # 3) Obtain the response
        received = None
        for _ in range(10):
            method, header, body = ch.basic_get(reply_q, auto_ack=True)
            if method:
                received = body.decode().strip("[]' ")
                break
            time.sleep(0.5)

        assert received in ("foo", "bar"), f"Unexpected reponse: {received}"
        print("✅ RabbitMQ-MultiNode InsultService OK")

    finally:
        proc.terminate(); proc.wait(timeout=5)
        try: conn.close()
        except Exception: pass


if __name__ == "__main__":
    main()
