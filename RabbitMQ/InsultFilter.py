import pika
import threading
import time

def worker():
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel()
    ch.queue_declare(queue='texts', durable=True)
    def callback(ch, method, props, body):
        text = body.decode()
        # Suponiendo que tenemos una lista de insultos
        insults = ["insult1", "insult2"]
        filtered = "CENSORED" if text.lower() in insults else text
        # Almacenar en algún lugar, por ejemplo, imprimir
        print(f"Filtered: {filtered}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    ch.basic_consume(queue='texts', on_message_callback=callback)
    ch.start_consuming()

if __name__ == "__main__":
    threading.Thread(target=worker, daemon=True).start()
    print("RabbitMQ InsultFilter worker iniciado.")
    while True:
        time.sleep(1)