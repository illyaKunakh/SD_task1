import pika
import multiprocessing

filtered = []

def filter_text(text):
    return "CENSORED" if text.lower() in ["insult1", "insult2"] else text

def receive():
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel()
    ch.queue_declare(queue='texts')
    ch.basic_consume(queue='texts', on_message_callback=lambda ch, method, props, body: [filtered.append(filter_text(body.decode())), ch.basic_ack(method.delivery_tag)], auto_ack=False)
    ch.start_consuming()

def list_filtered():
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel()
    ch.queue_declare(queue='texts_list')
    ch.basic_consume(queue='texts_list', on_message_callback=lambda ch, method, props, body: [ch.basic_publish('', props.reply_to, ';'.join(filtered).encode()), ch.basic_ack(method.delivery_tag)], auto_ack=False)
    ch.start_consuming()

if __name__ == "__main__":
    multiprocessing.Process(target=receive).start()
    multiprocessing.Process(target=list_filtered).start()