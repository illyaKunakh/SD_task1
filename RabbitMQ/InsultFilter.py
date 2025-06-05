import pika
import redis
import argparse
import time

def parse_args():
    parser = argparse.ArgumentParser(description="Insult Filter for RabbitMQ")
    parser.add_argument('--queue', type=str, default='insult_filter_queue', help='Queue name')
    return parser.parse_args()

# Called every time a message is received
# 1. Decodes the message body
# 2. Checks for insults in the message
# 3. Replaces any found insults with "CENSORED"
# 4. Simulates a processing delay
# 5. Publishes the filtered message back to the reply queue
# 6. Prints the filtered message to the console
def callback(ch, method, properties, body):
    text = body.decode()
    insults = redis_client.lrange("insult_list", 0, -1)
    for insult in insults:
        if insult in text:
            text = text.replace(insult, "CENSORED")
    time.sleep(0.0005)  # Simulate processing delay
    ch.basic_publish(exchange='', routing_key=properties.reply_to, body=text)
    print(f"Filtered: {text}")


if __name__ == "__main__":
    args = parse_args()
    redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=args.queue)

    channel.basic_consume(queue=args.queue, on_message_callback=callback, auto_ack=True)

    print(f"InsultFilter started on queue: {args.queue}")
    channel.start_consuming()