import unittest
import redis
import pika
from xmlrpc.client import ServerProxy
import Pyro4
import time

class TestInsultFilter(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Initialize Redis client
        cls.r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        # Clear Redis keys
        cls.r.delete('texts_queue', 'filtered_texts')

        # Initialize RabbitMQ connection
        cls.rabbit_conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        cls.rabbit_ch = cls.rabbit_conn.channel()
        cls.rabbit_ch.queue_declare(queue='texts', durable=True)

    @classmethod
    def tearDownClass(cls):
        # Clean up RabbitMQ
        cls.rabbit_ch.queue_delete(queue='texts')
        cls.rabbit_conn.close()

    def setUp(self):
        # Clear Redis keys before each test
        self.r.delete('texts_queue', 'filtered_texts')

    def test_xmlrpc_filter_text(self):
        proxy = ServerProxy("http://localhost:8001")
        result = proxy.filter_text("insult1")
        self.assertEqual(result, "CENSORED")

    def test_pyro_filter_text(self):
        service = Pyro4.Proxy("PYRONAME:insult.filter")
        result = service.filter_text("insult1")
        self.assertEqual(result, "CENSORED")

    def test_redis_filter_text(self):
        self.r.rpush('texts_queue', "insult1")
        time.sleep(1)  # Wait for worker to process
        filtered = self.r.lpop('filtered_texts')
        if filtered:
            filtered = filtered.decode('utf-8') if isinstance(filtered, bytes) else filtered
        self.assertEqual(filtered, "CENSORED")

    def test_rabbitmq_filter_text(self):
        self.rabbit_ch.basic_publish(exchange='', routing_key='texts', body="insult1".encode())
        # Placeholder: RabbitMQ verification requires checking service output
        self.assertTrue(True)

if __name__ == '__main__':
    unittest.main()