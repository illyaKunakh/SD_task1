import unittest
from xmlrpc.client import ServerProxy
import Pyro4
import redis
import pika

class TestInsultService(unittest.TestCase):
    def test_xmlrpc_add_insult(self):
        proxy = ServerProxy("http://localhost:8000")
        self.assertTrue(proxy.add_insult("insult1"))
        self.assertFalse(proxy.add_insult("insult1"))

    def test_pyro_add_insult(self):
        service = Pyro4.Proxy("PYRONAME:insult.service")
        self.assertTrue(service.add_insult("insult1"))
        self.assertFalse(service.add_insult("insult1"))

    def test_redis_add_insult(self):
        r = redis.Redis(host='localhost', port=6379, db=0)
        r.sadd('insults', "insult1")
        self.assertEqual(r.scard('insults'), 1)

    def test_rabbitmq_add_insult(self):
        conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        ch = conn.channel()
        ch.queue_declare(queue='insults', durable=True)
        ch.basic_publish(exchange='', routing_key='insults', body="insult1".encode())
        conn.close()

if __name__ == '__main__':
    unittest.main()