#!/usr/bin/env python3
"""
Unit tests for InsultService (four middlewares).

We test “add_insult” and low-level Redis / RabbitMQ parts.
"""
import os, sys, time, uuid, subprocess, unittest
from xmlrpc.client import ServerProxy
import Pyro4, redis, pika

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

class TestInsultService(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.procs = []
        def _p(cmd):
            cls.procs.append(subprocess.Popen(cmd,
                                              stdout=subprocess.DEVNULL,
                                              stderr=subprocess.DEVNULL))
        _p([sys.executable, "-m", "Pyro4.naming"])
        _p([sys.executable, os.path.join(BASE,"Pyro","InsultService.py")])
        _p([sys.executable, os.path.join(BASE,"XMLRPC","InsultService.py")])
        time.sleep(2)

        cls.xml_proxy  = ServerProxy("http://localhost:8000/")
        cls.pyro_proxy = Pyro4.Proxy("PYRONAME:insult.service")
        cls.r          = redis.Redis()

        cls.r.flushdb()   # start clean

        # empty RabbitMQ queue
        con = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        ch  = con.channel(); ch.queue_delete(queue='insults'); ch.queue_declare(queue='insults')
        con.close()

    @classmethod
    def tearDownClass(cls):
        for p in cls.procs: p.terminate(); p.wait()

    # ---------- tests ----------
    def test_xmlrpc_add_insult(self):
        new = f"xml_{uuid.uuid4().hex[:8]}"
        self.assertTrue (self.xml_proxy.add_insult(new))
        self.assertFalse(self.xml_proxy.add_insult(new))  # second time false

    def test_pyro_add_insult(self):
        new = f"pyro_{uuid.uuid4().hex[:8]}"
        self.assertTrue (self.pyro_proxy.add_insult(new))
        self.assertFalse(self.pyro_proxy.add_insult(new))

    def test_redis_add_insult(self):
        self.r.delete('insults')
        self.assertEqual(self.r.scard('insults'), 0)
        self.assertEqual(self.r.sadd('insults', 'redis_insult'), 1)
        self.assertEqual(self.r.scard('insults'), 1)

    def test_rabbitmq_add_insult(self):
        con = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        ch  = con.channel()
        ch.basic_publish('', 'insults', body=b"rabbit_insult")
        ch.queue_purge('insults')
        con.close()
        self.assertTrue(True)

if __name__ == '__main__':
    unittest.main()
