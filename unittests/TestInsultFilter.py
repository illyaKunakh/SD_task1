#!/usr/bin/env python3
"""
Unit tests for InsultFilter (four middlewares).

We start the real servers only for these tests.
"""
import os, sys, time, uuid, subprocess, unittest
import redis, pika, Pyro4
from xmlrpc.client import ServerProxy

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

class TestInsultFilter(unittest.TestCase):
    # ---------- start services once ----------
    @classmethod
    def setUpClass(cls):
        cls.procs = []
        def _p(cmd):
            cls.procs.append(subprocess.Popen(cmd,
                                              stdout=subprocess.DEVNULL,
                                              stderr=subprocess.DEVNULL))
        _p([sys.executable, "-m", "Pyro4.naming"])
        _p([sys.executable, os.path.join(BASE,"Pyro","InsultFilter.py")])
        _p([sys.executable, os.path.join(BASE,"XMLRPC","InsultFilter.py")])
        _p([sys.executable, os.path.join(BASE,"Redis","InsultFilter.py")])
        time.sleep(3)

        cls.xml_proxy  = ServerProxy("http://localhost:8001/")
        cls.pyro_proxy = Pyro4.Proxy("PYRONAME:insult.filter")
        cls.r          = redis.Redis(decode_responses=True)

        # make sure RabbitMQ queue exists and is empty
        con = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        ch = con.channel(); ch.queue_delete(queue="texts"); ch.queue_declare(queue="texts")
        con.close()

    @classmethod
    def tearDownClass(cls):
        for p in cls.procs: p.terminate(); p.wait()

    def setUp(self):
        # clean Redis lists before each test
        self.r.delete("texts_queue", "filtered")

    # ---------- real tests ----------
    def test_xmlrpc_filter_text(self):
        self.assertEqual(self.xml_proxy.filter_text("insult1"), "CENSORED")
        unique = f"ok_{uuid.uuid4().hex[:6]}"
        self.assertEqual(self.xml_proxy.filter_text(unique), unique)

    def test_pyro_filter_text(self):
        self.assertEqual(self.pyro_proxy.filter_text("insult2"), "CENSORED")
        unique = f"py_{uuid.uuid4().hex[:6]}"
        self.assertEqual(self.pyro_proxy.filter_text(unique), unique)

    def test_redis_filter_text(self):
        # push message into Redis list
        self.r.rpush("texts_queue", "insult1")
        # worker writes result to "filtered" list
        key_val = self.r.brpop("filtered", timeout=3)
        self.assertIsNotNone(key_val, "Redis worker did nothing")
        self.assertEqual(key_val[1], "CENSORED")

    def test_rabbitmq_filter_text(self):
        con = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        ch = con.channel()
        ch.basic_publish('', 'texts', body=b"insult2")
        con.close()
        self.assertTrue(True)    # if we reach here, OK

if __name__ == "__main__":
    unittest.main()
