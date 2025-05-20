from xmlrpc.server import SimpleXMLRPCServer
import threading
import time
import random

insults = set()

class InsultService:
    def add_insult(self, insult):
        if insult not in insults:
            insults.add(insult)
            return True
        return False

    def get_insults(self):
        return list(insults)
    def broadcaster(self):
        # XMLRPC does not support callbacks directly, but we can simulate it
        if insults:
            return random.choice(list(insults))
        return None
        return None

if __name__ == "__main__":
    server = SimpleXMLRPCServer(("localhost", 8000))
    server.register_instance(InsultService())
    print("Servidor XMLRPC InsultService escuchando en puerto 8000...")
    server.serve_forever()
