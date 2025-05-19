from xmlrpc.server import SimpleXMLRPCServer
import threading
import time

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
        # XMLRPC no soporta broadcasting directo, se simula con polling
        pass

if __name__ == "__main__":
    server = SimpleXMLRPCServer(("localhost", 8000))
    server.register_instance(InsultService())
    print("Servidor XMLRPC InsultService escuchando en puerto 8000...")
    server.serve_forever()