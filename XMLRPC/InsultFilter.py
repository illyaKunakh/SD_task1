from xmlrpc.server import SimpleXMLRPCServer
import threading

filtered_texts = []

class InsultFilter:
    def filter_text(self, text):
        # Suponiendo que tenemos una lista de insultos
        insults = ["insult1", "insult2"]
        filtered = "CENSORED" if text.lower() in insults else text
        filtered_texts.append(filtered)
        return filtered

    def get_filtered_texts(self):
        return filtered_texts

if __name__ == "__main__":
    server = SimpleXMLRPCServer(("localhost", 8001))
    server.register_instance(InsultFilter())
    print("Servidor XMLRPC InsultFilter escuchando en puerto 8001...")
    server.serve_forever()