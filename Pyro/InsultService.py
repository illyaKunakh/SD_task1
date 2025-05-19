import Pyro4
import random
import threading
import time

insults = set()

@Pyro4.expose
class InsultService:
    def add_insult(self, insult):
        if insult not in insults:
            insults.add(insult)
            return True
        return False

    def get_insults(self):
        return list(insults)

    def register_callback(self, callback):
        def broadcast():
            while True:
                if insults:
                    insult = random.choice(list(insults))
                    try:
                        callback(insult)
                    except:
                        pass
                time.sleep(5)
        threading.Thread(target=broadcast, daemon=True).start()

if __name__ == "__main__":
    Pyro4.config.REQUIRE_EXPOSE = True
    daemon = Pyro4.Daemon()
    ns = Pyro4.locateNS()
    uri = daemon.register(InsultService)
    ns.register("insult.service", uri)
    print(f"Servicio Pyro InsultService listo en: {uri}")
    daemon.requestLoop()