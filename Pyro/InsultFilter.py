import Pyro4

filtered_texts = []

@Pyro4.expose
class InsultFilter:
    def filter_text(self, text):
        insults = ["insult1", "insult2"]
        filtered = "CENSORED" if text.lower() in insults else text
        filtered_texts.append(filtered)
        return filtered

    def get_filtered_texts(self):
        return filtered_texts

if __name__ == "__main__":
    Pyro4.config.REQUIRE_EXPOSE = True
    daemon = Pyro4.Daemon()
    ns = Pyro4.locateNS()
    uri = daemon.register(InsultFilter)
    ns.register("insult.filter", uri)
    print(f"Servicio Pyro InsultFilter listo en: {uri}")
    daemon.requestLoop()