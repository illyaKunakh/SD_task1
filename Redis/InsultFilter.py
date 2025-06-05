import redis
import json

class InsultFilter:
    def __init__(self):
        self.client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        self.client_messages_filter = "client_messages_filter"
        self.redis_filtered_list = "redis_filtered_list"

    # 1. Continuously listens for petitions in the client_messages_filter queue
    # 2. For each petition, checks for insults in the redis_insult_list
    # 3. Replaces any found insults with "CENSORED"
    # 4. Pushes the filtered petition to the redis_filtered_list
    # 5. Prints the filtered petition to the console
    def resolve_petition(self):
        while True:
            _, petition = self.client.brpop(self.client_messages_filter, timeout=0)
            insults = self.client.lrange("redis_insult_list", 0, -1)
            for insult in insults:
                if insult in petition:
                    petition = petition.replace(insult, "CENSORED")
            self.client.lpush(self.redis_filtered_list, petition)
            print(f"Filtered: {petition}")

if __name__ == "__main__":
    filter = InsultFilter()
    print("InsultFilter started")
    filter.resolve_petition()