import redis
import json

class InsultFilter:
    
    def __init__(self):
        self.client = redis.Redis(host='localhost', port = 6379, db = 0, decode_responses = True)
        self.instance_id = self.client.incr("insult_service_instance_id")
        self.client_jsons = f"client_jsons{self.instance_id}"
        self.client_messages_filter = "client_messages_filter"
        self.redis_filtered_list = "redis_filtered_list"
    
    # Function to store filter petitions from clients
    def add_petition(self, petition):
        self.client.lpush(self.client_messages_filter, petition)
        print(f"Adding new petition: {petition}")

    # Resolve one petition
    def resolve_petition(self):
        _, petition = self.client.brpop(self.client_messages_filter, timeout=0)
        insults = self.client.lrange("insult_queue", 0, -1)
        for insult in insults:
            if insult in petition:
                petition = petition.replace(insult, "CENSORED")
        self.client.lpush(self.redis_filtered_list, petition)
        print(f"Resolved: {petition}")
    
    # Retrieve all resolutions
    def retrieve_resolutions(self, client):
        print("Retriving all resolutions")
        self.client.lpush(client, *self.get_resolve_queue())

    # Get all resolutions (private)
    def get_resolve_queue(self):
        return self.client.lrange(self.redis_filtered_list, 0, -1)
    
filter = InsultFilter()
print("Waiting for petitions to be filtered")
print(filter.client_jsons)
while True:
    _, raw_data = filter.client.brpop(filter.client_jsons, timeout=0)
    print("Petition recieved")
    petition = json.loads(raw_data)

    operation = petition["operation"]
    data = petition["data"]

    match operation:
        case "Z":
            filter.add_petition(data)
        
        case "O":
            filter.resolve_petition()

        case "V":
            filter.retrieve_resolutions(data)

        case _:
            print("Non exisiting operation")