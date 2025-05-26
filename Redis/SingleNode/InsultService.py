import redis
import random
import time
import json
from multiprocessing import Process, Manager, Event


class InsultService:
    
    def __init__(self):
        self.client = redis.Redis(host='localhost', port = 6379, db = 0, decode_responses = True)
        self.redis_insult_list = "redis_insult_list"              
        self.broadcast_queue = "broadcast_queue"           
        self.client_messages_service = f"client_messages_service"        
        self.process = None

    def add_insult(self, insult):      
        if insult not in self.get_insults():                   
            self.client.lpush(self.redis_insult_list, insult)         
        else:
            pass     

    def remove_insult(self, insult):
        if insult in self.get_insults():
            self.client.lrem(self.redis_insult_list, 0, insult)
            print(f"Removing {insult}")
        else:
            print(f"The {insult} has already been removed")
    
    def retrieve_insults(self, client):
        print("Retrieving all insults on redis")
        self.client.lpush(client, *self.get_insults())

    def random_insult(self):
        return random.choice(self.get_insults())
    
    # Send random insults to each master subscriber
    def random_events(self, stop):
        insult_list = self.get_insults()
        while not stop.is_set():
            if insult_list:
                insult = self.random_insult()
                print(f"Publishing {insult}")
                self.client.publish(self.broadcast_queue, insult)
                time.sleep(5)
    
    # Get all insults stored in redis
    def get_insults(self):
        return self.client.lrange(self.redis_insult_list, 0, -1)
        
service = InsultService()
service.client.ltrim(service.client_messages_service, 1, 0)
print(service.client_messages_service)
print("Waiting for petitions...")
while True:
    _, raw_data = service.client.brpop(service.client_messages_service, timeout=0)
    petition = json.loads(raw_data)

    operation = petition["operation"]
    data = petition["data"]

    match operation:
        case "Z":
            service.add_insult(data)
        
        case "O":
            service.retrieve_insults(data)

        case "V":
            print("Activating broadcast")
            with Manager() as manager:
                stop = Event()
                list = manager.list(service.get_insults())
                service.process = Process(target=service.random_events, args=(stop,))
                service.process.start()
        
        case "X":
            if service.process is not None:
                stop.set()
                service.process.join()
                print("Stopping broadcast")
            else:
                print("Broadcast not active")

        case _:
            pass
    