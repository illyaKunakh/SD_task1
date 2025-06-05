import redis
import json
import random
import time
from multiprocessing import Process, Event
from redis.lock import Lock

class InsultService:
    def __init__(self):
        self.client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        self.redis_insult_list = "redis_insult_list"
        self.broadcast_channel = "broadcast_channel"
        self.client_messages_service = "client_messages_service"
        self.broadcast_lock = Lock(self.client, "broadcast_lock", timeout=60)
        self.broadcast_process = None
        self.stop_event = None

    def add_insult(self, insult):
        if insult not in self.get_insults():
            self.client.lpush(self.redis_insult_list, insult)
            print(f"Added insult: {insult}")

    def retrieve_insults(self, client):
        print("Retrieving all insults")
        self.client.lpush(client, *self.get_insults())

    def random_insult(self):
        insults = self.get_insults()
        return random.choice(insults) if insults else None

    def broadcast_insults(self, stop_event):
        while not stop_event.is_set():
            insult = self.random_insult()
            if insult:
                self.client.publish(self.broadcast_channel, insult)
                print(f"Broadcasted: {insult}")
            time.sleep(5)

    def start_broadcast(self):
        if self.broadcast_process is None and self.broadcast_lock.acquire(blocking=False):
            self.stop_event = Event()
            self.broadcast_process = Process(target=self.broadcast_insults, args=(self.stop_event,))
            self.broadcast_process.start()
            print("Broadcast started")
        else:
            print("Broadcast already active or lock not acquired")

    def stop_broadcast(self):
        if self.broadcast_process is not None:
            self.stop_event.set()
            self.broadcast_process.join()
            self.broadcast_process = None
            self.stop_event = None
            self.broadcast_lock.release()
            print("Broadcast stopped")

    def get_insults(self):
        return self.client.lrange(self.redis_insult_list, 0, -1)

    # Continuously listens for petitions in the client_messages_service queue
    # and processes them based on the operation specified in the petition.
    # Supported operations:
    # - ADD_INSULT: Adds a new insult to the redis_insult_list
    # - LIST: Retrieves all insults and sends them to the specified client
    # - BCAST_START: Starts broadcasting insults to the broadcast channel
    # - BCAST_STOP: Stops the broadcasting of insults
    # If an unknown operation is received, it prints an error message.
    # The method runs indefinitely, processing petitions as they arrive.
    def process_petitions(self):
        while True:
            _, raw_data = self.client.brpop(self.client_messages_service, timeout=0)
            petition = json.loads(raw_data)
            operation = petition["operation"]
            data = petition["data"]

            if operation == "ADD_INSULT":
                self.add_insult(data)
            elif operation == "LIST":
                self.retrieve_insults(data)
            elif operation == "BCAST_START":
                self.start_broadcast()
            elif operation == "BCAST_STOP":
                self.stop_broadcast()
            else:
                print(f"Unknown operation: {operation}")

if __name__ == "__main__":
    service = InsultService()
    service.process_petitions()