import multiprocessing
import time
import random

import Pyro4

def dynamic_worker(service, method, *args):
    service.__getattribute__(method)(*args)

def monitor_queue_length():
    # Suponiendo que hay una forma de obtener la longitud de la cola
    return random.randint(0, 100)  # Ejemplo

def test_dynamic_scaling(service, method, *args):
    max_workers = 5
    base_workers = 1
    pool = multiprocessing.Pool(base_workers)

    while True:
        queue_length = monitor_queue_length()
        required_workers = min(max_workers, max(1, queue_length // 10))
        if required_workers > pool._processes:
            pool._processes = required_workers
        elif required_workers < pool._processes:
            pool._processes = required_workers

        pool.apply_async(dynamic_worker, (service, method, *args))
        time.sleep(1)

if __name__ == "__main__":
    service = Pyro4.Proxy("PYRONAME:insult.service")
    test_dynamic_scaling(service, 'add_insult', 'insult')