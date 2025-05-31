#!/usr/bin/env python3

import subprocess, time, sys, json
from pathlib import Path
import redis


def main() -> None:
    svc_path = Path(__file__).parents[2] / "Redis" / "MultipleNode" / "InsultService.py"
    r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

    # 1) counter before launching the service
    initial_id = int(r.get("insult_service_instance_id") or 0)

    # 2) launch the service
    proc = subprocess.Popen([sys.executable, str(svc_path)],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    try:
        # 3) wait and discover the new instance ID
        for _ in range(20):               # wait up to 2 s
            new_id = int(r.get("insult_service_instance_id") or 0)
            if new_id > initial_id:
                break
            time.sleep(0.1)
        else:
            raise RuntimeError("El servicio no incrementó insult_service_instance_id")

        queue = f"client_messages_service{new_id}"
        reply = "test_reply_multi"

        # pevious cleanup
        r.delete(queue, reply, "redis_insult_list")

        # 4) add insults
        for insult in ("foo", "bar"):
            r.lpush(queue, json.dumps({"operation": "ADD_INSULT", "data": insult}))

        # 5) pedimos la lista completa
        r.lpush(queue, json.dumps({"operation": "LIST", "data": reply}))

        # 6) waiting response (up to 4 s)
        insults = None
        for _ in range(40):
            insults = r.lrange(reply, 0, -1)
            if insults:
                break
            time.sleep(0.1)

        assert insults, "No answer from service"
        assert "foo" in insults and "bar" in insults, "Insults not found in response"

        print("✅ Redis-MultiNode InsultService OK")

    finally:
        proc.terminate(); proc.wait(timeout=5)


if __name__ == "__main__":
    main()
