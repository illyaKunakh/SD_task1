#!/usr/bin/env python3

import subprocess, time, sys, json
from pathlib import Path

import redis


def main() -> None:
    svc_path = Path(__file__).parents[2] / "Redis" / "SingleNode" / "InsultService.py"
    proc = subprocess.Popen([sys.executable, str(svc_path)],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    r      = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
    queue  = "client_messages_service"   # fixed queue for single-node version
    reply  = "test_reply_single"

    try:
        time.sleep(1)                

        # clean previous data
        r.delete(queue, reply, "redis_insult_list")

        # 1) insert insults
        for insult in ("foo", "bar"):
            r.lpush(queue, json.dumps({"operation": "ADD_INSULT", "data": insult}))

        # 2) get the list of insults
        r.lpush(queue, json.dumps({"operation": "LIST", "data": reply}))

        time.sleep(1)                    

        insults = r.lrange(reply, 0, -1)  
        assert insults, "No response from service"
        assert "foo" in insults, "'foo' NOT in response"
        assert "bar" in insults, "'bar' NOT in response"
        print("✅ Redis-SingleNode InsultService OK")

    finally:
        proc.terminate(); proc.wait(timeout=5)


if __name__ == "__main__":
    main()
