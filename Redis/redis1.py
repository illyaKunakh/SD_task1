import redis

# Connect to Redis server
client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# Set a key-value pair
client.set("name", "Alice")

# Get the value of the key
name = client.get("name")
print(f"Retrieved value: {name}")

# Delete the key
client.delete("name")

# Check if key exists
exists = client.exists("name")
print(f"Does 'name' exist? {'Yes' if exists else 'No'}")
