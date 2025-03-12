import redis

# Connect to Redis server
client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# Push elements to a Redis list (left push)
client.lpush("fruits", "apple", "banana", "cherry")

# Get all elements from the list
fruits = client.lrange("fruits", 0, -1)
print(f"Fruits list: {fruits}")

# Pop an element from the list (right pop)
popped_fruit = client.rpop("fruits")
print(f"Popped fruit: {popped_fruit}")

# Get updated list
updated_fruits = client.lrange("fruits", 0, -1)
print(f"Updated fruits list: {updated_fruits}")

# Delete the list
client.delete("fruits")
