pip install matplotlib pika redis

docker run --name redis -d -p 6379:6379 redis
docker run --name rabbitmq -d -p 5672:5672 -p 15672:15672 rabbitmq:3-management
