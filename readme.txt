## Instructions to ensure the correct functioning of the project (Tested on Debian Linux) ##

pip install matplotlib pika redis Pyro4
docker run --name redis -d -p 6379:6379 redis
docker run --name rabbitmq -d -p 5672:5672 -p 15672:15672 rabbitmq:3-management

# Five terminals with this config will be needed
cd /$root_to_project/SD_task1
source venv/bin/activate

# 4 Of there terminals will be executing the InsultFilter/InsultService .py's of PYRO and XMLRPC
# Term1: python3 ./Pyro/InsultFilter.py
# Term2: python3 ./Pyro/InsultService.py
# Term3: python3 ./XMLRPC/InsultFilter.py
# Term4: python3 ./XMLRPC/InsultService.py

# The last terminal will be used to execute the /unittests 

P.d: Not reccommended to execute Windows as it can't properly manage python processes