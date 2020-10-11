import os
from function import p2prpc_analyze_large_file
from pymongo import MongoClient

MONGO_PORT = int(os.environ['MONGO_PORT'])
MONGO_HOST = os.environ['MONGO_HOST']

MONGO_BROKER_PORT = int(os.environ['MONGO_PORT'])
MONGO_BROKER_HOST = '172.24.0.3:5002'


print(MONGO_HOST, MONGO_PORT)
db_name, db_collection = 'p2p', p2prpc_analyze_large_file.__name__
client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)[db_name][db_collection]
print(list(client.find()))
client.remove()
print(list(client.find()))


