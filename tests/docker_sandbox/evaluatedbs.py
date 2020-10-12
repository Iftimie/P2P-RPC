import os
from function import p2prpc_analyze_large_file
from pymongo import MongoClient

MONGO_PORT = 27017
CLIENT_MONGO_HOST='172.24.0.5'
BROKER_MONGO_HOST='172.24.0.2'


def clean(host, port):
    db_name, db_collection = 'p2p', p2prpc_analyze_large_file.__name__
    client = MongoClient(host=host, port=port)[db_name][db_collection]
    client.remove()


