import os
from function import p2prpc_analyze_large_file
from pymongo import MongoClient


def clean(host, port):
    db_name, db_collection = 'p2p', p2prpc_analyze_large_file.__name__
    client = MongoClient(host=host, port=port)[db_name][db_collection]
    client.remove()

clean(os.environ['MONGO_HOST'], int(os.environ['MONGO_PORT']))
clean('172.24.0.2', int(os.environ['MONGO_PORT']))


