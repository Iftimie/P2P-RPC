import os
from function_module.function import p2prpc_analyze_large_file
from pymongo import MongoClient
import click
from p2prpc.p2pdata import update_one


@click.group()
def cli():
    pass


def get_addresses():
    ans = []
    with open("iplist.txt", 'r') as f:
        for line in f.readlines():
            name, ip = line.split()
            ans.append((name, ip))
    return ans


def mockupdate(service, filter_, update):
    db_name, db_collection = 'p2p', p2prpc_analyze_large_file.__name__
    ip = None
    for name, ip in get_addresses():
        if name==service:
            break
    prevhost, os.environ['MONGO_HOST'] = os.environ['MONGO_HOST'], ip
    update_one(db_name, db_collection, filter_, update, upsert=False)
    os.environ['MONGO_HOST'] = prevhost
    print()


def clean_func(dbs):
    MONGO_PORT = 27017
    addresses = get_addresses()
    addresses = list(filter(lambda x: x[0] in dbs, addresses))
    db_name, db_collection = 'p2p', p2prpc_analyze_large_file.__name__
    for name, ip in addresses:
        client = MongoClient(host=ip, port=MONGO_PORT)[db_name][db_collection]
        client.remove()


def query(service):
    MONGO_PORT = 27017
    db_name, db_collection = 'p2p', p2prpc_analyze_large_file.__name__
    ip = None
    for name, ip in get_addresses():
        if name == service:
            break
    prevhost, os.environ['MONGO_HOST'] = os.environ['MONGO_HOST'], ip
    client = MongoClient(host=ip, port=MONGO_PORT)[db_name][db_collection]
    os.environ['MONGO_HOST'] = prevhost
    return list(client.find())


@cli.command()
@click.option('--dbs', nargs=0)
@click.argument('dbs', nargs=-1)
def clean(dbs):
    # we need a wrapper function because if we try to call clean. the code actually calls the
    # decorated function, and it will block
    clean_func(dbs)


def main():
    cli()


if __name__ == '__main__':
    main()

