import requests
from p2prpc.p2p_client import create_p2p_client_app, ServerThread, P2PClientApp
from p2prpc.p2p_brokerworker import P2PBrokerworkerApp
from p2prpc.p2pdata import find
import os
from concurrent.futures import ThreadPoolExecutor
import time
from p2prpc.p2p_clientworker import P2PClientworkerApp
from p2prpc.p2p_client import select_lru_worker
from shutil import rmtree
from typing import Any
from p2prpc.base import derive_vars_from_function


def get_node_states(address="localhost:5000"):
    res = requests.get('http://{}/node_states'.format(address)).json()  # will get the data defined above
    return res


def get_geo_location(ip=None):
    if ip is None:
        externalipres = requests.get('http://checkip.dyndns.org/')
        ip = externalipres.content.decode('utf-8').split(": ")[1].split("<")[0]
    res = requests.get("http://ip-api.com/json/{ip}".format(ip=ip)).json()
    return res


def function_call_states(app: [P2PClientworkerApp, P2PBrokerworkerApp, P2PClientApp]):
    for f_name in app.registry_functions:
        _, db, col = derive_vars_from_function(app.registry_functions[f_name]['original_func'])
        items = find(app.mongod_port, db, col, {})
        print(items)


def destroy_apps(client_app, broker_worker_thread, clientworker_thread):
    client_app.background_server.shutdown()
    print("Shutdown client")
    broker_worker_thread.shutdown()
    print("Shutdown brokerworker")
    clientworker_thread.shutdown()
    print("Shutdown clientworker")


def do_nothing_function(random_arg: int) -> {"results": str}:
    return {"results": "bye"}


def create_apps(tmpdir, port_offset, func):
    client_port = 5000 + port_offset
    broker_port = 5004 + port_offset

    ndclient_path = os.path.join(tmpdir, "ndclient.txt")
    cache_client_dir = os.path.join(tmpdir, "client")
    cache_bw_dir = os.path.join(tmpdir, "bw")
    with open(ndclient_path, "w") as f: f.write("localhost:{}\n".format(broker_port))
    client_app = create_p2p_client_app(ndclient_path, local_port=client_port, mongod_port=client_port+100, cache_path=cache_client_dir)
    client_func = client_app.register_p2p_func(can_do_locally_func=lambda: False)(func)

    broker_worker_app = P2PBrokerworkerApp(None, local_port=broker_port, mongod_port=broker_port+100, cache_path=cache_bw_dir)
    broker_worker_app.register_p2p_func(can_do_locally_func=lambda :False)(func)
    broker_worker_thread = ServerThread(broker_worker_app, 10)
    broker_worker_thread.start()
    while select_lru_worker(client_port) == (None, None):
        time.sleep(3)
        print("Waiting for client to know about broker")

    ndcw_path = os.path.join(tmpdir, "ndcw.txt")
    client_worker_port = 5005 + port_offset
    cache_cw_dir = os.path.join(tmpdir, "cw")
    with open(ndcw_path, "w") as f: f.write("localhost:{}\n".format(broker_port))
    clientworker_app = P2PClientworkerApp(ndcw_path, local_port=client_worker_port,
                                          mongod_port=client_worker_port + 100, cache_path=cache_cw_dir)
    clientworker_app.register_p2p_func(can_do_work_func=lambda: True)(func)
    clientworker_thread = ServerThread(clientworker_app)
    clientworker_thread.start()
    while select_lru_worker(client_worker_port) == (None, None):
        time.sleep(3)
        print("Waiting for clientworker to know about broker")

    with ThreadPoolExecutor(max_workers=10) as executor:
        num_calls = 1
        list_futures_of_futures = []
        for i in range(num_calls):
            future = executor.submit(client_func, random_arg=i)
            list_futures_of_futures.append(future)
        list_futures = [f.result() for f in list_futures_of_futures]
        assert len(list_futures) == num_calls
        list_results = [f.get() for f in list_futures]
        assert len(list_results) == num_calls

    return client_app, broker_worker_thread, clientworker_thread


def clean_and_create():
    test_dir = "/home/achellaris/delete_test_dir"
    if os.path.exists(test_dir):
        rmtree(test_dir)
        while os.path.exists(test_dir):
            time.sleep(3)
    os.mkdir(test_dir)
    return test_dir


client_app, broker_worker_thread, clientworker_thread = create_apps(clean_and_create(), 1510, func=do_nothing_function)

function_call_states(broker_worker_thread.app)

destroy_apps(client_app, broker_worker_thread, clientworker_thread)