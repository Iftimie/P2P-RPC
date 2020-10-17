from p2prpc.p2p_client import P2PClientApp, Future
from p2prpc.p2p_brokerworker import P2PBrokerworkerApp
from p2prpc.p2pdata import find
from p2prpc.p2p_clientworker import P2PClientworkerApp
from p2prpc.base import P2PArguments
from typing import List


def function_call_states(app: [P2PClientworkerApp, P2PBrokerworkerApp, P2PClientApp]) -> List[P2PArguments]:
    #  TODO this will actually return a P2PClientArguments or P2PBrokerArguments or P2PWorkerArguments
    all_futures = []
    for f_name, p2pfunction in app.registry_functions.items():
        arguments = p2pfunction.list_all_arguments()
        all_futures.extend([Future(p2pfunction, arg) for arg in arguments])
    return all_futures


def item_by_func_and_id(app, orig_func, identifier):
    p2pfunction = app.registry_functions[orig_func.__name__]
    return p2pfunction.load_arguments_from_db(filter_={"identifier": identifier})
