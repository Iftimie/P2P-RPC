from .base import P2PFlaskApp, is_debug_mode
from functools import wraps
from functools import partial
from .p2pdata import p2p_insert_one
from .p2pdata import p2p_pull_update_one, deserialize_doc_from_net, find
import inspect
import io
import os
from .p2p_brokerworker import function_executor, delete_old_requests
from . import p2p_brokerworker
from .base import configure_logger
import logging
from .globals import requests
import collections
import multiprocessing
from p2prpc.p2pdata import p2p_push_update_one
from pymongo import MongoClient
from .base import P2PArguments, P2PFunction
from .registry_args import remove_values_from_doc
from collections import Callable
import traceback
from .bookkeeper import query_node_states
from .bookkeeper import update_function, initial_discovery
import pymongo
from flask import make_response, jsonify

if 'MONGO_PORT' in os.environ:
    MONGO_PORT = int(os.environ['MONGO_PORT'])
else:
    MONGO_PORT = None
if 'MONGO_HOST' in os.environ:
    MONGO_HOST = os.environ['MONGO_HOST']
else:
    MONGO_HOST = None


def find_response_with_work(db, collection, func_name, password):
    logger = logging.getLogger(__name__)

    res_broker_ip = None
    res_broker_port = None
    res_json = dict()

    brokers = query_node_states()

    for broker in brokers:
        broker_address = broker['address']
        try:
            service_hostip = requests.get('http://{}/actual_service_ip'.format(broker_address),
                                          headers={'Authorization': password}).json()['service_ip']
            bookkeeper_port = broker_address.split(":")[1]
            service_port = str(int(bookkeeper_port) - 1) # TODO there is a distinction between bookkeeper port and actual server port.

            try:
                url = 'http://{}:{}/search_work/{}/{}/{}'.format(service_hostip, service_port, db, collection,
                                                                 func_name)

                res = requests.post(url, headers={"Authorization": password})

                if isinstance(res.json, collections.Callable):
                    returned_json = res.json()  # from requests lib
                else:  # is property
                    returned_json = res.json  # from Flask test client
                if returned_json and 'filter' in returned_json:
                    logger.info("Found work from {}".format(broker_address))
                    res_broker_ip, res_broker_port = service_hostip, service_port
                    res_json = returned_json
                    break
            except:
                pass

            try:
                broker_ip = broker_address.split(':')[0]
                url = 'http://{}:{}/search_work/{}/{}/{}'.format(broker_ip, service_port, db, collection,
                                                                 func_name)
                res = requests.post(url, headers={"Authorization": password})

                if isinstance(res.json, collections.Callable):
                    returned_json = res.json()  # from requests lib
                else:  # is property
                    returned_json = res.json  # from Flask test client
                if returned_json and 'filter' in returned_json:
                    logger.info("Found work from {}".format(broker_address))
                    res_broker_ip, res_broker_port = broker_ip, service_port
                    res_json = returned_json
                    break
            except:
                pass


        except:  # except connection timeout or something like that
            logger.info("broker unavailable {}".format(broker_address))
            pass

    if res_broker_ip is None:
        logger.info("No work found")

    # TODO it may be possible that res allready contains broker ip and port?
    return res_json, res_broker_ip, res_broker_port


def check_brokerworker_termination(registry_functions):
    logger = logging.getLogger(__name__)
    try:
        for p2pworkerfunction in registry_functions.values():
            new_running_jobs = []
            db_name = p2pworkerfunction.p2pfunction.db_name
            db_collection = p2pworkerfunction.p2pfunction.db_collection
            for process, p2pworkerarguments in p2pworkerfunction.running_jobs:
                if process.is_alive():
                    search_filter = {"$or": [{"identifier": p2pworkerarguments.p2parguments.args_identifier},
                                             {"identifier": p2pworkerarguments.remote_args_identifier}]}

                    p2p_pull_update_one(db_name, db_collection, search_filter,
                                        ['kill_clientworker'],
                                        deserializer=partial(deserialize_doc_from_net, up_dir=None, key_interpreter=p2pworkerfunction.p2pfunction.args_interpreter),
                                        password=p2pworkerfunction.p2pfunction.crypt_pass)
                    p2pworkerarguments_updated = p2pworkerfunction.load_arguments_from_db(search_filter)

                    if p2pworkerarguments_updated.kill_clientworker is True:
                        # don't delete the data here.
                        p2p_push_update_one(db_name, db_collection, search_filter,
                                            {"started": 'terminated', 'kill_clientworker': False}, password=p2pworkerfunction.p2pfunction.crypt_pass)
                        process.terminate()
                        logger.info("Terminated process: "+str(process))
                    else:
                        new_running_jobs.append((process, p2pworkerarguments))
            p2pworkerfunction.running_jobs = new_running_jobs
    except FileNotFoundError:
        # TODO. in case there worker is scaled to more than 1. while the first one looks into the DB, the second one can be
        #  trying to delete the function. but it cannot even load from the db because the file wasn't downloaded yet
        #  that's why I expect FileNotFoundError
        #  To solve this issue, I should introduce the Idea of owner. I may want to know to which broker/worker a particular argument belongs to
        logger.info("FileNotFoundError")
    except Exception as e:
        raise e


def check_brokerworker_deletion(registry_functions):
    logger = logging.getLogger(__name__)
    try:

        for p2pworkerfunction in registry_functions.values():
            for p2pworkerarguments in p2pworkerfunction.list_all_arguments():
                search_filter = {"$or": [{"identifier": p2pworkerarguments.p2parguments.args_identifier},
                                         {"identifier": p2pworkerarguments.remote_args_identifier}]}
                p2p_pull_update_one(p2pworkerfunction.p2pfunction.db_name,
                                    p2pworkerfunction.p2pfunction.db_collection, search_filter, ['delete_clientworker'],
                                    deserializer=partial(deserialize_doc_from_net, up_dir=None,
                                                         key_interpreter=p2pworkerfunction.p2pfunction.args_interpreter),
                                    password=p2pworkerfunction.p2pfunction.crypt_pass)
                p2pworkerarguments_updated = p2pworkerfunction.load_arguments_from_db(search_filter)

                if p2pworkerarguments_updated.delete_clientworker is True:
                    p2p_push_update_one(p2pworkerfunction.p2pfunction.db_name,
                                        p2pworkerfunction.p2pfunction.db_collection, search_filter,
                                        {'delete_clientworker': False},
                                        password=p2pworkerfunction.p2pfunction.crypt_pass)
                    p2pworkerfunction.remove_arguments_from_db(p2pworkerarguments_updated)
    except FileNotFoundError:
        # TODO. in case there worker is scaled to more than 1. while the first one looks into the DB, the second one can be
        #  trying to delete the function. but it cannot even load from the db because the file wasn't downloaded yet
        #  that's why I expect FileNotFoundError
        #  To solve this issue, I should introduce the Idea of owner. I may want to know to which broker/worker a particular argument belongs to
        logger.info("FileNotFoundError")
    except Exception as e:
        raise e


class P2PWorkerArguments:
    def __init__(self, expected_keys, expected_return_keys):
        self.p2parguments = P2PArguments(expected_keys, expected_return_keys)
        self.started = None # TODO maybe this property should also stau in p2parguments
        self.kill_clientworker = None
        self.delete_clientworker = None
        self.remote_args_identifier = None #TODO. this item appears in all 3 classes. refactor
        self.timestamp = None

    def object2doc(self):
        function_call_properties = self.p2parguments.object2doc()
        function_call_properties['remote_identifier'] = self.remote_args_identifier
        function_call_properties['started'] = self.started
        function_call_properties['kill_clientworker'] = self.kill_clientworker
        function_call_properties['delete_clientworker'] = self.delete_clientworker
        function_call_properties['timestamp'] = self.delete_clientworker
        return function_call_properties

    def doc2object(self, document):
        self.p2parguments.doc2object(document)
        self.remote_args_identifier = document['remote_identifier']
        if "started" in document:
            self.started = document['started']
        if "kill_clientworker" in document:
            self.kill_clientworker = document['kill_clientworker']
        if 'delete_clientworker' in document:
            self.delete_clientworker = document['delete_clientworker']
        if 'timestamp' in document:
            self.timestamp = document['timestamp']


class P2PWorkerFunction:
    def __init__(self, original_function: Callable, crypt_pass, cache_path):
        self.p2pfunction = P2PFunction(original_function, crypt_pass)
        self.updir = os.path.join(cache_path, self.p2pfunction.db_name, self.p2pfunction.db_collection)  # upload directory
        self.hint_args_file_keys = [k for k, v in inspect.signature(original_function).parameters.items() if v.annotation == io.IOBase]
        self.deserializer = partial(deserialize_doc_from_net, up_dir=self.updir, key_interpreter=self.p2pfunction.args_interpreter)
        self.running_jobs = list()

    @property
    def function_name(self):
        return self.p2pfunction.function_name

    def load_arguments_from_db(self, filter_):
        items = find(self.p2pfunction.db_name, self.p2pfunction.db_collection, filter_,
                     self.p2pfunction.args_interpreter)
        if len(items) == 0:
            return None
        p2pbrokerarguments = P2PWorkerArguments(self.p2pfunction.expected_keys, self.p2pfunction.expected_return_keys)
        p2pbrokerarguments.doc2object(items[0])
        return p2pbrokerarguments

    def list_all_arguments(self):
        logger = logging.getLogger(__name__)

        db_name = self.p2pfunction.db_name
        db_collection = self.p2pfunction.db_collection
        p2pworkerarguments = []
        try:
            for item in MongoClient(host=MONGO_HOST, port=MONGO_PORT)[db_name][db_collection].find({}):
                p2pworkerargument = P2PWorkerArguments(self.p2pfunction.expected_keys,
                                                        self.p2pfunction.expected_return_keys)
                p2pworkerargument.doc2object(item)
                p2pworkerarguments.append(p2pworkerargument)
        except pymongo.errors.AutoReconnect:
            logger.info("Unable to connect to mongo")

        return p2pworkerarguments

    def remove_arguments_from_db(self, p2pworkerarguments):

        remove_values_from_doc(p2pworkerarguments.object2doc())

        filter_ = {"identifier": p2pworkerarguments.p2parguments.args_identifier,
                   'remote_identifier': p2pworkerarguments.remote_args_identifier}

        MongoClient(host=MONGO_HOST, port=MONGO_PORT)[self.p2pfunction.db_name][
            self.p2pfunction.db_collection].delete_one(filter_)

    def download_arguments(self, filter_, broker_ip, broker_port):
        p2pworkerarguments = P2PWorkerArguments(self.p2pfunction.expected_keys,
                                                self.p2pfunction.expected_return_keys)
        p2pworkerarguments.remote_args_identifier = filter_['remote_identifier']
        p2pworkerarguments.p2parguments.args_identifier = filter_['identifier']
        serializable_document = p2pworkerarguments.object2doc()

        current_list_with_identifier = find(self.p2pfunction.db_name, self.p2pfunction.db_collection,
                                            filter_, self.p2pfunction.args_interpreter)
        if len(current_list_with_identifier) == 0:
            # TODO only insert if identifier was not allready downloaded
            #  otherwise in case of restart, the worker will redownload the data and it will crash in
            #  p2p_pull_update_one when checking   if len(collection_res) != 1:
            #  because p2p_insert_one makes a duplicate????
            p2p_insert_one(self.p2pfunction.db_name, self.p2pfunction.db_collection,
                           serializable_document, [broker_ip + ":" + str(broker_port)],
                           do_upload=False, password=self.p2pfunction.crypt_pass)
        p2p_pull_update_one(self.p2pfunction.db_name, self.p2pfunction.db_collection, filter_,
                            p2pworkerarguments.p2parguments.expected_keys, self.deserializer,
                            hint_file_keys=self.hint_args_file_keys, password=self.p2pfunction.crypt_pass)
        items = find(self.p2pfunction.db_name, self.p2pfunction.db_collection, filter_,
                     self.p2pfunction.args_interpreter)
        p2pworkerarguments.doc2object(items[0])
        return p2pworkerarguments


def route_function_stats(registry_functions):
    # TODO this is very similar to p2pbroker. refactor
    ans = []
    for fname, p2workerfunction in registry_functions.items():
        group = {}
        function_details = {}
        function_details["function_name"]=p2workerfunction.p2pfunction.function_name
        function_details["function_code"]=inspect.getsource(p2workerfunction.p2pfunction.original_function)
        p2pworkerarguments = p2workerfunction.list_all_arguments()
        p2pworkerarguments = [arg.object2doc() for arg in p2pworkerarguments]
        group["function_details"]=function_details
        group["p2pworkerarguments"] = p2pworkerarguments
        ans.append(group)
    return jsonify(ans)


class P2PClientworkerApp(P2PFlaskApp):

    def __init__(self, discovery_ips_file, cache_path, local_port=5002, password=""):
        configure_logger("worker", module_level_list=[(__name__, 'DEBUG'),
                                                            (p2p_brokerworker.__name__, 'INFO')])
        super(P2PClientworkerApp, self).__init__(__name__, local_port=local_port, discovery_ips_file=discovery_ips_file,
                                                 cache_path=cache_path, password=password)
        self.roles.append("worker")
        self.register_time_regular_func(partial(delete_old_requests,
                                                         registry_functions=self.registry_functions))
        self.register_time_regular_func(partial(check_brokerworker_termination,
                                                         self.registry_functions))
        self.register_time_regular_func(partial(check_brokerworker_deletion,
                                                         self.registry_functions))

        function_stats_partial = wraps(route_function_stats)(
            partial(self.pass_req_dec(route_function_stats),
                    registry_functions=self.registry_functions))
        self.route(f"/function_stats/", methods=['GET'])(function_stats_partial)

    def register_p2p_func(self, can_do_work_func):
        """
        In p2p worker, this decorator will have the role of deciding making a node behind a firewall or NAT capable of
        executing a function that receives input arguments from over the network.

        Args:
            can_do_work_func: checks if the clientworkerapp can do more work
        """

        def inner_decorator(f):
            p2pworkerfunction = P2PWorkerFunction(f, self.crypt_pass, self.cache_path)
            self.add_to_super_register(p2pworkerfunction)

            db_name = p2pworkerfunction.p2pfunction.db_name
            db_collection = p2pworkerfunction.p2pfunction.db_collection
            function_name = p2pworkerfunction.p2pfunction.function_name
            os.makedirs(p2pworkerfunction.updir, exist_ok=True)

            @wraps(f)
            def wrap():
                if not can_do_work_func():
                    return
                logger = logging.getLogger(__name__)
                logger.info("Searching for work")
                res, broker_ip, broker_port = find_response_with_work(db_name, db_collection, function_name,
                                                                      self.crypt_pass)
                if broker_ip is None:
                    return

                filter_ = res['filter'] # this should have format {"identifier": "xyz" "remote_identifier": "xyz"}
                p2pworkerarguments = p2pworkerfunction.download_arguments(filter_, broker_ip, broker_port)

                new_f = wraps(f)(
                    partial(function_executor,
                            p2pworkerfunction=p2pworkerfunction,
                            p2pworkerarguments=p2pworkerarguments,
                            # FIXME this if statement in case of debug mode was introduced just for an unfortunated combination of OS
                            #  and PyCharm version when variables in watch were hanging with no timeout just because of multiprocessing manaeger
                            logging_queue=self._logging_queue if not is_debug_mode() else None))

                process = multiprocessing.Process(target=new_f)
                process.daemon = True
                process.start()
                p2pworkerfunction.running_jobs.append((process, p2pworkerarguments))

            self.register_time_regular_func(wrap)
            return None

        return inner_decorator
