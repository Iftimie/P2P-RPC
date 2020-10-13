from .base import P2PFlaskApp, P2PFunction, P2PArguments
from .bookkeeper import query_node_states
from functools import wraps
from functools import partial
from .p2pdata import p2p_push_update_one, p2p_insert_one
from .p2pdata import p2p_pull_update_one, deserialize_doc_from_net
import logging
from .base import self_is_reachable
from .p2p_brokerworker import call_remote_func, function_executor, terminate_remote_func, delete_remote_func
import multiprocessing
import io
from .base import wait_until_online
import time
import threading
import inspect
from .globals import requests, future_timeout, future_sleeptime
from .registry_args import hash_kwargs, db_encoder
from .p2p_brokerworker import check_remote_identifier, check_function_termination, check_function_deletion
from .p2pdata import find, update_one
from werkzeug.serving import make_server
from .base import configure_logger
import os
from collections import Callable
from .registry_args import kicomp
from .bookkeeper import update_function, initial_discovery
from .errors import ClientNoBrokerFound, ClientFunctionDifferentBytecode, ClientFunctionError, ClientFutureTimeoutError, \
    ClientUnableFunctionTermination, ClientUnableFunctionDeletion, ClientP2PFunctionInvalidArguments, \
    ClientHashCollision, Broker2ClientIdentifierNotFound
import pymongo
from pymongo import MongoClient
from .registry_args import remove_values_from_doc
import pickle
import tempfile
import traceback

if 'MONGO_PORT' in os.environ:
    MONGO_PORT = int(os.environ['MONGO_PORT'])
else:
    MONGO_PORT = None
if 'MONGO_HOST' in os.environ:
    MONGO_HOST = os.environ['MONGO_HOST']
else:
    MONGO_HOST = None

def select_lru_worker(p2pfunction):
    """
    Selects the least recently used worker from the known states and returns its IP and PORT
    """
    func_bytecode = db_encoder[Callable](p2pfunction.original_function)
    crypt_pass = p2pfunction.crypt_pass
    funcname = p2pfunction.function_name
    logger = logging.getLogger(__name__)

    res = []
    for _ in range(10):
        res = query_node_states()
        if res:
            break
        time.sleep(1)
        logger.info("Empty node states list")

    if len(res) == 0:
        logger.info("No broker available")
        return None, None
    while res:
        try:
            lru_bookkeeper_host, lru_bookkeeper_port = res[0]['address'].split(":") # TODO same thing as below

            service_hostip = requests.get(f'http://{lru_bookkeeper_host}:{lru_bookkeeper_port}/actual_service_ip',
                                          headers={'Authorization': crypt_pass}).json()['service_ip']
            service_port = str(int(lru_bookkeeper_port) - 1) # TODO there is a distinction between bookkeeper port and actual server port.
            # server port is bookeeper_port -1

            addr = f"{service_hostip}:{service_port}"

            logger.info(requests.get('http://{}/registered_functions'.format(addr), headers={'Authorization': crypt_pass}))
            worker_functions = requests.get('http://{}/registered_functions'.format(addr), headers={'Authorization': crypt_pass}).json()

            if funcname not in worker_functions or worker_functions[funcname]["bytecode"] != func_bytecode:
                # In case there are errors here, check that you have the same version of python on all services
                raise ClientFunctionDifferentBytecode(p2pfunction, addr)
            break
        except Exception as e:
            logger.info("broker unavailable {} from error {}".format(res[0]['address'], e))
            res.popleft()

    if len(res) == 0:
        logger.info("No worker or broker available")
        return None, None
    lru_host, lru_port = addr.split(":")
    return lru_host, lru_port





def find_required_args():
    necessary_args = ['p2pworkerfunction', 'filter_']
    actual_args = dict()
    frame_infos = inspect.stack()[:]
    for frame in frame_infos:
        if frame.function == function_executor.__name__:
            f_locals = frame.frame.f_locals
            actual_args = {k: f_locals[k] for k in necessary_args}
            break
    return actual_args


def p2p_progress_hook(curidx, endidx):

    actual_args = find_required_args()
    update_ = {"progress": curidx/endidx * 100}
    filter_ = actual_args['filter_']
    p2pworkerfunction = actual_args['p2pworkerfunction']
    p2p_push_update_one(
                        p2pworkerfunction.p2pfunction.db_name,
                        p2pworkerfunction.p2pfunction.db_collection, filter_, update_,
                        password=p2pworkerfunction.p2pfunction.crypt_pass)


def default_saving_func(filepath, item):
    pickle.dump(item, open(filepath, 'wb'))


def p2p_save(key, item, filesuffix=".pkl", saving_func=default_saving_func):
    actual_args = find_required_args()
    fp = p2p_getfilepath(suffix=filesuffix)
    document = {key: fp}
    saving_func(fp, item)
    p2pworkerfunction = actual_args['p2pworkerfunction']
    filter_ = actual_args['filter_']
    update_one(p2pworkerfunction.p2pfunction.db_name, p2pworkerfunction.p2pfunction.db_collection, filter_, document, upsert=False)


def default_loading_func(filepath):
    return pickle.load(open(filepath, 'rb'))


def p2p_load(key, loading_func=default_loading_func):
    logger = logging.getLogger(__name__)
    actual_args = find_required_args()
    p2pworkerfunction = actual_args['p2pworkerfunction']
    filter_ = actual_args['filter_']
    item = find(p2pworkerfunction.p2pfunction.db_name, p2pworkerfunction.p2pfunction.db_collection, filter_,
                p2pworkerfunction.p2pfunction.args_interpreter)[0]
    logger.error(str(item))
    if key in item:
        return loading_func(item[key])
    else:
        return None


def p2p_getfilepath(suffix):
    filepath = tempfile.mkstemp(suffix=suffix, dir=None, text=False)[1]
    actual_args = find_required_args()
    key = "tmpfile"
    p2pworkerfunction = actual_args['p2pworkerfunction']
    filter_ = actual_args['filter_']
    item = find(p2pworkerfunction.p2pfunction.db_name, p2pworkerfunction.p2pfunction.db_collection, filter_,
                p2pworkerfunction.p2pfunction.args_interpreter)[0]
    count = 0
    while key in item:
        key += str(count)
        count += 1
    document = {key: filepath}
    update_one(p2pworkerfunction.p2pfunction.db_name, p2pworkerfunction.p2pfunction.db_collection, filter_, document,
               upsert=False)
    return filepath


def get_remote_future(p2pclientfunction, p2pclientarguments):
    logger = logging.getLogger(__name__)

    # up_dir = os.path.join(cache_path, db)
    # if not os.path.exists(up_dir):
    #     os.mkdir(up_dir)
    expected_return_keys = p2pclientfunction.p2pfunction.expected_return_keys
    hint_file_keys = [k for k in expected_return_keys if p2pclientfunction.p2pfunction.args_interpreter[k] == io.IOBase]

    filter_ = {"identifier": p2pclientarguments.p2parguments.args_identifier,
               "remote_identifier": p2pclientarguments.remote_args_identifier}

    fsf = frozenset(filter_.items())
    while fsf in p2pclientfunction.running_jobs and p2pclientfunction.running_jobs[fsf][0].is_alive():
        time.sleep(4)
        logger.info(f"Waiting for uploading job to finish. filter is {filter_}, job is alive {p2pclientfunction.running_jobs[fsf][0].is_alive()}"
                    f"pid is {p2pclientfunction.running_jobs[fsf][0].pid}")
        # what if due to expiration on broker, the upload didn't even finish??
        # even if the item is expired on broker. the upload part is guaranteed to finish. as long as user does not
        # terminate upload

    # TODO. do I really need to be an or here? Yes I do. if I send only the remote identifier, it will no see the one locally
    search_filter = {"$or": [{"identifier": p2pclientarguments.p2parguments.args_identifier},
                             {"identifier": p2pclientarguments.remote_args_identifier}]}
    p2p_pull_update_one(
                        p2pclientfunction.p2pfunction.db_name,
                        p2pclientfunction.p2pfunction.db_collection, search_filter,
                        expected_return_keys,
                        deserializer=partial(deserialize_doc_from_net,
                                             up_dir=p2pclientfunction.updir, key_interpreter=p2pclientfunction.p2pfunction.args_interpreter),
                        hint_file_keys=hint_file_keys,
                        password=p2pclientfunction.p2pfunction.crypt_pass)
    p2pclientarguments = p2pclientfunction.load_arguments_from_db(filter_)
    return p2pclientarguments


class P2PClientArguments:
    def __init__(self, kwargs, expected_return_keys):
        self.p2parguments = P2PArguments(list(kwargs.keys()), expected_return_keys)
        self.p2parguments.kwargs.update(kwargs)
        self.remote_args_identifier = None
        self.ip = None
        self.port = None

    def object2doc(self):
        function_call_properties = self.p2parguments.object2doc()
        function_call_properties['remote_identifier'] = self.remote_args_identifier
        return function_call_properties

    def doc2object(self, document):
        self.p2parguments.doc2object(document)
        self.remote_args_identifier = document['remote_identifier']
        self.ip, self.port = document['nodes'][0].split(":")
        self.port = int(self.port)

    def create_generic_filter(self):
        filter_ = {"identifier": self.p2parguments.args_identifier,
                   "remote_identifier": self.remote_args_identifier}
        return filter_


class P2PClientFunction:
    def __init__(self, original_function: Callable,  crypt_pass, local_port, cache_path):
        self.p2pfunction = P2PFunction(original_function,  crypt_pass)
        self.updir = os.path.join(cache_path, self.p2pfunction.db_name, self.p2pfunction.db_collection)  # same as in worker
        self.local_port = local_port
        self.running_jobs = dict()
        self.__salt_pepper_identifier = 1

    def load_arguments_from_db(self, filter_):
        items = find( self.p2pfunction.db_name, self.p2pfunction.db_collection, filter_,
                     self.p2pfunction.args_interpreter)
        if len(items) == 0:
            return None
        p2pclientarguments = P2PClientArguments({k:None for k in self.p2pfunction.expected_keys}, self.p2pfunction.expected_return_keys)
        p2pclientarguments.doc2object(items[0])
        return p2pclientarguments

    def list_all_arguments(self):
        logger = logging.getLogger(__name__)

        db_name = self.p2pfunction.db_name
        db_collection = self.p2pfunction.db_collection
        p2pclientarguments = []
        try:
            for item in MongoClient(host=MONGO_HOST, port=MONGO_PORT)[db_name][db_collection].find({}):
                p2pclientargument = P2PClientArguments({k:None for k in self.p2pfunction.expected_keys},
                                                       self.p2pfunction.expected_return_keys)
                p2pclientargument.doc2object(item)
                p2pclientarguments.append(p2pclientargument)
        except pymongo.errors.AutoReconnect:
            logger.info("Unable to connect to mongo")

        return p2pclientarguments

    def validate_arguments(self, args, kwargs):
        """
        After the function has been called, it's actual arguments are checked for some other constraints.
        For example all arguments must be specified as keyword arguments,
        All arguments must be instances of the declared type.
        # TODO to prevent security issues maybe the type(arg) == declared type, not isinstance(arg, declared_type)
        """
        if len(args) != 0:
            ClientP2PFunctionInvalidArguments(self.p2pfunction, "All arguments to a function in this p2p framework need to be specified by keyword arguments")

        f_param_sign = inspect.signature(self.p2pfunction.original_function).parameters
        if set(f_param_sign.keys()) != set(kwargs.keys()):
            ClientP2PFunctionInvalidArguments(self.p2pfunction, f"Expected keys {set(f_param_sign.keys())}, received Keys {set(kwargs.keys())}")
        # check that every value passed in this function has the same type as the one declared in function annotation
        for k, v in kwargs.items():
            f_param_k_annotation = f_param_sign[k].annotation
            if not isinstance(v, f_param_k_annotation):
                ClientP2PFunctionInvalidArguments(self.p2pfunction,
                                                  f"class of value {v} for argument {k} is not the same as the annotation {f_param_k_annotation}")
        for value in P2PFunction.restricted_values:
            if value in kwargs.values():
                ClientP2PFunctionInvalidArguments(self.p2pfunction, f"'{value}' string is a reserved value in this p2p framework. It helps "
                                 "identifying a file when serializing together with other arguments")
        files = [v for v in kwargs.values() if isinstance(v, io.IOBase)]
        if len(files) > 1:
            ClientP2PFunctionInvalidArguments(self.p2pfunction,
                                              "p2p framework does not currently support sending more than one file")
        if files:
            if any(file.closed or file.mode != 'rb' or file.tell() != 0 for file in files):
                ClientP2PFunctionInvalidArguments(self.p2pfunction,
                                                  "all files should be opened in read binary mode and pointer must be at start")

    def create_arguments_identifier(self, kwargs):
        """
        A hash from the arguments will be created that will help to easily identify the set of arguments for later retrieval

        Args:
            kwargs: the provided function keyword arguments
        Returns:
            identifier: string
        """
        identifier = hash_kwargs({k: v for k, v in kwargs.items() if k in self.p2pfunction.args_interpreter})
        identifier_original = identifier  # deepcopy

        while True:
            collection = find( self.p2pfunction.db_name, self.p2pfunction.db_collection,
                              {"identifier": identifier}, self.p2pfunction.args_interpreter)
            if len(collection) == 0:
                # we found an identifier that is not in DB
                return identifier
            elif len(collection) != 1:
                # we should find only one doc that has the same hash
                raise ClientHashCollision(self.p2pfunction, kwargs, collection)
            elif all(kicomp(kwargs[k]) == kicomp(item[k]) for item in collection for k in kwargs):
                # we found exactly 1 doc with the same hash and we must check that it has the same arguments
                return identifier
            else:
                # we found different arguments that produce the same hash so we must modify the hash determinastically
                identifier = identifier_original + str(self.__salt_pepper_identifier)
                self.__salt_pepper_identifier += 1
                # raising an error here if salt_pepper > 100 seems like it is unlikely to raise

    def create_arguments_remote_identifier(self, local_args_identifier, ip, port):
        """
        A remote identifier is created in order to prevent name collisions in worker nodes.
        """
        args = {"ip": ip, "port": port, "db": self.p2pfunction.db_name,
                "col": self.p2pfunction.db_collection,
                "func_name": self.p2pfunction.function_name,
                "password": self.p2pfunction.crypt_pass}
        count = 1
        original_local_identifier = local_args_identifier
        while True:
            args['identifier'] = local_args_identifier
            if check_remote_identifier(**args):
                return local_args_identifier
            else:
                local_args_identifier = original_local_identifier + str(count)
                count += 1
                # raising an error here if count > 100 seems like it is unlikely to raise

    def start_remote(self, ip, port, p2pclientarguments):
        """
        """
        logger = logging.getLogger(__name__)

        filter_ = p2pclientarguments.create_generic_filter()

        serializable_document = p2pclientarguments.object2doc()

        nodes = [str(ip) + ":" + str(port)]

        try:
            # TODO try to check if the file is already present on remote server. this is a stupid way.
            p2p_insert_one(self.p2pfunction.db_name, self.p2pfunction.db_collection,
                           serializable_document, nodes,
                           current_address_func=partial(self_is_reachable, self.local_port),
                           password=self.p2pfunction.crypt_pass, do_upload=False)
            hint_file_keys = [k for k in self.p2pfunction.expected_return_keys if
                              self.p2pfunction.args_interpreter[k] == io.IOBase]
            p2p_pull_update_one(
                self.p2pfunction.db_name,
                self.p2pfunction.db_collection, {"$or": [{"identifier": filter_['identifier']},
                                                         {"identifier": filter_['remote_identifier']}]},
                self.p2pfunction.expected_return_keys,
                deserializer=partial(deserialize_doc_from_net,
                                     up_dir=self.updir,
                                     key_interpreter=self.p2pfunction.args_interpreter),
                hint_file_keys=hint_file_keys,
                password=self.p2pfunction.crypt_pass)

            p2pclientarguments = self.load_arguments_from_db(filter_)

            if not any(p2pclientarguments.p2parguments.outputs[k] is None
                for k in self.p2pfunction.expected_return_keys
                if k != 'error'):
                logger.warning("Results are already present on broker.")
                return
        except Exception as e:
            logger.error(str(e))

        p2p_insert_one( self.p2pfunction.db_name, self.p2pfunction.db_collection,
                       serializable_document, nodes,
                       current_address_func=partial(self_is_reachable, self.local_port),
                       password=self.p2pfunction.crypt_pass, do_upload=True)
        logger.info(f"Uploading finished. pid is {os.getpid()}")
        call_remote_func(ip, port, self.p2pfunction.db_name, self.p2pfunction.db_collection, self.p2pfunction.function_name,
                         filter_, self.p2pfunction.crypt_pass)
        logger.info(f"call_remote_func finished. pid is {os.getpid()}")

    def register_arguments_in_db(self, p2pclientarguments, nodes):
        serializable_document = p2pclientarguments.object2doc()
        p2p_insert_one( self.p2pfunction.db_name,
                       self.p2pfunction.db_collection, serializable_document,
                       nodes,
                       current_address_func=partial(self_is_reachable, self.local_port),
                       password=self.p2pfunction.crypt_pass, do_upload=False)

    def arguments_already_submited(self, p2pclientarguments, time_limit=24):
        """
        # TODO this function may stay better in P2Pclientfunction class
        Returns boolean about if the current arguments resulted in an identifier that was already seen

        Args:
            args_identifier: string representing the hash of the function arguments. Created by create_arguments_identifier
        """
        logger = logging.getLogger(__name__)

        collection = find(
                          self.p2pfunction.db_name,
                          self.p2pfunction.db_collection,
                          {"identifier": p2pclientarguments.p2parguments.args_identifier},
                          self.p2pfunction.args_interpreter)

        if collection:
            assert len(collection) == 1
            item = collection[0]
            if (time.time() - item['timestamp']) > time_limit * 3600 and any(
                    item[k] is None for k in self.p2pfunction.expected_return_keys):
                # TODO the entry should actually be deleted instead of letting it be overwritten
                #  for elegancy
                logger.info("Time limit exceeded for item with identifier: " + item['identifier'])
                return False
            else:
                return True
        else:
            return False

    def remove_arguments_from_db(self, p2pworkerarguments):

        remove_values_from_doc(p2pworkerarguments.object2doc())

        filter_ = {"identifier": p2pworkerarguments.p2parguments.args_identifier,
                   'remote_identifier': p2pworkerarguments.remote_args_identifier}

        client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)[self.p2pfunction.db_name][
            self.p2pfunction.db_collection]
        client.delete_one(filter_)
        pass

class Future:

    def __init__(self, p2pclientfunction: P2PClientFunction, p2pclientarguments: P2PClientArguments, upload_job=None):
        self.p2pclientfunction = p2pclientfunction
        self.p2pclientarguments = p2pclientarguments
        self.upload_job = upload_job

    def get(self, timeout=future_timeout):
        logger = logging.getLogger(__name__)

        count_time = 0
        while any(self.p2pclientarguments.p2parguments.outputs[k] is None
                  for k in self.p2pclientfunction.p2pfunction.expected_return_keys
                  if k != 'error'):
            logger.info("Not done yet " + str(self.p2pclientarguments.p2parguments.args_identifier))
            self.p2pclientarguments = get_remote_future(self.p2pclientfunction, self.p2pclientarguments)
            if 'error' in self.p2pclientarguments.p2parguments.outputs and \
                    self.p2pclientarguments.p2parguments.outputs['error'] != None:
                document = self.p2pclientarguments.object2doc()
                logger.error(f"Error while executing function for document {document}")
                raise ClientFunctionError(self.p2pclientfunction.p2pfunction, self.p2pclientarguments.p2parguments)
            time.sleep(future_sleeptime)
            count_time += future_sleeptime
            if count_time > timeout:
                logger.error(f"Waiting time exceeded for document {self.p2pclientarguments.object2doc()}")
                raise ClientFutureTimeoutError(self.p2pclientfunction.p2pfunction, self.p2pclientarguments.p2parguments)
        logger.info(f"{self.p2pclientarguments.p2parguments.args_identifier} finished")
        return self.p2pclientarguments.p2parguments.outputs

    def terminate(self):
        logger = logging.getLogger(__name__)

        filter_ = {"identifier": self.p2pclientarguments.p2parguments.args_identifier,
                   "remote_identifier": self.p2pclientarguments.remote_args_identifier}
        frozenset_filter = frozenset(filter_.items())

        if frozenset_filter in self.p2pclientfunction.running_jobs:
            if self.p2pclientfunction.running_jobs[frozenset_filter][0].is_alive():
                self.p2pclientfunction.running_jobs[frozenset_filter][0].terminate()
                del self.p2pclientfunction.running_jobs[frozenset_filter]

        search_filter = {"$or": [{"identifier": self.p2pclientarguments.p2parguments.args_identifier},
                                 {"identifier": self.p2pclientarguments.remote_args_identifier}]}
        terminate_remote_func(self.p2pclientarguments.ip, self.p2pclientarguments.port,
                              self.p2pclientfunction.p2pfunction.db_name,
                              self.p2pclientfunction.p2pfunction.db_collection,
                              self.p2pclientfunction.p2pfunction.function_name,
                              search_filter,
                              self.p2pclientfunction.p2pfunction.crypt_pass)

        # wait termination on worker
        max_trials = 10
        while True:
            if max_trials == 0:
                raise ClientUnableFunctionTermination(self.p2pclientfunction.p2pfunction, self.p2pclientarguments.p2parguments)
            try:
                res = check_function_termination(self.p2pclientarguments.ip, self.p2pclientarguments.port,
                                                 self.p2pclientfunction.p2pfunction.db_name,
                                                 self.p2pclientfunction.p2pfunction.db_collection,
                                                 self.p2pclientfunction.p2pfunction.function_name,
                                                 search_filter,
                                                 self.p2pclientfunction.p2pfunction.crypt_pass)
                print(res.json()['status'])
                if res.json()['status'] is True:
                    break
            except Broker2ClientIdentifierNotFound:
                logger.info("Identifier not found on broker. Uploading job must have been killed before finishing.")
                break
            time.sleep(3)
            max_trials-=1
            logger.info("Still checking function termination")

    def restart(self):
        logger = logging.getLogger(__name__)
        self.terminate()
        search_filter = {"$or": [{"identifier": self.p2pclientarguments.p2parguments.args_identifier},
                                 {"identifier": self.p2pclientarguments.remote_args_identifier}]}
        call_remote_func(self.p2pclientarguments.ip, self.p2pclientarguments.port,
                         self.p2pclientfunction.p2pfunction.db_name,
                         self.p2pclientfunction.p2pfunction.db_collection,
                         self.p2pclientfunction.p2pfunction.function_name,
                         search_filter,
                         self.p2pclientfunction.p2pfunction.crypt_pass)
        logger.info(f"Function restarted for filter {search_filter}")

    def delete(self):
        # TODO. if terminate is called first, then delete. it should work
        # basically there are 2 terminate calls and a delete call
        self.terminate()

        search_filter = {"$or": [{"identifier": self.p2pclientarguments.p2parguments.args_identifier},
                                 {"identifier": self.p2pclientarguments.remote_args_identifier}]}
        delete_remote_func(self.p2pclientarguments.ip, self.p2pclientarguments.port,
                         self.p2pclientfunction.p2pfunction.db_name,
                         self.p2pclientfunction.p2pfunction.db_collection,
                         self.p2pclientfunction.p2pfunction.function_name,
                         search_filter,
                         self.p2pclientfunction.p2pfunction.crypt_pass)

        # wait deletion on both worker and on brokerworker
        max_trials = 10
        while True:
            if max_trials == 0:
                raise ClientUnableFunctionDeletion(self.p2pclientfunction.p2pfunction, self.p2pclientarguments.p2parguments)
            res = check_function_deletion(self.p2pclientarguments.ip, self.p2pclientarguments.port,
                         self.p2pclientfunction.p2pfunction.db_name,
                         self.p2pclientfunction.p2pfunction.db_collection,
                         self.p2pclientfunction.p2pfunction.function_name,
                         search_filter,
                         self.p2pclientfunction.p2pfunction.crypt_pass)
            if res.json()['status'] is True:
                self.p2pclientfunction.remove_arguments_from_db(self.p2pclientarguments)
                break
            time.sleep(3)


class ServerThread(threading.Thread):

    def __init__(self, app: P2PFlaskApp, processes=1):
        threading.Thread.__init__(self)
        self.srv = make_server('0.0.0.0', app.local_port, app, processes=processes)
        self.app = app
        self.ctx = app.app_context()
        self.ctx.push()

    def run(self):
        self.app.start_background_threads()
        self.srv.serve_forever()

    def shutdown(self):
        self.app.stop_background_threads()
        self.srv.shutdown()


class P2PClientApp(P2PFlaskApp):

    def __init__(self, discovery_ips_file, cache_path, local_port=5000,  password=""):
        if not os.path.exists(cache_path):
            os.mkdir(cache_path)
        configure_logger(os.path.join(cache_path, "client"), module_level_list=[(__name__, 'DEBUG')])
        super(P2PClientApp, self).__init__(__name__, local_port=local_port, discovery_ips_file=discovery_ips_file,
                                                 cache_path=cache_path, password=password)
        self.roles.append("client")
        self.jobs = dict()
        self.background_server = None

    def function_stats(self):
        ans = []
        for p2pfunction in self.registry_functions.values():
            p2pclientfunction = P2PClientFunction(p2pfunction.original_function, self.crypt_pass, self.local_port, self.cache_path)
            group = {}
            function_details = {}
            function_details["function_name"] = p2pclientfunction.p2pfunction.function_name
            function_details["function_code"] = inspect.getsource(p2pclientfunction.p2pfunction.original_function)

            p2pclientarguments = p2pclientfunction.list_all_arguments()
            p2pclientarguments = [arg.object2doc() for arg in p2pclientarguments]
            group["function_details"] = function_details
            group["p2pclientarguments"] = p2pclientarguments
            ans.append(group)
        return ans

    def register_p2p_func(self):
        """
        In p2p client, this decorator will have the role of deciding if the function should be executed remotely or
        locally. It will store the input in a collection. If the current node is reachable, then data will be updated automatically,
        otherwise data will be updated at subsequent calls by using a future object

        Args:
            self: P2PFlaskApp object this instance is passed as argument from create_p2p_client_app. This is done like that
                just to avoid making redundant Classes. Just trying to make the code more functional
            can_do_locally_func: function that returns True if work can be done locally and false if it should be done remotely
                if not specified, then it means all calls should be done remotely
        """

        def inner_decorator(f):
            p2pclientfunction = P2PClientFunction(f,  self.crypt_pass, self.local_port, self.cache_path)
            self.add_to_super_register(p2pclientfunction.p2pfunction)

            os.makedirs(p2pclientfunction.updir, exist_ok=True) # same as in worker

            @wraps(f)
            def wrap(*args, **kwargs):
                logger = logging.getLogger(__name__)

                p2pclientfunction.validate_arguments(args, kwargs)
                p2pclientarguments = P2PClientArguments(kwargs, p2pclientfunction.p2pfunction.expected_return_keys)
                p2pclientarguments.p2parguments.args_identifier = p2pclientfunction.create_arguments_identifier(kwargs)
                if p2pclientfunction.arguments_already_submited(p2pclientarguments):
                    logger.info("Returning future that may already be precomputed")
                    p2pclientarguments = p2pclientfunction.load_arguments_from_db(
                        filter_={"identifier": p2pclientarguments.p2parguments.args_identifier})
                    return Future(p2pclientfunction, p2pclientarguments)

                lru_ip, lru_port = select_lru_worker(p2pclientfunction.p2pfunction)
                if lru_ip is None:
                    raise ClientNoBrokerFound(p2pclientfunction.p2pfunction, p2pclientarguments.p2parguments)

                p2pclientarguments.remote_args_identifier = p2pclientfunction.create_arguments_remote_identifier(
                    p2pclientarguments.p2parguments.args_identifier,
                    lru_ip, lru_port
                )
                p2pclientarguments.ip = lru_ip
                p2pclientarguments.port = lru_port
                nodes = [str(lru_ip) + ":" + str(lru_port)]

                p2pclientfunction.register_arguments_in_db(p2pclientarguments, nodes)
                # TODO it seems redunant. here it does not upload. it only registers locally.

                logger.info("Dispacthed function work to {},{}".format(lru_ip, lru_port))

                #The reason why start remote must be a subprocess is because the uploading job might take time
                process = multiprocessing.Process(
                    target=partial(p2pclientfunction.start_remote, lru_ip, lru_port, p2pclientarguments))
                process.daemon = True
                process.start()
                filter_ = {"identifier": p2pclientarguments.p2parguments.args_identifier,
                           "remote_identifier": p2pclientarguments.remote_args_identifier}
                p2pclientfunction.running_jobs[frozenset(filter_.items())] = (process, p2pclientarguments)
                return Future(p2pclientfunction, p2pclientarguments, upload_job=process)

            return wrap

        return inner_decorator


def create_p2p_client_app(discovery_ips_file, cache_path, local_port=4999,  password="", processes=3):
    p2p_client_app = P2PClientApp(discovery_ips_file=discovery_ips_file, local_port=local_port,
                                  cache_path=cache_path, password=password)
    p2p_client_app.background_server = ServerThread(p2p_client_app, processes=processes)
    p2p_client_app.background_server.start()
    wait_until_online(p2p_client_app.local_port, p2p_client_app.crypt_pass)
    return p2p_client_app
