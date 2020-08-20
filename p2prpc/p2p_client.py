from .base import P2PFlaskApp, P2PFunction, P2PArguments, create_bookkeeper_p2pblueprint, is_debug_mode
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
import requests
from .registry_args import hash_kwargs, db_encoder
from .p2p_brokerworker import check_remote_identifier, check_function_termination, check_function_deletion
from .p2pdata import find
from werkzeug.serving import make_server
from .base import configure_logger
import os
import traceback
from collections import defaultdict, Callable
from p2prpc.p2pdata import update_one
import tempfile
import pickle
from .registry_args import kicomp
from pymongo import MongoClient
from p2prpc.registry_args import deserialize_doc_from_db, remove_values_from_doc
import dis


def select_lru_worker(local_port, func, password):
    """
    Selects the least recently used worker from the known states and returns its IP and PORT
    """
    func_bytecode = db_encoder[Callable](func)
    logger = logging.getLogger(__name__)
    try:
        res = requests.get('http://localhost:{}/node_states'.format(local_port), timeout=120).json()  # will get the data defined above
    except:
        logger.info(traceback.format_exc())
        return None, None

    res = list(filter(lambda d: 'node_type' in d, res))
    res1 = [item for item in res if 'worker' in item['node_type'] or 'broker' in item['node_type']]
    if len(res1) == 0:
        logger.info("No worker or broker available")
        return None, None
    res1 = sorted(res1, key=lambda x: x['workload'])
    while res1:
        try:
            addr = res1[0]['address']
            response = requests.get('http://{}/echo'.format(addr), headers={'Authorization': password})
            if response.status_code != 200:
                raise ValueError
            worker_functions = requests.get('http://{}/registered_functions'.format(addr), headers={'Authorization': password}).json()

            if func.__name__ not in worker_functions or worker_functions[func.__name__]["bytecode"] != func_bytecode:
                raise ValueError(f"Function {func.__name__} in worker {addr} has different bytecode compared to local function")
            break
        except:
            logger.info("worker unavailable {}".format(res1[0]['address']))
            res1.pop(0)

    if len(res1) == 0:
        logger.info("No worker or broker available")
        return None, None
    return res1[0]['address'].split(":")


def get_available_brokers(local_port):
    logger = logging.getLogger(__name__)
    res1 = []
    try:
        res1 = requests.get('http://localhost:{}/node_states'.format(local_port)).json()  # will get the data defined above
        res1 = [item for item in res1 if 'broker' in item['node_type']]
        if len(res1) == 0:
            logger.info("No broker available")
            return res1
        res1 = sorted(res1, key=lambda x: x['workload'])
    except:
        logger.info(traceback.format_exc())

    # workload for client
    #   client will send file to brokers with workers waiting
    # workload for worker
    #   worker will fetch from brokers with most work to do (i.e. workload 0)
    # a client will not be able to update the the workload of a broker!!!!!
    return res1


def find_required_args():
    necessary_args = ['db', 'col', 'filter', 'mongod_port', 'password', 'key_interpreter']
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
    filter_ = actual_args['filter']
    p2p_push_update_one(actual_args['mongod_port'], actual_args['db'], actual_args['col'], filter_, update_, password=actual_args['password'])


def default_saving_func(filepath, item):
    pickle.dump(item, open(filepath, 'wb'))


def p2p_save(key, item, filesuffix=".pkl", saving_func=default_saving_func):
    actual_args = find_required_args()
    fp = p2p_getfilepath(suffix=filesuffix)
    document = {key: fp}
    saving_func(fp, item)
    update_one(actual_args['mongod_port'], actual_args['db'], actual_args['col'], actual_args['filter'], document, upsert=False)


def default_loading_func(filepath):
    pickle.load(open(filepath, 'rb'))


def p2p_load(key, loading_func=default_loading_func):
    actual_args = find_required_args()
    item = find(actual_args['mongod_port'], actual_args['db'], actual_args['col'], actual_args['filter'], actual_args['key_interpreter'])[0]
    if key in item:
        return loading_func(item[key])
    else:
        return None


def p2p_getfilepath(suffix):
    filepath = tempfile.mkstemp(suffix=suffix, dir=None, text=False)[1]
    actual_args = find_required_args()
    key = "tmpfile"
    item = find(actual_args['mongod_port'], actual_args['db'], actual_args['col'], actual_args['filter'], actual_args['key_interpreter'])[0]
    count = 0
    while key in item:
        key += str(count)
        count += 1
    document = {key: filepath}
    update_one(actual_args['mongod_port'], actual_args['db'], actual_args['col'], actual_args['filter'], document,
               upsert=False)
    return filepath


def p2p_dictionary_update(update_dictionary):
    """
    Arbitrary dictionaries can be passed in order to update the database.
    Some keyworks are not allowed such as identifier, timestamp, nodes etc (TODO complete the list of forbidden args)
    """
    assert all(key not in update_dictionary for key in ["nodes", "timestamp", "identifier", "id_", "remote_identifier"])
    actual_args = find_required_args()
    filter_ = {"identifier": actual_args['identifier']}
    p2p_push_update_one(actual_args['db_url'], actual_args['db'], actual_args['col'], filter_, update_dictionary, password=actual_args['password'])


def get_remote_future(f, identifier, cache_path, mongod_port, db, col, key_interpreter_dict, password, jobs):
    logger = logging.getLogger(__name__)
    up_dir = os.path.join(cache_path, db)
    if not os.path.exists(up_dir):
        os.mkdir(up_dir)
    item = find(mongod_port, db, col, {"identifier": identifier}, key_interpreter_dict)[0]
    expected_keys = inspect.signature(f).return_annotation
    expected_keys_list = list(expected_keys.keys())
    expected_keys_list.append("progress")
    expected_keys_list.append("error")
    if any(item[k] is None for k in expected_keys):
        hint_file_keys = [k for k, v in expected_keys.items() if v == io.IOBase]

        filter_ = {"identifier": identifier, "remote_identifier": item['remote_identifier']}
        fsf = frozenset(filter_.items())
        while fsf in jobs and jobs[fsf].is_alive():
            time.sleep(4)
            logger.info("Waiting for uploading job to finish")
            # what if due to expiration on broker, the upload didn't even finish??
            # even if the item is expired on broker. the upload part is guaranteed to finish. as long as user does not
            # terminate upload

        search_filter = {"$or": [{"identifier": identifier}, {"identifier": item['remote_identifier']}]} # TODO I think it has to be
        #  {"$or": [{"identifier": identifier}, {"remote_identifier": item['remote_identifier']}]}
        p2p_pull_update_one(mongod_port, db, col, search_filter, expected_keys_list,
                            deserializer=partial(deserialize_doc_from_net,
                                                 up_dir=up_dir, key_interpreter=key_interpreter_dict),
                            hint_file_keys=hint_file_keys,
                            password=password)

    item = find(mongod_port, db, col, {"identifier": identifier}, key_interpreter_dict)[0]
    item = {k: item[k] for k in expected_keys_list}
    return item


class Future:

    def __init__(self, get_future_func, restart_func, terminate_func, delete_func, check_termination_func, check_deletion_func, expected_return_keys):
        self.expected_return_keys = expected_return_keys
        self.expected_return_keys.remove('error')
        self.__get_future_func = get_future_func
        self.__restart_func = restart_func
        self.__terminate_func = terminate_func
        self.__delete_func = delete_func
        self.__check_function_termination = check_termination_func
        self.__check_function_deletion = check_deletion_func

    def get(self, timeout=3600*24):
        logger = logging.getLogger(__name__)

        item = self.__get_future_func()
        count_time = 0
        wait_time = 4
        while any(item[k] is None for k in self.expected_return_keys):
            item = self.__get_future_func()
            if 'error' in item and item['error'] != None:
                print("error, maybe the item is missing on broker due to expiration, or anything else")
                raise Exception(str(item))
            time.sleep(wait_time)
            count_time += wait_time
            if count_time > timeout:
                raise TimeoutError("Waiting time exceeded")
        logger.info("Not done yet " + str(item))

        return item

    def restart(self):
        self.__terminate_func()
        self.__restart_func()

    def delete(self):
        self.__terminate_func()
        # wait termination on clientworker
        max_trials = 10
        while True:
            if max_trials==0:
                raise ValueError("Unable to terminate function")
            res = self.__check_function_termination()
            if res.json()['status'] == True:
                break
            time.sleep(3)

        self.__delete_func()
        # wait deletion on both clientworker and on brokerworker
        max_trials = 10
        while True:
            print("Waiting for deletion")
            if max_trials == 0:
                raise ValueError("Unable to delete function")
            res = self.__check_function_deletion()
            if res.json()['status'] == True:
                break
            time.sleep(3)


class P2PClientArguments:
    def __init__(self, kwargs, expected_return_keys):
        self.p2parguments = P2PArguments(list(kwargs.keys()), expected_return_keys)
        self.p2parguments.kwargs.update(kwargs)
        self.remote_args_identifier = None

    def object2doc(self):
        function_call_properties = self.p2parguments.object2doc()
        function_call_properties['remote_identifier'] = self.remote_args_identifier
        return function_call_properties

    def doc2object(self, document):
        self.p2parguments.doc2object(document)
        self.remote_args_identifier = document['remote_identifier']

    def create_generic_filter(self):
        filter_ = {"identifier": self.p2parguments.args_identifier,
                   "remote_identifier": self.remote_args_identifier}
        return filter_


class P2PClientFunction:
    def __init__(self, original_function: Callable, mongod_port, crypt_pass, local_port):
        self.p2pfunction = P2PFunction(original_function, mongod_port, crypt_pass)
        self.local_port = local_port
        self.__salt_pepper_identifier = 1

    def validate_arguments(self, args, kwargs):
        """
        After the function has been called, it's actual arguments are checked for some other constraints.
        For example all arguments must be specified as keyword arguments,
        All arguments must be instances of the declared type.
        # TODO to prevent security issues maybe the type(arg) == declared type, not isinstance(arg, declared_type)
        """
        if len(args) != 0:
            raise ValueError("All arguments to a function in this p2p framework need to be specified by keyword arguments")

        # check that every value passed in this function has the same type as the one declared in function annotation
        f_param_sign = inspect.signature(self.p2pfunction.original_function).parameters
        assert set(f_param_sign.keys()) == set(kwargs.keys())
        for k, v in kwargs.items():
            f_param_k_annotation = f_param_sign[k].annotation
            if not isinstance(v, f_param_k_annotation):
                raise ValueError(
                    f"class of value {v} for argument {k} is not the same as the annotation {f_param_k_annotation}")
        for value in P2PFunction.restricted_values:
            if value in kwargs.values():
                raise ValueError(f"'{value}' string is a reserved value in this p2p framework. It helps "
                                 "identifying a file when serializing together with other arguments")
        files = [v for v in kwargs.values() if isinstance(v, io.IOBase)]
        if len(files) > 1:
            raise ValueError("p2p framework does not currently support sending more than one file")
        if files:
            if any(file.closed or file.mode != 'rb' or file.tell() != 0 for file in files):
                raise ValueError("all files should be opened in read binary mode and pointer must be at start")

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
            collection = find(self.p2pfunction.mongod_port, self.p2pfunction.db_name, self.p2pfunction.db_collection,
                              {"identifier": identifier}, self.p2pfunction.args_interpreter)
            if len(collection) == 0:
                # we found an identifier that is not in DB
                return identifier
            elif len(collection) != 1:
                # we should find only one doc that has the same hash
                raise ValueError("Multiple documents for a hash")
            elif all(kicomp(kwargs[k]) == kicomp(item[k]) for item in collection for k in kwargs):
                # we found exactly 1 doc with the same hash and we must check that it has the same arguments
                return identifier
            else:
                # we found different arguments that produce the same hash so we must modify the hash determinastically
                identifier = identifier_original + str(self.__salt_pepper_identifier)
                self.__salt_pepper_identifier += 1
                if self.__salt_pepper_identifier > 100:
                    raise ValueError("Too many hash collisions. Change the hash function")

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
                if count > 100:
                    raise ValueError("Too many hash collisions. Change the hash function")

    def start_remote(self, ip, port, p2pclientarguments):
        """
        """
        filter_ = p2pclientarguments.create_generic_filter()

        serializable_document = p2pclientarguments.object2doc()

        nodes = [str(ip) + ":" + str(port)]
        p2p_insert_one(self.p2pfunction.mongod_port, self.p2pfunction.db_name, self.p2pfunction.db_collection,
                       serializable_document, nodes,
                       current_address_func=partial(self_is_reachable, self.local_port),
                       password=self.p2pfunction.crypt_pass, do_upload=True)
        call_remote_func(ip, port, self.p2pfunction.db_name, self.p2pfunction.db_collection, self.p2pfunction.function_name,
                         filter_, self.p2pfunction.crypt_pass)

    def register_arguments_in_db(self, p2pclientarguments, nodes):
        serializable_document = p2pclientarguments.object2doc()
        p2p_insert_one(self.p2pfunction.mongod_port, self.p2pfunction.db_name,
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

        collection = find(self.p2pfunction.mongod_port,
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


def wait_for_discovery(local_port):
    """
    Try at most max_trials times to connect to p2p network or until the list of node si not empty
    """
    logger = logging.getLogger(__name__)
    max_trials = 3
    count = 0
    while count < max_trials:
        res = requests.get('http://localhost:{}/node_states'.format(local_port)).json()  # will get the data defined above
        if len(res) != 0:
            break
        logger.warning("No peers found. Waiting for nodes")
        count += 1
        time.sleep(5)


class P2PClientApp(P2PFlaskApp):

    def __init__(self, discovery_ips_file, cache_path, local_port=5000, mongod_port=5100, password=""):
        configure_logger("client", module_level_list=[(__name__, 'DEBUG')])
        super(P2PClientApp, self).__init__(__name__, local_port=local_port, discovery_ips_file=discovery_ips_file, mongod_port=mongod_port,
                                                 cache_path=cache_path, password=password)
        self.roles.append("client")
        self.jobs = dict()
        self.background_server = None



    def terminate_remote(self, filter_, db, col, ki, funcname):
        item = find(self.mongod_port, db, col, filter_, ki)[0]
        frozenset_filter = frozenset(filter_.items())
        ip, port = item['nodes'][0].split(":")

        if frozenset_filter in self.jobs:
            if self.jobs[frozenset_filter].is_alive():
                self.jobs[frozenset_filter].terminate()
            terminate_remote_func(ip, port, db, col, funcname, filter_, self.crypt_pass)

    def delete_remote(self, filter_, db, col, ki, funcname):
        self.terminate_remote(filter_, db, col, ki, funcname)
        item = find(self.mongod_port, db, col, filter_, ki)[0]
        ip, port = item['nodes'][0].split(":")
        delete_remote_func(ip, port, db, col, funcname, filter_, self.crypt_pass)

    def create_future(self, p2pclientfunction, identifier):
        # key_interpreter, db, col = derive_vars_from_function(f)
        item = find(p2pclientfunction.p2pfunction.mongod_port,
                    p2pclientfunction.p2pfunction.db_name,
                    p2pclientfunction.p2pfunction.db_collection,
                    {"identifier": identifier},
                    p2pclientfunction.p2pfunction.args_interpreter)[0]

        filter = {"identifier": identifier, "remote_identifier": item['remote_identifier']}
        ip, port = item['nodes'][0].split(":")
        return Future(
            partial(get_remote_future, p2pclientfunction.p2pfunction.original_function, identifier, self.cache_path, self.mongod_port, p2pclientfunction.p2pfunction.db_name, p2pclientfunction.p2pfunction.db_collection, p2pclientfunction.p2pfunction.args_interpreter, self.crypt_pass, self.jobs),
            restart_func=partial(call_remote_func, ip, port, p2pclientfunction.p2pfunction.db_name, p2pclientfunction.p2pfunction.db_collection, p2pclientfunction.p2pfunction.function_name, filter, self.crypt_pass),
            terminate_func=partial(self.terminate_remote, filter,p2pclientfunction.p2pfunction.db_name, p2pclientfunction.p2pfunction.db_collection, p2pclientfunction.p2pfunction.args_interpreter, p2pclientfunction.p2pfunction.function_name),
            delete_func=partial(self.delete_remote, filter, p2pclientfunction.p2pfunction.db_name, p2pclientfunction.p2pfunction.db_collection, p2pclientfunction.p2pfunction.args_interpreter, p2pclientfunction.p2pfunction.function_name),
            check_termination_func=partial(check_function_termination, ip, port, p2pclientfunction.p2pfunction.db_name, p2pclientfunction.p2pfunction.db_collection, p2pclientfunction.p2pfunction.function_name, filter, self.crypt_pass),
            check_deletion_func=partial(check_function_deletion, ip, port, p2pclientfunction.p2pfunction.db_name, p2pclientfunction.p2pfunction.db_collection, p2pclientfunction.p2pfunction.function_name, filter, self.crypt_pass),
            expected_return_keys=p2pclientfunction.p2pfunction.expected_return_keys
        )

    def register_p2p_func(self, can_do_locally_func=lambda: False):
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
            p2pclientfunction = P2PClientFunction(f, self.mongod_port, self.crypt_pass, self.local_port)
            self.add_to_super_register(p2pclientfunction.p2pfunction)

            @wraps(f)
            def wrap(*args, **kwargs):
                logger = logging.getLogger(__name__)

                p2pclientfunction.validate_arguments(args, kwargs)
                p2pclientarguments = P2PClientArguments(kwargs, p2pclientfunction.p2pfunction.expected_return_keys)
                p2pclientarguments.p2parguments.args_identifier = p2pclientfunction.create_arguments_identifier(kwargs)
                if p2pclientfunction.arguments_already_submited(p2pclientarguments):
                    logger.info("Returning future that may already be precomputed")
                    return self.create_future(p2pclientfunction, p2pclientarguments.p2parguments.args_identifier)

                lru_ip, lru_port = select_lru_worker(self.local_port, f, self.crypt_pass)
                if lru_ip is None:
                    raise ValueError("No broker found")

                p2pclientarguments.remote_args_identifier = p2pclientfunction.create_arguments_remote_identifier(
                    p2pclientarguments.p2parguments.args_identifier,
                    lru_ip, lru_port
                )
                nodes = [str(lru_ip) + ":" + str(lru_port)]

                p2pclientfunction.register_arguments_in_db(p2pclientarguments, nodes)
                # TODO it seems redunant. here it does not upload. it only registers locally.

                logger.info("Dispacthed function work to {},{}".format(lru_ip, lru_port))

                #The reason why start remote must be a subprocess is because the uploading job might take time
                p = multiprocessing.Process(
                    target=partial(p2pclientfunction.start_remote, lru_ip, lru_port, p2pclientarguments))
                p.daemon = True
                p.start()
                self.jobs[frozenset(p2pclientarguments.create_generic_filter().items())] = p
                return self.create_future(p2pclientfunction, p2pclientarguments.p2parguments.args_identifier)

            return wrap

        return inner_decorator

def create_p2p_client_app(discovery_ips_file, cache_path, local_port=5000, mongod_port=5100, password="", processes=3):
    p2p_client_app = P2PClientApp(discovery_ips_file=discovery_ips_file, local_port=local_port, mongod_port=mongod_port,
                                  cache_path=cache_path, password=password)
    p2p_client_app.background_server = ServerThread(p2p_client_app, processes=processes)
    p2p_client_app.background_server.start()
    wait_until_online(p2p_client_app.local_port, p2p_client_app.crypt_pass)
    wait_for_discovery(p2p_client_app.local_port)
    return p2p_client_app
