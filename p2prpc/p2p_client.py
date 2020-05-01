from .base import P2PFlaskApp, validate_arguments, create_bookkeeper_p2pblueprint, is_debug_mode
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
from .base import derive_vars_from_function
from .registry_args import hash_kwargs, db_encoder
from .p2p_brokerworker import check_remote_identifier
from .p2pdata import find
from .registry_args import kicomp
from werkzeug.serving import make_server
from .base import configure_logger
import os
import traceback
from collections import defaultdict, Callable
from p2prpc.p2pdata import update_one
import tempfile
import pickle
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
        res = requests.get('http://localhost:{}/node_states'.format(local_port)).json()  # will get the data defined above
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


def get_local_future(f, identifier, cache_path, mongod_port, db, col, key_interpreter_dict):
    expected_keys = inspect.signature(f).return_annotation
    expected_keys_list = list(expected_keys.keys())
    expected_keys_list.append("progress")
    expected_keys_list.append("error")

    item = find(mongod_port, db, col, {"identifier": identifier}, key_interpreter_dict)[0]
    item = {k: v for k, v in item.items() if k in expected_keys_list}
    return item


class Future:

    def __init__(self, get_future_func, restart_func, terminate_func, delete_func):
        self.__get_future_func = get_future_func
        self.__restart_func = restart_func
        self.__terminate_func = terminate_func
        self.__delete_func = delete_func

    def get(self, timeout=3600*24):
        logger = logging.getLogger(__name__)

        item = self.__get_future_func()
        count_time = 0
        wait_time = 4
        while any(item[k] is None for k in item):
            item = self.__get_future_func()
            if 'error' in item and item['error'] != '':
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


def get_expected_keys(f):
    """
    By inspecting the the function signature we can get a dictionary with the arguments (because all are annotated)
    and a dictionary about the return (because this is required by the p2p framework)
    The combined dictionary will be stored in the database
    """
    expected_keys = inspect.signature(f).return_annotation
    expected_keys = {k: None for k in expected_keys}
    expected_keys['progress'] = 0
    expected_keys['error'] = ""
    return expected_keys


def identifier_seen(mongod_port, identifier, db, col, expected_keys, key_interpreter, time_limit=24):
    """
    Returns boolean about if the current arguments resulted in an identifier that was already seen
    """
    logger = logging.getLogger(__name__)

    collection = find(mongod_port, db, col, {"identifier": identifier}, key_interpreter)

    if collection:
        assert len(collection) == 1
        item = collection[0]
        if (time.time() - item['timestamp']) > time_limit * 3600 and any(item[k] is None for k in expected_keys):
            # TODO the entry should actually be deleted instead of letting it be overwritten
            #  for elegancy
            logger.info("Time limit exceeded for item with identifier: "+item['identifier'])
            return False
        else:
            return True
    else:
        return False


def create_identifier(mongod_port, db, col, kwargs, key_interpreter_dict):
    """
    A hash from the arguments will be created that will help to easily identify the set of arguments for later retrieval

    Args:
        db_url, db, col: arguments for tinymongo db
        kwargs: the provided function keyword arguments
        key_interpreter_dict: dictionary containing key and the expected data type. Used for decoding arguments from the
            database in case the initial created identifier already exists (for different reasons)

    Returns:
        identifier: string
    """
    identifier = hash_kwargs({k:v for k, v in kwargs.items() if k in key_interpreter_dict})
    identifier_original = identifier  # deepcopy

    count = 1
    while True:
        collection = find(mongod_port, db, col, {"identifier": identifier}, key_interpreter_dict)
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
            identifier = identifier_original + str(count)
            count += 1
            if count > 100:
                raise ValueError("Too many hash collisions. Change the hash function")


def create_remote_identifier(local_identifier, check_remote_identifier_args):
    """
    A remote identifier is created in order to prevent name collisions in worker nodes.
    """
    original_local_identifier = local_identifier
    args = {k: v for k, v in check_remote_identifier_args.items()}
    count = 1
    while True:
        args['identifier'] = local_identifier
        if check_remote_identifier(**args):
            return local_identifier
        else:
            local_identifier = original_local_identifier + str(count)
            count += 1
            if count > 100:
                raise ValueError("Too many hash collisions. Change the hash function")


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
        self.registry_functions = defaultdict(dict)

    def start_local(self, newf, identifier):
        p = multiprocessing.Process(target=newf)
        p.daemon = True
        p.start()
        self.jobs[identifier] = p

    def terminate_local(self, identifier):
        if identifier in self.jobs and self.jobs[identifier].is_alive():
            self.jobs[identifier].terminate()

    def delete_local(self, identifier, original_func):
        self.terminate_local(identifier)
        key_interpreter, db_name, col = derive_vars_from_function(original_func)
        mongodb = MongoClient(port=self.__mongod_port)['p2p']
        item = find(self.mongod_port, db_name, col, identifier)[0]
        document = deserialize_doc_from_db(item, key_interpreter)
        remove_values_from_doc(document)
        mongodb[db_name].remove(item)

    def start_remote(self, lru_ip, lru_port, db, col, fname, filter_, ki):
        kwargs = find(self.mongod_port, db, col, filter_, ki)[0]
        kwargs_ = {k: v for k,v in kwargs.items() if k in ki}
        other_keys = ['identifier', 'remote_identifier', 'progress', 'error']
        kwargs_.update({k: kwargs[k] for k in other_keys})

        nodes = [str(lru_ip) + ":" + str(lru_port)]
        p2p_insert_one(self.mongod_port, db, col, kwargs_, nodes,
                       current_address_func=partial(self_is_reachable, self.local_port),
                       password=self.crypt_pass, do_upload=True)
        call_remote_func(lru_ip, lru_port, db, col, fname, filter_, self.crypt_pass)

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


    def create_future(self, f, identifier):
        key_interpreter, db, col = derive_vars_from_function(f)
        item = find(self.mongod_port, db, col, {"identifier": identifier}, key_interpreter)[0]
        if item['nodes'] or 'remote_identifier' in item:
            filter = {"identifier": identifier, "remote_identifier": item['remote_identifier']}
            ip, port = item['nodes'][0].split(":")
            return Future(
                partial(get_remote_future, f, identifier, self.cache_path, self.mongod_port, db, col, key_interpreter, self.crypt_pass, self.jobs),
                restart_func=partial(call_remote_func, ip, port, db, col, f.__name__, filter, self.crypt_pass),
                terminate_func=partial(self.terminate_remote, filter, db, col, key_interpreter, f.__name__),
                delete_func=partial(self.delete_remote, filter, db, col, key_interpreter, f.__name__))
        else:
            new_f = wraps(f)(partial(function_executor, f=f, filter={'identifier': item['identifier']},
                                     mongod_port=self.mongod_port, db=db, col=col,
                                     key_interpreter=key_interpreter,
                                     logging_queue=self._logging_queue if not is_debug_mode() else None,
                                     password=self.crypt_pass))
            return Future(partial(get_local_future, f, identifier, self.cache_path, self.mongod_port, db, col, key_interpreter),
                          restart_func=partial(self.start_local, new_f, identifier),
                          terminate_func=partial(self.terminate_local, identifier),
                          delete_func=partial(self.delete_local, identifier))

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
            if f.__name__ in self.registry_functions:
                raise ValueError(f"Function {f.__name__} already registered")
            key_interpreter, db, col = derive_vars_from_function(f)
            self.registry_functions[f.__name__]['original_func'] = f

            @wraps(f)
            def wrap(*args, **kwargs):
                logger = logging.getLogger(__name__)

                identifier = create_identifier(self.mongod_port, db, col, kwargs, key_interpreter)
                expected_keys = get_expected_keys(f)
                if identifier_seen(self.mongod_port, identifier, db, col, expected_keys, key_interpreter):
                    logger.info("Returning future that may already be precomputed")
                    return self.create_future(f, identifier)

                validate_arguments(f, args, kwargs)
                kwargs.update(expected_keys)
                kwargs['identifier'] = identifier

                lru_ip, lru_port = select_lru_worker(self.local_port, f, self.crypt_pass)

                if can_do_locally_func() or lru_ip is None:
                    nodes = []
                    p2p_insert_one(self.mongod_port, db, col, kwargs, nodes, self.crypt_pass)
                    new_f = wraps(f)(partial(function_executor, f=f, filter={'identifier': kwargs['identifier']},
                                             mongod_port=self.mongod_port, db=db, col=col,
                                             key_interpreter=key_interpreter,
                                             # FIXME this if statement in case of debug mode was introduced just for an unfortunated combination of OS
                                             #  and PyCharm version when variables in watch were hanging with no timeout just because of multiprocessing manaeger
                                             logging_queue=self._logging_queue if not is_debug_mode() else None,
                                             password=self.crypt_pass))
                    self.start_local(new_f, kwargs['identifier'])
                    logger.info("Executing function locally")
                else:
                    # TODO put all of this in a job
                    nodes = [str(lru_ip) + ":" + str(lru_port)]
                    kwargs['remote_identifier'] = create_remote_identifier(kwargs['identifier'],
                                                                           {"ip": lru_ip, "port": lru_port, "db": db,
                                                                            "col": col, "func_name": f.__name__,
                                                                            "password": self.crypt_pass})
                    p2p_insert_one(self.mongod_port, db, col, kwargs, nodes,
                                   current_address_func=partial(self_is_reachable, self.local_port),
                                   password=self.crypt_pass, do_upload=False)

                    filter = {"identifier": identifier, "remote_identifier": kwargs['remote_identifier']}
                    logger.info("Dispacthed function work to {},{}".format(lru_ip, lru_port))
                    p = multiprocessing.Process(target=partial(self.start_remote, lru_ip, lru_port, db, col, f.__name__, filter, key_interpreter))
                    p.daemon = True
                    p.start()
                    self.jobs[frozenset(filter.items())] = p
                return self.create_future(f, identifier)

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
