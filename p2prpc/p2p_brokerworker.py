import requests
from .base import P2PFlaskApp, create_bookkeeper_p2pblueprint
import multiprocessing
from flask import make_response, jsonify
from .base import derive_vars_from_function
import time
from .p2pdata import password_required
from flask import request
from functools import wraps, partial
from .p2pdata import p2p_route_insert_one, deserialize_doc_from_net, p2p_route_pull_update_one, \
    p2p_route_push_update_one
from .p2pdata import find, p2p_push_update_one
from pymongo import MongoClient
import inspect
import logging
from json import dumps, loads
from .base import configure_logger
from collections import defaultdict
from passlib.hash import sha256_crypt
import os
from .p2pdata import deserialize_doc_from_db
from .registry_args import remove_values_from_doc


def call_remote_func(ip, port, db, col, func_name, filter, password):
    """
    Client wrapper function over REST Api call
    """
    data = {"filter_json": dumps(filter)}

    url = "http://{ip}:{port}/execute_function/{db}/{col}/{fname}".format(ip=ip, port=port, db=db, col=col,
                                                                          fname=func_name)
    res = requests.post(url, files={}, data=data, headers={"Authorization": password})
    return res


def check_remote_identifier(ip, port, db, col, func_name, identifier, password):
    url = "http://{ip}:{port}/identifier_available/{db}/{col}/{fname}/{identifier}".format(ip=ip, port=port, db=db,
                                                                                     col=col, fname=func_name,
                                                                                 identifier=identifier)
    res = requests.get(url, files={}, data={}, headers={"Authorization": password})
    if res.status_code == 200 and res.content == b'yes':
        return True
    elif res.status_code == 404 and res.content == b'no':
        return False
    else:
        raise ValueError("Problem")


def function_executor(f, filter, db, col, mongod_port, key_interpreter, logging_queue, password):
    qh = logging.handlers.QueueHandler(logging_queue)
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.handlers = []
    root.addHandler(qh)

    logger = logging.getLogger(__name__)

    kwargs_ = find(mongod_port, db, col, filter, key_interpreter)[0]
    kwargs = {k: kwargs_[k] for k in inspect.signature(f).parameters.keys()}

    logger.info("Executing function: " + f.__name__)
    print(kwargs)
    try:
        update_ = f(**kwargs)
    except Exception as e:
        logger.error("Function execution crashed for filter: {}".format(str(filter)), exc_info=True)
        raise e
    logger.info("Finished executing function: " + f.__name__)
    update_['finished'] = True
    update_['progress'] = 100.0
    if not all(isinstance(k, str) for k in update_.keys()):
        raise ValueError("All keys in the returned dictionary must be strings in func {}".format(f.__name__))

    try:
        p2p_push_update_one(mongod_port, db, col, filter, update_, password=password)
    except Exception as e:
        logger.error("Results pushing to server resulted in errors", exc_info=True)
        raise e
    return update_


def route_execute_function(f, mongod_port, db, col, key_interpreter, can_do_locally_func, self):
    """
    Function designed to be decorated with flask.app.route
    The function should be partially applied will all arguments

    # TODO identifier should be receives using a post request or query parameter or url parameter as in here
    #  most of the other routed functions are requesting post json

    Args from network:
        identifier: string will be received when there is a http call

    Args from partial application of the function:
        f: function to execute
        db_path, db, col are part of tinymongo db
        key_interpreter: dictionary containing keys and the expected data types
        can_do_locally_func: function that returns True or False and says if the current function should be executed or not
        self: P2PFlaskApp instance. this instance contains a worker pool and a list of futures #TODO maybe instead of self, only these arguments should be passed
    Returns:
        flask response that will contain the dictionary as a json in header metadata and one file (which may be an archive for multiple files)
    """
    logger = logging.getLogger(__name__)

    filter = loads(request.form['filter_json'])

    if can_do_locally_func():
        new_f = wraps(f)(
            partial(function_executor,
                    f=f, filter=filter,
                    mongod_port=mongod_port, db=db, col=col,
                    key_interpreter=key_interpreter,
                    logging_queue=self._logging_queue,
                    password=self.crypt_pass))
        res = self.worker_pool.apply_async(func=new_f)
        MongoClient(port=mongod_port)[db][col].update_one(filter, {"$set": {"started": f.__name__}})
        self.list_futures.append(res)
    else:
        logger.info("Cannot execute function now: " + f.__name__)

    return make_response("ok")


def route_search_work(mongod_port, db, collection, func_name, time_limit):
    logger = logging.getLogger(__name__)

    col = list(MongoClient(port=mongod_port)[db][collection].find({}))

    col = list(filter(lambda item: "finished" not in item, col))
    col1 = filter(lambda item: "started" not in item, col)
    col2 = filter(lambda item: "started" in item and item['started'] != func_name, col)
    col3 = filter(lambda item: "started" in item and item['started'] == func_name and (time.time() - item['timestamp']) > time_limit * 3600, col)

    col = list(col1) + list(col2) + list(col3)
    # list(col) + list(col2)
    # TODO fix this. it returns items that have finished
    if col:

        col.sort(key=lambda item: item["timestamp"])  # might allready be sorted
        item = col[0]
        filter_ = {"identifier": item["identifier"], "remote_identifier": item['remote_identifier']}
        logger.info("Node{}(possible client worker) will ask for filter: {}".format(request.remote_addr, str(filter_)))
        MongoClient(port=mongod_port)[db][collection].update_one(filter_, {"$set": {"started": func_name}})
        return jsonify({"filter": filter_})
    else:
        return jsonify({})


def route_identifier_available(mongod_port, db, col, identifier):
    """
    Function designed to be decorated with flask.app.route
    The function should be partially applied will all arguments

    The function will return a boolean about the received identifier already exists or not in the database

    Args from network:
        identifier: string will be received when there is a http call
    Args from partial application of the function:
        db_path, db, col are part of tinymongo db
    """
    collection = find(mongod_port, db, col, {"identifier": identifier})
    if len(collection) == 0:
        return make_response("yes", 200)
    else:
        return make_response("no", 404)

def heartbeat(mongod_port, db="tms"):
    """
    Pottential vulnerability from flooding here
    """
    collection = MongoClient(port=mongod_port)[db]["broker_heartbeats"]
    collection.insert_one({"time_of_heartbeat": time.time()})
    return make_response("Thank god you are alive", 200)


def delete_old_finished_requests(mongod_port, registry_functions, time_limit=24):
    db = MongoClient(port=mongod_port)['p2p']
    collection_names = set(db.collection_names()) - {"_default"}
    for col_name in collection_names:
        key_interpreter_dict = registry_functions[col_name]['key_interpreter']

        col_items = list(db[col_name].find({}))
        col_items = filter(lambda item: "finished" in item, col_items)
        col_items = filter(lambda item: (time.time() - item['timestamp']) > time_limit * 3600, col_items)

        for item in col_items:
            if (time.time() - item['timestamp']) > time_limit * 3600:
                document = deserialize_doc_from_db(item, key_interpreter_dict)
                remove_values_from_doc(document)
                db[col_name].remove(item)


class P2PBrokerworkerApp(P2PFlaskApp):

    def __init__(self, discovery_ips_file, cache_path, local_port=5001, mongod_port=5101, password="", old_requests_time_limit=23):
        configure_logger("brokerworker", module_level_list=[(__name__, 'DEBUG')])
        super(P2PBrokerworkerApp, self).__init__(__name__, local_port=local_port, discovery_ips_file=discovery_ips_file, mongod_port=mongod_port,
                                                 cache_path=cache_path, password=password)
        self.roles.append("brokerworker")
        self.registry_functions = defaultdict(dict)
        self.pass_req_dec = password_required(password)
        self.worker_pool = multiprocessing.Pool(2)
        self.list_futures = []
        self.register_time_regular_func(partial(delete_old_finished_requests,
                                                mongod_port=mongod_port,
                                                registry_functions=self.registry_functions,
                                                time_limit=old_requests_time_limit))

    def register_p2p_func(self, can_do_locally_func=lambda: True, time_limit=12):
        """
        In p2p brokerworker, this decorator will have the role of either executing a function that was registered (worker role), or store the arguments in a
         database in order to execute the function later by a clientworker (broker role).

        Args:
            self: P2PFlaskApp object this instance is passed as argument from create_p2p_client_app. This is done like that
                just to avoid making redundant Classes. Just trying to make the code more functional
            can_do_locally_func: function that returns True if work can be done locally and false if it should be done later
                by this current node or by a clientworker
            limit=hours
        """

        def inner_decorator(f):
            if f.__name__ in self.registry_functions:
                raise ValueError("Function name already registered")
            key_interpreter, db, col = derive_vars_from_function(f)

            self.registry_functions[f.__name__]['key_interpreter'] = key_interpreter

            updir = os.path.join(self.cache_path, db, col)  # upload directory
            os.makedirs(updir, exist_ok=True)

            # these functions below make more sense in p2p_data.py
            p2p_route_insert_one_func = wraps(p2p_route_insert_one)(
                partial(self.pass_req_dec(p2p_route_insert_one),
                        db=db, col=col, mongod_port=self.mongod_port,
                        deserializer=partial(deserialize_doc_from_net, up_dir=updir, key_interpreter=key_interpreter)))

            self.route("/insert_one/{db}/{col}".format(db=db, col=col), methods=['POST'])(p2p_route_insert_one_func)

            p2p_route_push_update_one_func = wraps(p2p_route_push_update_one)(
                partial(self.pass_req_dec(p2p_route_push_update_one),
                        mongod_port=self.mongod_port, db=db, col=col,
                        deserializer=partial(deserialize_doc_from_net, up_dir=updir, key_interpreter=key_interpreter)))
            self.route("/push_update_one/{db}/{col}".format(db=db, col=col), methods=['POST'])(
                p2p_route_push_update_one_func)

            p2p_route_pull_update_one_func = wraps(p2p_route_pull_update_one)(
                partial(self.pass_req_dec(p2p_route_pull_update_one),
                        mongod_port=self.mongod_port, db=db, col=col))
            self.route("/pull_update_one/{db}/{col}".format(db=db, col=col), methods=['POST'])(
                p2p_route_pull_update_one_func)

            execute_function_partial = wraps(f)(
                partial(self.pass_req_dec(route_execute_function),
                        f=f, mongod_port=self.mongod_port, db=db, col=col,
                        key_interpreter=key_interpreter, can_do_locally_func=can_do_locally_func, self=self))
            self.route('/execute_function/{db}/{col}/{fname}'.format(db=db, col=col, fname=f.__name__),
                       methods=['POST'])(execute_function_partial)

            search_work_partial = wraps(route_search_work)(
                partial(self.pass_req_dec(route_search_work),
                        mongod_port=self.mongod_port, db=db, collection=col,
                        func_name=f.__name__, time_limit=time_limit))
            self.route("/search_work/{db}/{col}/{fname}".format(db=db, col=col, fname=f.__name__), methods=['POST'])(
                search_work_partial)

            identifier_available_partial = wraps(route_identifier_available)(
                partial(self.pass_req_dec(route_identifier_available),
                        mongod_port=self.mongod_port, db=db, col=col))
            self.route("/identifier_available/{db}/{col}/{fname}/<identifier>".format(db=db, col=col, fname=f.__name__),
                       methods=['GET'])(identifier_available_partial)

        return inner_decorator
