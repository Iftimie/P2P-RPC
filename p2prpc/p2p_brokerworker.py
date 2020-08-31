from .globals import requests
from .base import P2PFlaskApp
from flask import make_response, jsonify
import time
from flask import request
from functools import wraps, partial
from .p2pdata import p2p_route_insert_one, deserialize_doc_from_net, p2p_route_pull_update_one, \
    p2p_route_push_update_one
from .p2pdata import find, p2p_push_update_one
from pymongo import MongoClient
import pymongo
import logging
from json import dumps, loads
from .base import configure_logger
import os
from .registry_args import remove_values_from_doc, db_encoder
import traceback
from collections import Callable
from .errors import Broker2ClientIdentifierNotFound, WorkerInvalidResults
from .base import P2PFunction
from .base import P2PArguments
from .base import P2PBlueprint
from .bookkeeper import route_node_states
from .bookkeeper import initial_discovery, update_function
import inspect


def call_remote_func(ip, port, db, col, func_name, filter, password):
    """
    Client wrapper function over REST Api call
    """
    logger = logging.getLogger(__name__)

    data = {"filter_json": dumps(filter)}

    url = "http://{ip}:{port}/execute_function/{db}/{col}/{fname}".format(ip=ip, port=port, db=db, col=col,
                                                                          fname=func_name)
    res = requests.post(url, files={}, data=data, headers={"Authorization": password})
    if res.status_code == 404 and b'Filter not found' in res.content:
        logger.error("Filter not found {} on broker {} {}".format(filter, ip, port))
        raise Broker2ClientIdentifierNotFound(func_name, filter)
    return res


def terminate_remote_func(ip, port, db, col, func_name, filter, password):
    data = {"filter_json": dumps(filter)}

    url = "http://{ip}:{port}/terminate_function/{db}/{col}/{fname}".format(ip=ip, port=port, db=db, col=col,
                                                                          fname=func_name)
    res = requests.post(url, files={}, data=data, headers={"Authorization": password})
    return res

def delete_remote_func(ip, port, db, col, func_name, filter, password):
    data = {"filter_json": dumps(filter)}

    url = "http://{ip}:{port}/delete_function/{db}/{col}/{fname}".format(ip=ip, port=port, db=db, col=col,
                                                                            fname=func_name)
    res = requests.post(url, files={}, data=data, headers={"Authorization": password})
    return res

def check_function_termination(ip, port, db, col, func_name, filter, password):
    logger = logging.getLogger(__name__)

    data = {"filter_json": dumps(filter)}

    url = "http://{ip}:{port}/check_function_termination/{db}/{col}/{fname}".format(ip=ip, port=port, db=db, col=col,
                                                                          fname=func_name)
    res = requests.post(url, files={}, data=data, headers={"Authorization": password})
    if res.status_code == 404 and b'Filter not found' in res.content:
        logger.error("Filter not found {} on broker {} {}".format(filter, ip, port))
        raise Broker2ClientIdentifierNotFound(func_name, filter)
    return res


def check_function_deletion(ip, port, db, col, func_name, filter, password):
    data = {"filter_json": dumps(filter)}

    url = "http://{ip}:{port}/check_function_deletion/{db}/{col}/{fname}".format(ip=ip, port=port, db=db, col=col,
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


def check_function_stats(ip, port, password):
    url = "http://{ip}:{port}/function_stats".format(ip=ip, port=port)
    res = requests.get(url, headers={"Authorization": password})
    if res.status_code == 200:
        return res.json()
    else:
        return []


class P2PBrokerArguments:
    def __init__(self, expected_keys, expected_return_keys):
        self.p2parguments = P2PArguments(expected_keys, expected_return_keys)
        self.started = None # TODO maybe this property should also stau in p2parguments
        self.kill_clientworker = None
        self.delete_clientworker = None
        self.timestamp = None
        self.remote_args_identifier = None

    def object2doc(self):
        function_call_properties = self.p2parguments.object2doc()
        function_call_properties['started'] = self.started
        function_call_properties['kill_clientworker'] = self.kill_clientworker
        function_call_properties['delete_clientworker'] = self.delete_clientworker
        return function_call_properties

    def doc2object(self, document):
        self.p2parguments.doc2object(document)
        self.timestamp = document['timestamp'] # should exist
        self.remote_args_identifier = document['remote_identifier']
        if "started" in document:
            self.started = document['started']
        if "kill_clientworker" in document:
            self.kill_clientworker = document['kill_clientworker']
        if 'delete_clientworker' in document:
            self.delete_clientworker = document['delete_clientworker']


class P2PBrokerFunction:
    def __init__(self, original_function: Callable, mongod_port, crypt_pass):
        self.p2pfunction = P2PFunction(original_function, mongod_port, crypt_pass)

    @property
    def function_name(self):
        return self.p2pfunction.function_name

    def load_arguments_from_db(self, filter_):
        items = find(self.p2pfunction.mongod_port, self.p2pfunction.db_name, self.p2pfunction.db_collection, filter_,
                     self.p2pfunction.args_interpreter)
        if len(items) == 0:
            return None
        p2pbrokerarguments = P2PBrokerArguments(self.p2pfunction.expected_keys, self.p2pfunction.expected_return_keys)
        p2pbrokerarguments.doc2object(items[0])
        return p2pbrokerarguments

    def list_all_arguments(self):
        logger = logging.getLogger(__name__)

        mongodport = self.p2pfunction.mongod_port
        db_name = self.p2pfunction.db_name
        db_collection = self.p2pfunction.db_collection
        p2pbrokerarguments = []
        try:
            for item in MongoClient(port=mongodport)[db_name][db_collection].find({}):
                p2pbrokerargument = P2PBrokerArguments(self.p2pfunction.expected_keys,
                                                        self.p2pfunction.expected_return_keys)
                p2pbrokerargument.doc2object(item)
                p2pbrokerarguments.append(p2pbrokerargument)
        except pymongo.errors.AutoReconnect:
            logger.info("Unable to connect to mongo")

        return p2pbrokerarguments

    def update_arguments_in_db(self, filter, keylist, p2pbrokerarguments):
        serializable_document = p2pbrokerarguments.object2doc()
        newvalues = {k:serializable_document[k] for k in keylist}
        MongoClient(port=self.p2pfunction.mongod_port)[self.p2pfunction.db_name][self.p2pfunction.db_collection].update_one(
            filter, {"$set": newvalues})

    def remove_arguments_from_db(self, p2pbrokerarguments):

        remove_values_from_doc(p2pbrokerarguments.object2doc())

        filter_ = {"identifier": p2pbrokerarguments.p2parguments.args_identifier,
                   'remote_identifier': p2pbrokerarguments.remote_args_identifier}

        MongoClient(port=self.p2pfunction.mongod_port)[self.p2pfunction.db_name][
            self.p2pfunction.db_collection].delete_one(filter_)


def function_executor(p2pworkerfunction, p2pworkerarguments, logging_queue):
    """
    """
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.handlers = []
    if logging_queue is not None:
        # FIXME this if statement in case of debug mode was introduced just for an unfortunated combination of OS
        #  and PyCharm version when variables in watch were hanging with no timeout just because of multiprocessing manaeger
        qh = logging.handlers.QueueHandler(logging_queue)
        root.addHandler(qh)

    logger = logging.getLogger(__name__)

    function_name = p2pworkerfunction.p2pfunction.function_name
    interpreted_kwargs = p2pworkerarguments.p2parguments.kwargs
    original_function = p2pworkerfunction.p2pfunction.original_function
    logger.info("Executing function: " + p2pworkerfunction.p2pfunction.function_name)
    filter_ = {'identifier': p2pworkerarguments.p2parguments.args_identifier,
               'remote_identifier': p2pworkerarguments.remote_args_identifier}
    try:
        update_ = original_function(**interpreted_kwargs)
    except Exception as e:
        log_message = "Function execution crashed for filter: {}\n".format(str(filter_)) + traceback.format_exc()
        logger.error(log_message)
        p2p_push_update_one(p2pworkerfunction.p2pfunction.mongod_port,
                            p2pworkerfunction.p2pfunction.db_name,
                            p2pworkerfunction.p2pfunction.db_collection,
                            filter_, {"error": log_message},
                            password=p2pworkerfunction.p2pfunction.crypt_pass)
        raise e
    logger.info("Finished executing function: " + function_name)
    update_['progress'] = 100.0
    update_['started'] = 'terminated'
    return_anno = inspect.signature(original_function).return_annotation
    for k in return_anno:
        if k not in update_:
            raise WorkerInvalidResults(p2pworkerfunction.p2pfunction, p2pworkerarguments.p2parguments,
                                   f"Key {k} is missing from result dictionary")

    for k, class_ in return_anno.items():
        if not isinstance(update_[k], class_):
            raise WorkerInvalidResults(p2pworkerfunction.p2pfunction, p2pworkerarguments.p2parguments,
                                 f"Class of value {update_[k]} for result key {k} is not the same as the annotation {class_}")

    try:
        p2p_push_update_one(p2pworkerfunction.p2pfunction.mongod_port,
                            p2pworkerfunction.p2pfunction.db_name,
                            p2pworkerfunction.p2pfunction.db_collection,
                            filter_, update_,
                            password=p2pworkerfunction.p2pfunction.crypt_pass)
    except Exception as e:
        logger.error("Results pushing to server resulted in errors", exc_info=True)
        raise e
    return update_


def route_terminate_function(p2pbrokerfunction):
    logger = logging.getLogger(__name__)

    filter = loads(request.form['filter_json'])
    p2pbrokerarguments = p2pbrokerfunction.load_arguments_from_db(filter)
    if p2pbrokerarguments is None:
        return make_response("Filter not found {} on broker".format(filter), 404)
    p2pbrokerarguments.kill_clientworker = True
    p2pbrokerfunction.update_arguments_in_db(filter, ['kill_clientworker'], p2pbrokerarguments)
    # must be a clientworker that is doing the job
    logger.info("job is on clientworker {} added notification for termination".format(filter))
    return make_response("ok", 200)


def route_delete_function(p2pbrokerfunction):
    logger = logging.getLogger(__name__)

    filter = loads(request.form['filter_json'])
    p2pbrokerarguments = p2pbrokerfunction.load_arguments_from_db(filter)
    if p2pbrokerarguments is None:
        return make_response("Filter not found {} on broker".format(filter), 404)
    p2pbrokerarguments.delete_clientworker = True
    p2pbrokerfunction.update_arguments_in_db(filter, ['delete_clientworker'], p2pbrokerarguments)
    logger.info("job is on clientworker {} added notification for deletion".format(filter))
    return make_response("ok", 200)


def route_check_function_termination(p2pbrokerfunction):
    filter = loads(request.form['filter_json'])
    p2pbrokerarguments = p2pbrokerfunction.load_arguments_from_db(filter)
    if p2pbrokerarguments is None:
        return make_response("Filter not found {} on broker".format(filter), 404)

    if p2pbrokerarguments.started == 'terminated' and p2pbrokerarguments.kill_clientworker is False:
        return jsonify({"status": True})
    else:
        return jsonify({"status": False})


def route_check_function_deletion(p2pbrokerfunction):
    filter = loads(request.form['filter_json'])
    p2pbrokerarguments = p2pbrokerfunction.load_arguments_from_db(filter)
    if p2pbrokerarguments is None:
        return make_response("Filter not found {} on broker".format(filter), 404)

    # IF job is on client worker, then a remainder of the job must still exist here
    # TODO small remainings should still exist, but the big data is important to be deleted

    # there should be some mechanism so that a document is deleted only after the clientworker deleted it
    if p2pbrokerarguments.started=='terminated' and p2pbrokerarguments.delete_clientworker is False:
        # we need to check that delete_clientworker is False because before the call of function_deletion, a signal was sent as
        # delete_clientworker is True so that the clientworker can delete it
        p2pbrokerfunction.remove_arguments_from_db(p2pbrokerarguments)
        return jsonify({"status": True})
    else:
        return jsonify({"status": False})

def route_execute_function(p2pbrokerfunction):
    """
    Function designed to be decorated with flask.app.route
    The function is partially applied will arguments

    # TODO identifier should be receives using a post request or query parameter or url parameter as in here
    #  most of the other routed functions are requesting post json

    Args from network:
        identifier: string will be received when there is a http call

    Args from partial application of the function:
        p2pbrokerfunction: function to execute
    Returns:
        status code
    """
    logger = logging.getLogger(__name__)
    filter = loads(request.form['filter_json'])
    p2pbrokerarguments = p2pbrokerfunction.load_arguments_from_db(filter)
    if p2pbrokerarguments is None:
        return make_response("Filter not found {} on broker".format(filter), 404)

    p2pbrokerarguments.started='available'
    p2pbrokerarguments.kill_clientworker = False
    p2pbrokerarguments.delete_clientworker = False
    p2pbrokerfunction.update_arguments_in_db(filter, ['started', 'kill_clientworker', 'delete_clientworker'], p2pbrokerarguments)
    # TODO I should probably make the item available for processing
    # In fact if there is not any key started in the dictionary, then it means that it is available for processing
    # check route_search_work

    logger.info("Updated status to available for processing: " + p2pbrokerfunction.p2pfunction.function_name)
    #I should replace the started key with some status. but i know I have to update in many other parts
    # the fact that started is a reserved word
    # I also have to modify route_search_work

    return make_response("ok")


def route_search_work(p2pbrokerfunction):
    logger = logging.getLogger(__name__)

    p2pbrokerarguments = [a for a in p2pbrokerfunction.list_all_arguments() if a.started=='available']

    if not p2pbrokerarguments:
        return jsonify({})

    p2pbrokerarguments.sort(key=lambda arg: arg.timestamp)
    p2pbrokerargument = p2pbrokerarguments[0]
    filter_ = {"identifier": p2pbrokerargument.p2parguments.args_identifier,
               "remote_identifier": p2pbrokerargument.remote_args_identifier}
    p2pbrokerargument.started = 'in_progress'
    p2pbrokerfunction.update_arguments_in_db(filter_, ['started'], p2pbrokerargument)
    logger.info("Node{}(possible client worker) will ask for filter: {}".format(request.remote_addr, str(filter_)))
    return jsonify({"filter": filter_})


def route_identifier_available(p2pbrokerfunction, identifier):
    """
    Function designed to be decorated with flask.app.route
    The function should be partially applied will all arguments

    The function will return a boolean about the received identifier already exists or not in the database

    Args from network:
        identifier: string will be received when there is a http call
    Args from partial application of the function:
        p2pbrokerfunction
    """
    filter = {'identifier': identifier}
    p2pbrokerarguments = p2pbrokerfunction.load_arguments_from_db(filter)
    if p2pbrokerarguments is None:
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


def delete_old_requests(registry_functions, time_limit=24):
    logger = logging.getLogger(__name__)

    for p2pbrokerfunction in registry_functions.values():
        for p2pbrokerargument in p2pbrokerfunction.list_all_arguments():
            # if p2pbrokerargument.started == 'terminated' and include_finished:
            # I removed this as the function was terminating but the client didn't had the time to download the results
            #     p2pbrokerfunction.remove_arguments_from_db(p2pbrokerargument)
            if time.time() - p2pbrokerargument.timestamp > time_limit * 3600:
                p2pbrokerfunction.remove_arguments_from_db(p2pbrokerargument)
                logger.info(f"Removed {p2pbrokerargument.object2doc()}")



def route_registered_functions(registry_functions):
    current_items = {}
    for fname, p2pbrokerfunction in registry_functions.items():
        original_function = p2pbrokerfunction.p2pfunction.original_function
        value = {"bytecode": db_encoder[Callable](original_function)}
        current_items[fname] = value
    return jsonify(current_items)


def route_function_stats(registry_functions):
    ans = []
    for fname, p2pbrokerfunction in registry_functions.items():
        group = {}
        function_details = {}
        function_details["function_name"]=p2pbrokerfunction.p2pfunction.function_name
        function_details["function_code"]=inspect.getsource(p2pbrokerfunction.p2pfunction.original_function)
        p2pbrokerarguments = p2pbrokerfunction.list_all_arguments()
        p2pbrokerarguments = [arg.object2doc() for arg in p2pbrokerarguments]
        group["function_details"]=function_details
        group["p2pbrokerarguments"] = p2pbrokerarguments
        ans.append(group)
    return jsonify(ans)


def create_bookkeeper_p2pblueprint(mongod_port) -> P2PBlueprint:
    """
    Creates the bookkeeper blueprint

    Args:
        local_port: integer
        app_roles:
        discovery_ips_file: path to file with initial configuration of the network. The file should contain a list with
            reachable addresses

    Return:
        P2PBluePrint
    """
    bookkeeper_bp = P2PBlueprint("bookkeeper_bp", __name__, role="bookkeeper")
    decorated_route_node_states = (wraps(route_node_states)(partial(route_node_states, mongod_port)))
    bookkeeper_bp.route("/node_states", methods=['POST', 'GET'])(decorated_route_node_states)

    time_regular_func = partial(update_function, mongod_port)
    bookkeeper_bp.register_time_regular_func(time_regular_func)

    return bookkeeper_bp


class P2PBrokerworkerApp(P2PFlaskApp):

    def __init__(self, discovery_ips_file, cache_path, local_port=5001, mongod_port=5101, password="", old_requests_time_limit=23):
        configure_logger("brokerworker", module_level_list=[(__name__, 'DEBUG')])
        super(P2PBrokerworkerApp, self).__init__(__name__, local_port=local_port, discovery_ips_file=discovery_ips_file, mongod_port=mongod_port,
                                                 cache_path=cache_path, password=password)
        self.roles.append("brokerworker")
        self.promises = []

        # FIXME this if else statement in case of debug mode was introduced just for an unfortunated combination of OS
        #  and PyCharm version when variables in watch were hanging with no timeout just because of multiprocessing manaegr
        self._initial_funcs.append(partial(initial_discovery, mongod_port, local_port, self.roles, discovery_ips_file, self.crypt_pass, isbroker=True))
        self.register_time_regular_func(partial(delete_old_requests,
                                                registry_functions=self.registry_functions,
                                                time_limit=old_requests_time_limit))
        bookkeeper_bp = create_bookkeeper_p2pblueprint(mongod_port=self.mongod_port)
        self.register_blueprint(bookkeeper_bp)

        registered_functions_partial = wraps(route_registered_functions)(
            partial(self.pass_req_dec(route_registered_functions),
                    registry_functions=self.registry_functions))
        self.route(f"/registered_functions/", methods=['GET'])(registered_functions_partial)

        function_stats_partial = wraps(route_function_stats)(
            partial(self.pass_req_dec(route_function_stats),
                    registry_functions=self.registry_functions))
        self.route(f"/function_stats/", methods=['GET'])(function_stats_partial)

    def register_p2p_func(self, time_limit=12):
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
            p2pbrokerfunction = P2PBrokerFunction(f, self.mongod_port, self.crypt_pass)
            self.add_to_super_register(p2pbrokerfunction)

            updir = os.path.join(self.cache_path,
                                 p2pbrokerfunction.p2pfunction.db_name,
                                 p2pbrokerfunction.p2pfunction.db_collection)  # upload directory
            db_name = p2pbrokerfunction.p2pfunction.db_name
            db_collection = p2pbrokerfunction.p2pfunction.db_collection
            function_name = p2pbrokerfunction.p2pfunction.function_name
            key_interpreter = p2pbrokerfunction.p2pfunction.args_interpreter
            os.makedirs(updir, exist_ok=True)

            # these functions below make more sense in p2p_data.py
            p2p_route_insert_one_func = wraps(p2p_route_insert_one)(
                partial(self.pass_req_dec(p2p_route_insert_one),
                        db=db_name, col=db_collection, mongod_port=self.mongod_port,
                        deserializer=partial(deserialize_doc_from_net, up_dir=updir, key_interpreter=key_interpreter)))
            p2p_route_insert_one_func.__name__ = p2p_route_insert_one_func.__name__ + db_collection
            self.route(f"/insert_one/{db_name}/{db_collection}", methods=['POST'])(p2p_route_insert_one_func)

            p2p_route_push_update_one_func = wraps(p2p_route_push_update_one)(
                partial(self.pass_req_dec(p2p_route_push_update_one),
                        mongod_port=self.mongod_port, db=db_name, col=db_collection,
                        deserializer=partial(deserialize_doc_from_net, up_dir=updir, key_interpreter=key_interpreter)))
            p2p_route_push_update_one_func.__name__ = p2p_route_push_update_one_func.__name__ + db_collection
            self.route(f"/push_update_one/{db_name}/{db_collection}", methods=['POST'])(
                p2p_route_push_update_one_func)

            p2p_route_pull_update_one_func = wraps(p2p_route_pull_update_one)(
                partial(self.pass_req_dec(p2p_route_pull_update_one),
                        mongod_port=self.mongod_port, db=db_name, col=db_collection))
            p2p_route_pull_update_one_func.__name__ = p2p_route_pull_update_one_func.__name__ + db_collection
            self.route(f"/pull_update_one/{db_name}/{db_collection}", methods=['POST'])(
                p2p_route_pull_update_one_func)

            execute_function_partial = wraps(route_execute_function)(
                partial(self.pass_req_dec(route_execute_function),
                        p2pbrokerfunction=p2pbrokerfunction))
            execute_function_partial.__name__ = execute_function_partial.__name__ + db_collection
            self.route(f'/execute_function/{db_name}/{db_collection}/{function_name}',
                       methods=['POST'])(execute_function_partial)

            terminate_function_partial = wraps(route_terminate_function)(
                partial(self.pass_req_dec(route_terminate_function),
                        p2pbrokerfunction=p2pbrokerfunction))
            terminate_function_partial.__name__ = terminate_function_partial.__name__ + db_collection
            self.route(f'/terminate_function/{db_name}/{db_collection}/{function_name}',
                       methods=['POST'])(terminate_function_partial)

            delete_function_partial = wraps(route_delete_function)(
                partial(self.pass_req_dec(route_delete_function),
                        p2pbrokerfunction=p2pbrokerfunction))
            delete_function_partial.__name__ = delete_function_partial.__name__ + db_collection
            self.route(f'/delete_function/{db_name}/{db_collection}/{function_name}',
                       methods=['POST'])(delete_function_partial)

            check_function_termination_partial = wraps(route_check_function_termination)(
                partial(self.pass_req_dec(route_check_function_termination),
                        p2pbrokerfunction=p2pbrokerfunction))
            check_function_termination_partial.__name__ = check_function_termination_partial.__name__ + db_collection
            self.route(f'/check_function_termination/{db_name}/{db_collection}/{function_name}',
                       methods=['POST'])(check_function_termination_partial)

            check_function_deletion_partial = wraps(route_check_function_deletion)(
                partial(self.pass_req_dec(route_check_function_deletion),
                        p2pbrokerfunction=p2pbrokerfunction))
            check_function_deletion_partial.__name__ = check_function_deletion_partial.__name__ + db_collection
            self.route(f'/check_function_deletion/{db_name}/{db_collection}/{function_name}',
                       methods=['POST'])(check_function_deletion_partial)

            search_work_partial = wraps(route_search_work)(
                partial(self.pass_req_dec(route_search_work),
                        p2pbrokerfunction=p2pbrokerfunction))
            search_work_partial.__name__ = search_work_partial.__name__ + db_collection
            self.route(f"/search_work/{db_name}/{db_collection}/{function_name}", methods=['POST'])(
                search_work_partial)

            identifier_available_partial = wraps(route_identifier_available)(
                partial(self.pass_req_dec(route_identifier_available),
                        p2pbrokerfunction=p2pbrokerfunction))
            identifier_available_partial.__name__ = identifier_available_partial.__name__ + db_collection
            self.route(f"/identifier_available/{db_name}/{db_collection}/{function_name}/<identifier>",
                       methods=['GET'])(identifier_available_partial)

        return inner_decorator
