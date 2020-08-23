from logging.config import dictConfig
from flask import Flask, Blueprint, make_response, request
import time
import threading
import logging
import requests
from .bookkeeper import route_node_states, update_function
import io
from warnings import warn
import collections
import inspect
import socket
from contextlib import closing
from deprecated import deprecated
from .registry_args import allowed_func_datatypes
from .registry_args import get_class_dictionary_from_func
import multiprocessing
import traceback
import os
import subprocess
from passlib.hash import sha256_crypt
import pymongo
import sys
from collections import Callable, defaultdict
logger = logging.getLogger(__name__)


"""
https://kite.com/blog/python/functional-programming/
Using higher-order function with type comments is an advanced skill. Type signatures often become long and unwieldy 
nests of Callable. For example, the correct way to type a simple higher order decorator that returns the input function 
is by declaring F = TypeVar[‘F’, bound=Callable[..., Any]] then annotating as def transparent(func: F) -> F: return func
. Or, you may be tempted to bail and use Any instead of trying to figure out the correct signature.
"""


def echo():
    """
    Implicit function binded(routed) to /echo path. This helps a client determine if a broker/worker/P2Papp if it still exists.
    """
    return make_response("I exist", 200)


def wait_until_online(local_port, password):
    # while the app is not alive yet
    while True:
        try:
            response = requests.get('http://{}:{}/echo'.format('localhost', local_port), headers={'Authorization': password})
            if response.status_code == 200:
                break
        except:
            logger.info("App not ready")

def validate_function_signature(original_function):
    """
    Verifies that the function signature respects the required information about. The function format should be the following:
    Each argument must be type annotated. In signature the return type must also appear as a dictionary with keys and their data types.
    The default accepted data types are file, float, int, string.

    In the future more data types could be allowed, but for each new datatype a few functions for serialization, deserialization,
    hashing must also be supplied.

    For example:
    def f(arg1: io.IOBase, arg2: str, arg3: float, arg4: int) -> {"key1": io.IOBase, "key2": str}:
        # do something
        return {"key1":open(filepath, "rb"), "key2": "value2"}

    There are some reserved keywords:
    "identifier"
    """
    formal_args = list(inspect.signature(original_function).parameters.keys())
    if any(key in formal_args for key in P2PFunction.restricted_keywords):
        raise ValueError("identifier, nodes, timestamp are restricted keywords in this p2p framework")
    if any("tmpfile" in k for k in formal_args):
        raise ValueError("tmpfile is a restricted substring in argument names in this p2p framework")

    return_anno = inspect.signature(original_function).return_annotation
    if not "return" in inspect.getsource(original_function):
        raise ValueError("Function must return something.")
    if not isinstance(return_anno, dict):
        raise ValueError("Function must be return annotated with dict")
    if not all(isinstance(k, str) for k in return_anno.keys()):
        raise ValueError("Return dictionary must have as keys only strings")
    if not all(v in allowed_func_datatypes for v in return_anno.values()):
        raise ValueError("Return values must be one of the following types {}".format(allowed_func_datatypes))

    params = [v[1] for v in list(inspect.signature(original_function).parameters.items())]
    if any(p.annotation == inspect._empty for p in params):
        raise ValueError("Function must have all arguments type annotated")
    if any(p.annotation not in allowed_func_datatypes for p in params):
        raise ValueError("Function arguments must be one of the following types {}".format(allowed_func_datatypes))

    if len(set(formal_args) & set(return_anno.keys())) != 0:
        raise ValueError("The keys specified in return annotation must not be found in arguments names")
    # TODO in the future all formal args should have type annotations
    #  and provide serialization and deserialization methods for each
def derive_vars_from_function( original_function):
    """
    Inspects function signature and creates some necessary variables for p2p framework

    Args:
        original_function: function to be decorated

    Return:
        key_interpreter: Function is annotated as def f(arg1: io.IOBase, arg2: str)
            the resulting dictionary will be {"arg1" io.IOBase, "arg2": str}
        The following refer to tinymongo db
            db_url: cache_path
            db: hardcoded "p2p" key
            col: function name
    """
    db = "p2p"
    validate_function_signature(original_function)
    key_interpreter = get_class_dictionary_from_func(original_function)
    col = original_function.__name__
    return key_interpreter, db, col


class P2PArguments:
    def __init__(self, expected_keys, expected_return_keys):
        """
        Args:
            expected_keys: list of keys that represent input arguments
            expected_return_keys: list of keys that represent outputs
        """
        self.args_identifier = None
        self.expected_keys = expected_keys[:]
        self.expected_return_keys = expected_return_keys[:]
        self.kwargs = {k:None for k in expected_keys}
        self.outputs = {k:None for k in expected_return_keys}

    def object2doc(self):
        """
        Transform the current object into a mongodb serializable dictionary (document)
        """
        function_call_properties = {}
        function_call_properties.update(self.kwargs)
        function_call_properties.update(self.outputs)
        function_call_properties['identifier'] = self.args_identifier
        return function_call_properties

    def doc2object(self, document):
        self.args_identifier = document['identifier']
        for k in self.expected_keys:
            self.kwargs[k] = document[k]
        for k in self.expected_return_keys:
            self.outputs[k] = document[k]

class P2PFunction:

    restricted_keywords = ["identifier", "remote_identifier", "nodes", "timestamp", "started", "kill_clientworker", "delete_clientworker",
                           'progress', 'error', 'current_address']
    restricted_values = ["value_for_key_is_file"]

    def __init__(self, original_function: Callable, mongod_port, crypt_pass):
        self.original_function = original_function
        self.function_name = original_function.__name__
        self.args_interpreter, self.db_name, self.db_collection = self.derive_vars_from_function(original_function)
        self.mongod_port = mongod_port
        self.expected_keys, self.expected_return_keys = self.get_expected_keys(original_function)
        self.crypt_pass = crypt_pass

    def validate_function_signature(self, original_function):
        """
        Verifies that the function signature respects the required information about. The function format should be the following:
        Each argument must be type annotated. In signature the return type must also appear as a dictionary with keys and their data types.
        The default accepted data types are file, float, int, string.

        In the future more data types could be allowed, but for each new datatype a few functions for serialization, deserialization,
        hashing must also be supplied.

        For example:
        def f(arg1: io.IOBase, arg2: str, arg3: float, arg4: int) -> {"key1": io.IOBase, "key2": str}:
            # do something
            return {"key1":open(filepath, "rb"), "key2": "value2"}

        There are some reserved keywords:
        "identifier"
        """
        formal_args = list(inspect.signature(original_function).parameters.keys())
        if any(key in formal_args for key in P2PFunction.restricted_keywords):
            raise ValueError("identifier, nodes, timestamp are restricted keywords in this p2p framework")
        if any("tmpfile" in k for k in formal_args):
            raise ValueError("tmpfile is a restricted substring in argument names in this p2p framework")

        return_anno = inspect.signature(original_function).return_annotation
        if not "return" in inspect.getsource(original_function):
            raise ValueError("Function must return something.")
        if not isinstance(return_anno, dict):
            raise ValueError("Function must be return annotated with dict")
        if not all(isinstance(k, str) for k in return_anno.keys()):
            raise ValueError("Return dictionary must have as keys only strings")
        if not all(v in allowed_func_datatypes for v in return_anno.values()):
            raise ValueError("Return values must be one of the following types {}".format(allowed_func_datatypes))

        params = [v[1] for v in list(inspect.signature(original_function).parameters.items())]
        if any(p.annotation == inspect._empty for p in params):
            raise ValueError("Function must have all arguments type annotated")
        if any(p.annotation not in allowed_func_datatypes for p in params):
            raise ValueError("Function arguments must be one of the following types {}".format(allowed_func_datatypes))

        if len(set(formal_args) & set(return_anno.keys())) != 0:
            raise ValueError("The keys specified in return annotation must not be found in arguments names")
        # TODO in the future all formal args should have type annotations
        #  and provide serialization and deserialization methods for each

    def derive_vars_from_function(self, original_function):
        """
        Inspects function signature and creates some necessary variables for p2p framework

        Args:
            original_function: function to be decorated

        Return:
            key_interpreter: Function is annotated as def f(arg1: io.IOBase, arg2: str)
                the resulting dictionary will be {"arg1" io.IOBase, "arg2": str}
            The following refer to tinymongo db
                db_url: cache_path
                db: hardcoded "p2p" key
                col: function name
        """
        db = "p2p"
        self.validate_function_signature(original_function)
        key_interpreter = get_class_dictionary_from_func(original_function)
        col = original_function.__name__
        return key_interpreter, db, col

    def get_expected_keys(self, original_function):
        """
        By inspecting the the function signature we can get a dictionary with the arguments (because all are annotated)
        and a dictionary about the return (because this is required by the p2p framework)
        The combined dictionary will be stored in the database
        """
        expected_keys = list(inspect.signature(original_function).parameters.keys())

        expected_return_keys = list(inspect.signature(original_function).return_annotation.keys())
        expected_return_keys.extend(['progress', 'error'])
        return expected_keys, expected_return_keys



class P2PBlueprint(Blueprint):
    """
    The P2PBlueprint also has a background task, a function that is designed to be called every N seconds.

    Or should it be named something like ActiveBlueprint? And the Blueprint should have an alias such as PassiveBlueprint?
    """

    def __init__(self, *args, role, **kwargs):
        super(P2PBlueprint, self).__init__(*args, **kwargs)
        self.time_regular_funcs = []
        self.role = role
        self.rule_mappings = {}
        self.overwritten_rules = [] # List[Tuple[str, callable]]

    def register_time_regular_func(self, f):
        """
        Registers a callable that is called every self.time_interval seconds.
        The callable will not receive any arguments. If arguments are needed, make it a partial function or a class with
        __call__ implemented.
        """
        self.time_regular_funcs.append(f)

    def route(self, *args, **kwargs):
        """
        Overwritten method for catching the rules and their functions in a map. In case the function is a locally declared function such as a partial,
        and we may want to overwrite that method, we need to store somewhere that locally declared function, otherwise we cannot access it.

        Example of route override:
        https://github.com/Iftimie/TruckMonitoringSystem/blob/6405f0341ad41c32fae7e4bab2d264b65a1d8ee9/truckms/service/worker/broker.py#L163
        """
        if args:
            rule = args[0]
        else:
            rule = kwargs['rule']

        decorator_function = super(P2PBlueprint, self).route(*args, **kwargs)
        def decorated_function_catcher(f):
            if rule in self.rule_mappings:
                self.overwritten_rules.append((rule, self.rule_mappings[rule]))
            self.rule_mappings[rule] = f
            return decorator_function(f)

        return decorated_function_catcher


def wait_for_mongo_online(mongod_port):
    while True:
        try:
            client = pymongo.MongoClient(port=mongod_port)
            client.server_info()
            break
        except:
            logger.info("Mongod not online yet")
            continue


def is_debug_mode():
    gettrace = getattr(sys, 'gettrace', None)
    if gettrace is None:
        return False
    elif gettrace():
        return True


def password_required(password):
    def internal_decorator(f):
        @wraps(f)
        def wrap(*args, **kwargs):
            if not sha256_crypt.verify(password, request.headers.get('Authorization')):
                return make_response("Unauthorized", 401)
            return f(*args, **kwargs)
        return wrap
    return internal_decorator


class P2PFlaskApp(Flask):
    """
    Flask derived class for P2P applications. In this framework, the P2P app can have different roles. Not all nodes in
    the network are equal. Some act only as clients, some are reachable and act as workers or brokers, some have GPUs
     but are not reachable and thus act as workers. Given this possible set of configurations, the P2PFlaskApp has a
     specific role (names such as "streamer", "bookkeeper", "worker", "broker", "clientworker" etc).
     Also given the specific role, the app may have or not a background task (for example a task that makes network
     discovery)
    """

    def __init__(self, *args, local_port=None, mongod_port=None, cache_path=None, password="", discovery_ips_file, **kwargs):
        """
        Args:
            args: positional arguments
            overwritten_routes: list of tuples (route, function_pointer). In case of overwriting a blueprint route,
                we need to address this situation manually.
            kwargs: keyword arguments
        """
        self.overwritten_routes = []  # List[Tuple[str, callable]]
        super(P2PFlaskApp, self).__init__(*args, **kwargs)
        self.roles = []
        self._blueprints = {}
        self._time_regular_funcs = []
        self._initial_funcs = []
        # FIXME this if else statement in case of debug mode was introduced just for an unfortunated combination of OS
        #  and PyCharm version when variables in watch were hanging with no timeout just because of multiprocessing manaegr
        if is_debug_mode():
            self._logging_queue = multiprocessing.Queue()
        else:
            self.manager = multiprocessing.Manager()
            self._logging_queue = self.manager.Queue()
        self._time_regular_thread = None
        self._logger_thread = None
        self._stop_thread = False
        self._time_interval = 5
        if local_port is None:
            local_port = find_free_port()
        self.local_port = local_port
        self.mongod_port = mongod_port
        self.cache_path = cache_path
        self.mongod_process = None
        self.password = password
        self.discovery_ips_file = discovery_ips_file
        self.crypt_pass = sha256_crypt.encrypt(password)
        self.pass_req_dec = password_required(password)
        self.registry_functions = defaultdict(dict)

        if not os.path.isabs(cache_path):
            raise ValueError("cache_path must be absolute {}".format(cache_path))
        if not os.path.exists(cache_path):
            os.mkdir(cache_path)

        self.route("/echo", methods=['GET'])(self.pass_req_dec(echo))

    def add_to_super_register(self, p2pfunction: P2PFunction):
        if p2pfunction.function_name in self.registry_functions:
            raise ValueError(f"Function {p2pfunction.__name__} already registered")
        self.registry_functions[p2pfunction.function_name] = p2pfunction

    def add_url_rule(self, rule, endpoint=None, view_func=None, **options):
        # Flask registers views when an application starts
        # do not add view from self.overwritten_routes
        for rule_, view_func_ in self.overwritten_routes:
            if rule_ == rule and view_func == view_func_:
                warn("Overwritten rule: {}".format(rule))
                return
        return super(P2PFlaskApp, self).add_url_rule(rule, endpoint, view_func, **options)

    def register_blueprint(self, blueprint, **options):
        """
        Overwritten method. It helps identifying overwritten routes
        """
        if isinstance(blueprint, P2PBlueprint):
            for f in blueprint.time_regular_funcs:
                self.register_time_regular_func(f)
            self.roles.append(blueprint.role)
            self.overwritten_routes += blueprint.overwritten_rules

        self._blueprints[blueprint.name] = blueprint
        super(P2PFlaskApp, self).register_blueprint(blueprint)

    # TODO I should also implement the shutdown method that will close the time_regular_thread

    def _run_init_funcs(self):
        for f in self._initial_funcs:
            f()

    def _time_regular(self, list_funcs, time_interval, local_port, stop_thread, password):
        # while the app is not alive it
        count = 0
        max_trials = 10
        while count < max_trials:
            try:
                response = requests.get('http://{}:{}/echo'.format('localhost', local_port), headers={"Authorization": password})
                if response.status_code == 200:
                    break
            except:
                logger.info("App not ready")
            time.sleep(1)
            count += 1

        # infinite loop will not start until the app is online
        while not stop_thread():
            for f in list_funcs:
                try:
                    f()
                except Exception as e:
                    logger.error("Error in node of type: {}".format(self.roles))
                    raise e
            time.sleep(time_interval)

    @staticmethod
    def _dispatch_log_records(queue):
        while True:
            try:
                record = queue.get()
                queue.task_done()
                if isinstance(record, str) and record == 'STOP _dispatch_log_records':
                    break
                logger = logging.getLogger(record.name)
                logger.handle(record)
            except BrokenPipeError as e:
                break
            except Exception as e:
                traceback.print_exc()
                pass

    def register_time_regular_func(self, f):
        """
        Registers a callable that is called every self.time_interval seconds.
        The callable will not receive any arguments. If arguments are needed, make it a partial function or a class with
        __call__ implemented.
        The functions should not have infinite loops, or if there are blocking functions a timeout should be used
        """
        self._time_regular_funcs.append(f)

    def start_background_threads(self):
        self.mongod_process = subprocess.Popen(["mongod", "--dbpath", self.cache_path, "--port", str(self.mongod_port),
                                                 "--logpath", os.path.join(self.cache_path, "mongodlog.log")])
        wait_for_mongo_online(self.mongod_port)
        self._run_init_funcs()
        # TODO also kill this process
        self._logger_thread = threading.Thread(target=P2PFlaskApp._dispatch_log_records,
                                               args=(
                                                   self._logging_queue,
                                               ))
        self._logger_thread.start()
        self._time_regular_thread = threading.Thread(target=self._time_regular,
                                                     args=(
                                                         self._time_regular_funcs, self._time_interval, self.local_port,
                                                         lambda: self._stop_thread, self.crypt_pass))
        self._time_regular_thread.start()

    def stop_background_threads(self):
        self._stop_thread = True
        self._time_regular_thread.join(timeout=120)
        if self._time_regular_thread.isAlive():
            logger.info("time regular thread still Alive")
        self._logging_queue.put_nowait('STOP _dispatch_log_records')
        self._logger_thread.join()
        self.mongod_process.kill()
        # or use the command: mongod --dbpath /path/to/your/db --shutdown

    def run(self, *args, **kwargs):
        if len(args) > 0:
            raise ValueError("Specify arguments by keyword arguments")
        if 'port' in kwargs:
            raise ValueError("port argument does not need to be specified as it either specified in constructor or generated randomly")

        kwargs['port'] = self.local_port
        self.start_background_threads()
        super(P2PFlaskApp, self).run(*args, **kwargs)


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def self_is_reachable(local_port):
    """
    return the public address: ip:port if it is reachable
    It makes a call to a public server such as 'http://checkip.dyndns.org/'. Inspired from the bitcoin protocol
    else it returns None
    """
    externalipres = requests.get('http://checkip.dyndns.org/')
    part = externalipres.content.decode('utf-8').split(": ")[1]
    ip_ = part.split("<")[0]

    # LAN ip state
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    LAN_ip_ = s.getsockname()[0]
    s.close()

    try:
        echo_response = requests.get('http://{}:{}/echo'.format(ip_, local_port), timeout=2)
        if echo_response.status_code == 200:
            return ["{}:{}".format(ip_, local_port), "{}:{}".format(LAN_ip_, local_port)]
        else:
            return ["{}:{}".format(LAN_ip_, local_port)]
    except:
        return ["{}:{}".format(LAN_ip_, local_port)]


from functools import wraps, partial


def ignore_decorator(f):
    @wraps(f)
    def wrap(*args, **kwargs):
        warn("Function call to " + f.__name__ + " is ignored")
        return
    return wrap


@deprecated(reason="P2P RPC will not implement progress update in this way")
def find_update_callables(kwargs):
    callables = [(k, v) for k, v in kwargs.items() if isinstance(v, collections.Callable)]
    update_callables = list(filter(
        lambda kc: ("return" in inspect.getsource(kc[1]) or 'lambda' in inspect.getsource(kc[1])) and inspect.signature(
            kc[1]).return_annotation == dict, callables))
    update_callables = {k: v for k, v in update_callables}
    return update_callables


@ignore_decorator
@deprecated(reason="P2P RPC will not implement progress update in this way")
def validate_update_callables(kwargs):
    update_callables = find_update_callables(kwargs)
    if len(update_callables) > 1:
        raise ValueError("Only one function that has return_annotation with dict and has return keyword is accepted")
    warn(
        "If function returns a dictionary in order to update a document in collection, then annotate it with '-> dict'"
        "If found return value and dict return annotation. The returned value will be used to update the document in collection")










def configure_logger(name, module_level_list=None, default_level='WARNING'):
    # https://fangpenlin.com/posts/2012/08/26/good-logging-practice-in-python/

    if module_level_list is None:
        module_level_list = []

    format_string = "[%(processName)s %(threadName)s %(asctime)20s - %(name)s.%(funcName)s:%(lineno)s %(levelname)s] %(message)s"

    handlers = {
        "info_file_handler": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "MYFORMATTER",
            "filename": "{}_info.log".format(name),
            "maxBytes": 10485760,
            "backupCount": 20,
            "encoding": "utf8"
        },
        "error_file_handler": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "ERROR",
            "formatter": "MYFORMATTER",
            "filename": "{}_errors.log".format(name),
            "maxBytes": 10485760,
            "backupCount": 20,
            "encoding": "utf8"
        },
        'wsgi': {
            "level": "ERROR",
            'class': 'logging.StreamHandler',
            'stream': 'ext://flask.logging.wsgi_errors_stream',
            'formatter': 'MYFORMATTER'
        },
        'werkzeug': {
            'level': "ERROR",
            'class': 'logging.StreamHandler',
            'stream': "ext://sys.stdout",
            'formatter': 'MYFORMATTER'
        }
    }  # TODO maybe add an email handler
    for level in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
        handlers['console_'+level] = {'level': level,
                                      'class': 'logging.StreamHandler',
                                      'stream': "ext://sys.stdout",
                                      'formatter': 'MYFORMATTER'}
    dconf = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {'MYFORMATTER': {
            'format': format_string,
        }},
        'handlers': handlers,

        'loggers': {
            '': {
                'level': default_level,
                'handlers': ['console_'+default_level, 'info_file_handler', 'error_file_handler', 'wsgi', 'werkzeug']
            },
            'root': {
                'level': default_level,
                'handlers': ['console_'+default_level, 'info_file_handler', 'error_file_handler', 'wsgi', 'werkzeug']
            },
        },
    }
    # I do this like that because I want in console to ignore INFO messages from flask (root is WARNING)
    # but I do not want to ignore from other modules
    for m_l in module_level_list:
        dconf['loggers'][m_l[0]] = {'level': m_l[1], 'handlers': ['console_'+m_l[1], 'info_file_handler', 'error_file_handler']}

    dictConfig(dconf)
