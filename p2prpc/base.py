from logging.config import dictConfig
from typing import List
from flask import Flask, Blueprint, make_response
import time
import threading
import logging
import requests
from .bookkeeper import node_states, update_function
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


def wait_until_online(local_port):
    # while the app is not alive yet
    while True:
        try:
            response = requests.get('http://{}:{}/echo'.format('localhost', local_port))
            if response.status_code == 200:
                break
        except:
            logger.info("App not ready")


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


def create_bookkeeper_p2pblueprint(local_port: int, app_roles: List[str], discovery_ips_file: str, mongod_port) -> P2PBlueprint:
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
    func = (wraps(node_states)(partial(node_states, mongod_port)))
    bookkeeper_bp.route("/node_states", methods=['POST', 'GET'])(func)

    time_regular_func = partial(update_function, local_port, app_roles, discovery_ips_file)
    bookkeeper_bp.register_time_regular_func(time_regular_func)

    return bookkeeper_bp


def wait_for_mongo_online(mongod_port):
    while True:
        try:
            client = pymongo.MongoClient(port=mongod_port)
            client.server_info()
            break
        except:
            logger.info("Mongod not online yet")
            continue


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
        self._time_regular_thread = None
        self._logging_queue = multiprocessing.Manager().Queue()
        self._logger_thread = None
        self._stop_thread = False
        self._time_interval = 10
        if local_port is None:
            local_port = find_free_port()
        self.local_port = local_port
        self.mongod_port = mongod_port
        self.cache_path = cache_path
        self.mongod_process = None
        self.password = password
        self.discovery_ips_file = discovery_ips_file
        self.crypt_pass = sha256_crypt.encrypt(password)

        if not os.path.exists(cache_path):
            os.mkdir(cache_path)

        self.route("/echo", methods=['GET'])(echo)

        bookkeeper_bp = create_bookkeeper_p2pblueprint(local_port=self.local_port,
                                                       app_roles=self.roles,
                                                       discovery_ips_file=self.discovery_ips_file, mongod_port=self.mongod_port)
        self.register_blueprint(bookkeeper_bp)

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


    def _time_regular(self, list_funcs, time_interval, local_port, stop_thread):
        # while the app is not alive it
        count = 0
        max_trials = 10
        while count < max_trials:
            try:
                response = requests.get('http://{}:{}/echo'.format('localhost', local_port))
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
                    logger.error("Eroor in node of type: {}".format(self.roles))
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
        # TODO also kill this process
        self._logger_thread = threading.Thread(target=P2PFlaskApp._dispatch_log_records,
                                               args=(
                                                   self._logging_queue,
                                               ))
        self._logger_thread.start()
        self._time_regular_thread = threading.Thread(target=self._time_regular,
                                                     args=(
                                                         self._time_regular_funcs, self._time_interval, self.local_port,
                                                         lambda: self._stop_thread))
        self._time_regular_thread.start()

    def stop_background_threads(self):
        self._stop_thread = True
        logger.info("Joining time regular thread")
        self._time_regular_thread.join()
        self._logging_queue.put_nowait('STOP _dispatch_log_records')
        logger.info("Joining logger thread")
        self._logger_thread.join()
        logger.info("Killing mongod")
        self.mongod_process.kill()
        # or use the command: mongod --dbpath /path/to/your/db --shutdown
        logger.info("Finished all background processes")

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


def validate_arguments(f, args, kwargs):
    """
    After the function has been called, it's actual arguments are checked for some other constraints.
    For example all arguments must be specified as keyword arguments,
    All arguments must be instances of the declared type.
    # TODO to prevent security issues maybe the type(arg) == declared type, not isinstance(arg, declared_type)
    """
    if len(args) != 0:
        raise ValueError("All arguments to a function in this p2p framework need to be specified by keyword arguments")

    # check that every value passed in this function has the same type as the one declared in function annotation
    f_param_sign = inspect.signature(f).parameters
    assert set(f_param_sign.keys()) == set(kwargs.keys())
    for k, v in kwargs.items():
        f_param_k_annotation = f_param_sign[k].annotation
        if not isinstance(v, f_param_k_annotation):
            raise ValueError(
                f"class of value {v} for argument {k} is not the same as the annotation {f_param_k_annotation}")
    if "value_for_key_is_file" in kwargs.values():
        raise ValueError("'value_for_key_is_file' string is a reserved value in this p2p framework. It helps "
                         "identifying a file when serializing together with other arguments")
    files = [v for v in kwargs.values() if isinstance(v, io.IOBase)]
    if len(files) > 1:
        raise ValueError("p2p framework does not currently support sending more files")
    if files:
        if any(file.closed or file.mode != 'rb' or file.tell() != 0 for file in files):
            raise ValueError("all files should be opened in read binary mode and pointer must be at start")


def validate_function_signature(func):
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
    formal_args = list(inspect.signature(func).parameters.keys())
    if any(key in formal_args for key in ["identifier", "nodes", "timestamp"]):
        raise ValueError("identifier, nodes, timestamp are restricted keywords in this p2p framework")

    return_anno = inspect.signature(func).return_annotation
    if not "return" in inspect.getsource(func):
        raise ValueError("Function must return something.")
    if not isinstance(return_anno, dict):
        raise ValueError("Function must be return annotated with dict")
    if not all(isinstance(k, str) for k in return_anno.keys()):
        raise ValueError("Return dictionary must have as keys only strings")
    if not all(v in allowed_func_datatypes for v in return_anno.values()):
        raise ValueError("Return values must be one of the following types {}".format(allowed_func_datatypes))

    params = [v[1] for v in list(inspect.signature(func).parameters.items())]
    if any(p.annotation == inspect._empty for p in params):
        raise ValueError("Function must have all arguments type annotated")
    if any(p.annotation not in allowed_func_datatypes for p in params):
        raise ValueError("Function arguments must be one of the following types {}".format(allowed_func_datatypes))

    if len(set(formal_args) & set(return_anno.keys())) != 0:
        raise ValueError("The keys specified in return annotation must not be found in arguments names")
    # TODO in the future all formal args should have type annotations
    #  and provide serialization and deserialization methods for each


def derive_vars_from_function(f):
    """
    Inspects function signature and creates some necessary variables for p2p framework

    Args:
        f: function to be decorated
        cache_path: path to directory to store function arguments and results

    Return:
        key_interpreter: Function is annotated as def f(arg1: io.IOBase, arg2: str)
            the resulting dictionary will be {"arg1" io.IOBase, "arg2": str}
        The following refer to tinymongo db
            db_url: cache_path
            db: hardcoded "p2p" key
            col: function name
    """
    db = "p2p"
    validate_function_signature(f)
    key_interpreter = get_class_dictionary_from_func(f)
    col = f.__name__
    return key_interpreter, db, col


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
