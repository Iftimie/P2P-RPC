from .base import P2PFlaskApp, create_bookkeeper_p2pblueprint
from functools import wraps
from functools import partial
from .p2pdata import p2p_insert_one
from .p2pdata import p2p_pull_update_one, deserialize_doc_from_net
import inspect
import io
import os
from .p2p_brokerworker import function_executor, delete_old_requests
from . import p2p_brokerworker
from .base import derive_vars_from_function
from .base import configure_logger
import logging
from .p2p_client import get_available_brokers
import requests
import collections
import multiprocessing
from collections import defaultdict


def find_response_with_work(local_port, db, collection, func_name, password):
    logger = logging.getLogger(__name__)

    res_broker_ip = None
    res_broker_port = None
    res_json = dict()

    brokers = get_available_brokers(local_port=local_port)

    if not brokers:
        logger.info("No broker found")

    for broker in brokers:
        broker_address = broker['address']
        try:
            url = 'http://{}/search_work/{}/{}/{}'.format(broker_address, db, collection, func_name)
            res = requests.post(url, timeout=5, headers={"Authorization": password})
            if isinstance(res.json, collections.Callable):
                returned_json = res.json()  # from requests lib
            else: # is property
                returned_json = res.json  # from Flask test client
            if returned_json and 'filter' in returned_json:
                logger.info("Found work from {}".format(broker_address))
                res_broker_ip,res_broker_port = broker_address.split(":")
                res_json = returned_json
                break
        except:  # except connection timeout or something like that
            logger.info("broker unavailable {}".format(broker_address))
            pass

    if res_broker_ip is None:
        logger.info("No work found")

    # TODO it may be possible that res allready contains broker ip and port?
    return res_json, res_broker_ip, res_broker_port


class P2PClientworkerApp(P2PFlaskApp):

    def __init__(self, discovery_ips_file, cache_path, local_port=5002, mongod_port=5102, password=""):
        configure_logger("clientworker", module_level_list=[(__name__, 'DEBUG'),
                                                            (p2p_brokerworker.__name__, 'INFO')])
        super(P2PClientworkerApp, self).__init__(__name__, local_port=local_port, discovery_ips_file=discovery_ips_file, mongod_port=mongod_port,
                                                 cache_path=cache_path, password=password)
        self.registry_functions = defaultdict(dict)
        self.worker_pool = multiprocessing.Pool(2)
        self.list_futures = []
        self.register_time_regular_func(partial(delete_old_requests,
                                                         mongod_port=mongod_port,
                                                         registry_functions=self.registry_functions))

    def register_p2p_func(self, can_do_work_func):
        """
        In p2p clientworker, this decorator will have the role of deciding making a node behind a firewall or NAT capable of
        executing a function that receives input arguments from over the network.

        Args:
            can_do_work_func: checks if the clientworkerapp can do more work
        """

        def inner_decorator(f):
            if f.__name__ in self.registry_functions:
                raise ValueError("Function name already registered")
            key_interpreter, db, col = derive_vars_from_function(f)

            self.registry_functions[f.__name__]['key_interpreter'] = key_interpreter

            updir = os.path.join(self.cache_path, db, col)  # upload directory
            os.makedirs(updir, exist_ok=True)

            param_keys = list(inspect.signature(f).parameters.keys())
            key_return = list(inspect.signature(f).return_annotation.keys())
            hint_args_file_keys = [k for k, v in inspect.signature(f).parameters.items() if v.annotation == io.IOBase]

            @wraps(f)
            def wrap():
                if not can_do_work_func():
                    return
                logger = logging.getLogger(__name__)
                logger.info("Searching for work")
                res, broker_ip, broker_port = find_response_with_work(self.local_port, db, col, f.__name__,
                                                                      self.crypt_pass)
                if broker_ip is None:
                    return

                filter_ = res['filter']

                local_data = {k: v for k, v in filter_.items()}
                local_data.update({k: None for k in param_keys})
                local_data.update({k: None for k in key_return})

                deserializer = partial(deserialize_doc_from_net, up_dir=updir, key_interpreter=key_interpreter)
                p2p_insert_one(self.mongod_port, db, col, local_data, [broker_ip + ":" + str(broker_port)],
                               do_upload=False, password=self.crypt_pass)
                p2p_pull_update_one(self.mongod_port, db, col, filter_, param_keys, deserializer,
                                    hint_file_keys=hint_args_file_keys, password=self.crypt_pass)

                new_f = wraps(f)(
                    partial(function_executor,
                            f=f, filter=filter_,
                            mongod_port=self.mongod_port, db=db, col=col,
                            key_interpreter=key_interpreter,
                            logging_queue=self._logging_queue,
                            password=self.crypt_pass))
                res = self.worker_pool.apply_async(func=new_f)
                self.list_futures.append(res)
                # _ = function_executor(f, filter_, db, col, db_url, key_interpreter, self._logging_queue)
                # something weird was happening with logging when the function was executed in the same thread

            self.register_time_regular_func(wrap)
            return None

        return inner_decorator
