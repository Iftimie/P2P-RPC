from .base import P2PFlaskApp, create_bookkeeper_p2pblueprint, is_debug_mode
from functools import wraps
from functools import partial
from .p2pdata import p2p_insert_one
from .p2pdata import p2p_pull_update_one, deserialize_doc_from_net, find
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
from json import dumps


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


from p2prpc.p2pdata import p2p_push_update_one
from json import dumps, loads
def check_brokerworker_termination(jobs, mongod_port, password):
    logger = logging.getLogger(__name__)

    # TODO instead of keeping this stupid jobs dictionary, I could just store the PID in mongodb

    for filteritems in list(jobs.keys()):
        process, db, col, func_name = jobs[filteritems]
        if not process.is_alive():
            del jobs[filteritems]
            continue
        filter = {k: v for k, v in filteritems}
        p2p_pull_update_one(mongod_port, db, col, filter, ['kill_clientworker'], deserializer=partial(deserialize_doc_from_net, up_dir=None), password=password)
        item = find(mongod_port, db, col, filter)[0]
        if item['kill_clientworker'] is True:
            logger.info("Terminated function {} {}".format(func_name, filter))
            process.terminate()
            del jobs[filteritems]
            search_filter = {"$or": [{"identifier": item['identifier']}, {"identifier": item['remote_identifier']}]}
            p2p_push_update_one(mongod_port, db, col, search_filter, {"started": 'terminated', 'kill_clientworker': False}, password=password)
            print("pushed termination")

from pymongo import MongoClient
from .p2pdata import deserialize_doc_from_db
from .registry_args import remove_values_from_doc, db_encoder
# aparently this function is not used
def check_brokerworker_deletion(self):
    logger = logging.getLogger(__name__)

    for funcname in self.registry_functions:
        f = self.registry_functions[funcname]['original_func']
        key_interpreter, db, col = derive_vars_from_function(f)
        items = find(self.mongod_port, db, col, {})
        for item in items:
            print("asdasdasdasd")
            search_filter = {"$or": [{"identifier": item['identifier']}, {"identifier": item['remote_identifier']}]}
            p2p_pull_update_one(self.mongod_port, db, col, search_filter, ['delete_clientworker'],
                                deserializer=partial(deserialize_doc_from_net, up_dir=None), password=self.crypt_pass)
            itemtodel = find(self.mongod_port, db, col, search_filter)[0]
            print("itemtodel", itemtodel)
            if itemtodel['delete_clientworker'] is True:
                mongodb = MongoClient(port=self.mongod_port)[db][col]
                p2p_push_update_one(self.mongod_port, db, col, search_filter, {'delete_clientworker': False}, password=self.crypt_pass)
                document = deserialize_doc_from_db(itemtodel, key_interpreter)
                remove_values_from_doc(document)
                # mongodb.delete_one(search_filter) # IF I PUT ITEMTODEL IT WON'T WORK. WTFF??
                itemtodel = find(self.mongod_port, db, col, search_filter)[0]
                mongodb.delete_one(itemtodel)
                ### OOOH PROBABLY BECAUSE IT IS AN OLD OBJECT, OUTDATED. BECASE i DID P2P PUSH UPDATE
                # yep. DONT INVALIDATE OBJECTS BY CALLING FUNCTIONS THAT UPDATE THE DB AND THE OBJECT REMAINS OUTDATED
                print("itemul vietii", itemtodel)
                print("dupa stergere", find(self.mongod_port, db, col, search_filter))
    return


class P2PClientworkerApp(P2PFlaskApp):

    def __init__(self, discovery_ips_file, cache_path, local_port=5002, mongod_port=5102, password=""):
        configure_logger("clientworker", module_level_list=[(__name__, 'DEBUG'),
                                                            (p2p_brokerworker.__name__, 'INFO')])
        super(P2PClientworkerApp, self).__init__(__name__, local_port=local_port, discovery_ips_file=discovery_ips_file, mongod_port=mongod_port,
                                                 cache_path=cache_path, password=password)
        self.roles.append("clientworker")
        self.registry_functions = defaultdict(dict)
        self.jobs = dict()
        self.register_time_regular_func(partial(delete_old_requests,
                                                         mongod_port=mongod_port,
                                                         registry_functions=self.registry_functions))
        self.register_time_regular_func(partial(check_brokerworker_termination,
                                                         self.jobs, self.mongod_port, self.crypt_pass))
        self.register_time_regular_func(partial(check_brokerworker_deletion,
                                                         self))

    def start_local(self, newf, filter_, db, col, funcname):
        process = multiprocessing.Process(target=newf)
        process.daemon = True
        process.start()
        self.jobs[frozenset(filter_.items())] = (process, db, col, funcname)

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
            self.registry_functions[f.__name__]['original_func'] = f


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
                local_data.update({"started":None, "kill_clientworker": None, "delete_clientworker": None})

                deserializer = partial(deserialize_doc_from_net, up_dir=updir, key_interpreter=key_interpreter)

                current_list_with_identifier = find(self.mongod_port, db, col, filter_, key_interpreter)
                if len(current_list_with_identifier)==0:
                    # TODO only insert if identifier was not allready downloaded
                    #  otherwise in case of restart, the clientworker will redownload the data and it will crash in
                    #  p2p_pull_update_one when checking   if len(collection_res) != 1:
                    p2p_insert_one(self.mongod_port, db, col, local_data, [broker_ip + ":" + str(broker_port)],
                                   do_upload=False, password=self.crypt_pass)
                p2p_pull_update_one(self.mongod_port, db, col, filter_, param_keys, deserializer,
                                    hint_file_keys=hint_args_file_keys, password=self.crypt_pass)

                new_f = wraps(f)(
                    partial(function_executor,
                            f=f, filter=filter_,
                            mongod_port=self.mongod_port, db=db, col=col,
                            key_interpreter=key_interpreter,
                            # FIXME this if statement in case of debug mode was introduced just for an unfortunated combination of OS
                            #  and PyCharm version when variables in watch were hanging with no timeout just because of multiprocessing manaeger
                            logging_queue=self._logging_queue if not is_debug_mode() else None,
                            password=self.crypt_pass))

                self.start_local(new_f, filter_, db, col, f.__name__)

            self.register_time_regular_func(wrap)
            return None

        return inner_decorator
