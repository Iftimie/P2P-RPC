from flask import make_response, request, jsonify
import threading
from werkzeug.serving import make_server
import requests
import logging
import traceback
import socket
from pymongo import MongoClient
import json
from requests.exceptions import ReadTimeout

p2pbookdb = "p2pbookdb"
collection = "nodes"


def node_states(mongod_port):
    logger = logging.getLogger(__name__)
    try:
        client = MongoClient(port=mongod_port)
        db = client[p2pbookdb]
        current_items = list(db[collection].find({}))
        for item in current_items:
            del item['_id']
    except json.decoder.JSONDecodeError:
        logger.debug("{} json decoding error. probably because of not being threadsafe".format(request.remote_addr))
        return make_response("not ok", 500)
    if request.method == 'POST':
        current_states = {d['address']: d for d in current_items}
        current_states.update({d['address']: d for d in request.json})
        db[collection].remove({})
        new_data = list(current_states.values())
        db[collection].insert_many(new_data)
        return make_response("done", 200)
    else:
        return jsonify(current_items)


def get_state_in_lan(local_port, app_roles):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    ip_ = s.getsockname()[0]
    s.close()
    state = {'address': ip_+":"+str(local_port),
                              'workload': find_workload(),
                              'node_type': ",".join(app_roles)}
    return state


def get_state_in_wan(local_port, app_roles):
    state = []
    try:
        externalipres = requests.get('http://checkip.dyndns.org/')
        part = externalipres.content.decode('utf-8').split(": ")[1]
        ip_ = part.split("<")[0]
        try:
            echo_response = requests.get('http://{}:{}/echo'.format(ip_, local_port), timeout=3)
            if echo_response.status_code == 200:
                state.append({'address': ip_+":"+str(local_port),
                                          'workload': find_workload(),
                                          'node_type': ",".join(app_roles)})
        except:
            pass
    except:
        pass
    return state


def get_states_from_file(discovery_ips_file):
    states = []
    # other states
    if discovery_ips_file is not None:
        with open(discovery_ips_file, 'r') as f:
            for line in f.readlines():
                if len(line) < 4: continue
                states.append({'address': line.strip()})
    return states


def write_states_to_file(discovery_ips_file, discovered_states):
    # TODO I should move this reading from here to the app creation and use app.test_client.get
    if discovery_ips_file is not None:
        with open(discovery_ips_file, 'w') as f:
            for state in discovered_states:
                f.write("{address}\n".format(address=state['address']))


def set_from_list(discovered_states):
    merge_states = dict()
    for d in discovered_states:
        if d['address'] in merge_states:
            if len(d) > len(merge_states[d['address']]):  # or d is newer than merge_states[d]
                merge_states[d['address']] = d
        else:
            merge_states[d['address']] = d
    discovered_states = list(merge_states.values())
    return discovered_states


def query_pull_from_nodes(discovered_states):
    states = discovered_states[:]
    for state in discovered_states[:]:
        try:
            discovered_ = requests.get('http://{}/node_states'.format(state['address']), timeout=3).json()
            states.extend(discovered_)
        except:
            #some adresses may be dead
            #TODO maybe remove them?
            pass
    states = set_from_list(states)
    return states


def push_to_nodes(discovered_states):
    logger = logging.getLogger(__name__)
    # publish the results to the current node and also to the rest of the nodes
    for state in discovered_states:
        try:
            response = requests.post('http://{}/node_states'.format(state['address']), json=discovered_states, timeout=3)
        except:
            logger.info("{} no longer exists".format(state['address']))
            # some adresses may be dead
            # TODO maybe remove them?
            pass


def update_function(local_port, app_roles, discovery_ips_file):
    """
    Function for bookkeeper to make network discovery
    discovery_ips_file: can be None
    """
    #TODO invetigate why the call blocks and needs to have a timeout or at least set timeouts for all requests
    #TODO invetigate why the call blocks and needs to have a timeout or at least set timeouts for all requests
    #TODO invetigate why the call blocks and needs to have a timeout or at least set timeouts for all requests
    #TODO invetigate why the call blocks and needs to have a timeout or at least set timeouts for all requests
    #TODO invetigate why the call blocks and needs to have a timeout or at least set timeouts for all requests
    #TODO invetigate why the call blocks and needs to have a timeout or at least set timeouts for all requests
    #TODO invetigate why the call blocks and needs to have a timeout or at least set timeouts for all requests
    #TODO invetigate why the call blocks and needs to have a timeout or at least set timeouts for all requests
    #TODO invetigate why the call blocks and needs to have a timeout or at least set timeouts for all requests
    #TODO invetigate why the call blocks and needs to have a timeout or at least set timeouts for all requests
    #TODO invetigate why the call blocks and needs to have a timeout or at least set timeouts for all requests
    #TODO invetigate why the call blocks and needs to have a timeout or at least set timeouts for all requests
    #TODO invetigate why the call blocks and needs to have a timeout or at least set timeouts for all requests
    #TODO invetigate why the call blocks and needs to have a timeout or at least set timeouts for all requests
    #TODO invetigate why the call blocks and needs to have a timeout or at least set timeouts for all requests
    #TODO invetigate why the call blocks and needs to have a timeout or at least set timeouts for all requests
    #TODO invetigate why the call blocks and needs to have a timeout or at least set timeouts for all requests
    logger = logging.getLogger(__name__)

    discovered_states = []

    # get the current list of nodes
    try:
        res = requests.get('http://localhost:{}/node_states'.format(local_port), timeout=3).json()  # will get the data defined above
        discovered_states.extend(res)
    except ReadTimeout:
        logger.warning("Unable to contact localhost for getting current node states")
        logger.info(traceback.format_exc())
    except Exception as e:
        raise e

    # get the current node state in LAN
    discovered_states.append(get_state_in_lan(local_port, app_roles))
    discovered_states.extend(get_state_in_wan(local_port, app_roles))
    discovered_states.extend(get_states_from_file(discovery_ips_file))

    discovered_states = set_from_list(discovered_states)

    # query the remote nodes
    discovered_states = query_pull_from_nodes(discovered_states)

    # also store them
    write_states_to_file(discovery_ips_file, discovered_states)

    discovered_states = list(filter(lambda d: len(d) > 1, discovered_states))

    for state in discovered_states:
        if '_id' in state:
            del state['_id']

    try:
        # publish them locally
        requests.post('http://localhost:{}/node_states'.format(local_port), json=discovered_states, timeout=3)
    except ReadTimeout:
        logger.warning("Unable to contact localhost for publishing new node states")
    except Exception as e:
        raise e

    # publish them remotely
    push_to_nodes(discovered_states)


def find_workload():
    return 0


# class used only for test purposes
class ServerThread(threading.Thread):

    def __init__(self, app, host='0.0.0.0', port=5000, central_host=None, central_port=None):
        #TODO this ServerThread should actually call something from update_function, since the calls are similar

        threading.Thread.__init__(self)
        self.host = host
        self.port = port
        self.srv = make_server(host, port, app)
        self.ctx = app.app_context()
        self.client = app.test_client()
        assert self.host != 'localhost' and self.host != '127.0.0.1'
        # just to avoid confusion. localhost will be 127.0.0.1
        # I am not sure altough what 0.0.0.0 means
        # https://superuser.com/questions/949428/whats-the-difference-between-127-0-0-1-and-0-0-0-0
        # Typically you use bind-address 0.0.0.0 to allow connections from outside networks and sources. Many servers like MySQL typically bind to 127.0.0.1 allowing only loopback connections, requiring the admin to change it to 0.0.0.0 to enable outside connectivity.

        # update_function(local_port=port, app_roles=app.roles, discovery_ips_file=None)

        data = [{'ip': self.host, 'port': self.port, 'workload': find_workload(), 'hardware': "Nvidia GTX 960M Intel i7",
                 'nickname': "rmstn",
                 'node_type': ",".join(app.roles + ['bookkeeper']), 'email': 'iftimie.alexandru.florentin@gmail.com'}]
        # register self state to local service
        res = self.client.post("/node_states", json=data)
        if central_host is not None and central_port is not None:
            # register self state to central service
            res = requests.post('http://{}:{}/node_states'.format(central_host, central_port), json=data, timeout=1)
            # register remote states to local service
            res = self.client.post("/node_states", json=requests.get('http://{}:{}/node_states'.format(central_host, central_port)).json())

        self.ctx.push()

    def run(self):
        self.srv.serve_forever()

    def shutdown(self):
        self.srv.shutdown()