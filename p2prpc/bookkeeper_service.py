from p2prpc.bookkeeper import route_node_states, initial_discovery
from functools import wraps, partial
from p2prpc.bookkeeper import update_function, get_ip_actual_service
from flask import Flask
import sys
import os
import time
BOOKKEEPER_PORT = int(os.environ['BOOKKEEPER_PORT'])
DISCOVERY_FILE = os.environ['DISCOVERY_FILE'] if 'DISCOVERY_FILE' in os.environ else None
APP_ROLES = os.environ['APP_ROLES']
PASSWORD = os.environ['PASSWORD']
SERVICE_NAME = os.environ['SERVICE_NAME']
SERVICE_PORT = int(os.environ['SERVICE_PORT'])

if __name__ == '__main__':
    if sys.argv[1] == 'service':
        app = Flask(__name__)
        decorated_route_node_states = (wraps(route_node_states)(partial(route_node_states)))
        app.route("/node_states", methods=['POST', 'GET'])(decorated_route_node_states)

        decorated_get_ip_actual_service = (wraps(get_ip_actual_service)(partial(get_ip_actual_service,
                                                                            service_port=SERVICE_PORT,
                                                                            service_name=SERVICE_NAME)))
        app.route("/actual_service_ip", methods=['POST', 'GET'])(decorated_get_ip_actual_service)

        app.run(host='0.0.0.0', port=BOOKKEEPER_PORT)
    elif sys.argv[1]=='update':
        APP_ROLES = APP_ROLES.split('-')
        initial_discovery(BOOKKEEPER_PORT, APP_ROLES, DISCOVERY_FILE, PASSWORD)
        # infinite loop will not start until the app is online
        while True:
            update_function()
            time.sleep(5)
    else:
        raise ValueError(f"Argument {sys.argv[1]} not known")
