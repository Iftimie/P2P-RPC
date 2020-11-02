client_dockercompose_string = \
"""
---
version: '3.3'
services:

  mongo-client:
    container_name: mongo-client
    image: mvertes/alpine-mongo
    ports:
      - "27017"
    environment:
      - MONGO_PORT=5100
    networks:
      - broker_mynet

  client-discovery:
    container_name: client-discovery
    ports:
      - "5000:5000"
    build:
      context: {docker_context}
      dockerfile: {dockerfile_path}
    depends_on:
      - mongo-client
    volumes:
      - {network_discovery_file}:/app/client/network_discovery_client.txt
    environment:
      - MONGO_PORT=27017
      - MONGO_HOST=mongo-client
      - BOOKKEEPER_PORT=5000
      - APP_ROLES=client
      - DISCOVERY_FILE=/app/client/network_discovery_client.txt
      - PASSWORD="{super_secret_password}"
      - SERVICE_NAME=client
      - SERVICE_PORT=4999
    command:
      - bash
      - -c
      - |
        cd /app/client
        export PYTHONPATH=$$PYTHONPATH:./
        python ./p2prpc/bookkeeper_service.py service &
        python ./p2prpc/bookkeeper_service.py update
    networks:
      - broker_mynet

networks:
  broker_mynet:
    external: true
# sudo docker-compose -f client.docker-compose.yml up
"""

client_app_template = \
"""
import os
import sys
sys.path.append(os.getcwd())
with open(os.path.join(os.path.dirname(__file__), 'mongohost.txt'), 'r') as f:
    os.environ['MONGO_HOST'] = f.read()
from p2prpc.p2p_client import create_p2p_client_app
from {module} import {function}
import os.path as osp
import time
import logging
logger = logging.getLogger(__name__)

client_app = create_p2p_client_app("discovery.txt", password="{super_secret_password}", cache_path=osp.join(osp.abspath(osp.dirname(__file__)), 'clientdb'))


{function} = client_app.register_p2p_func()({function})

kwargs = dict()
res = {function}(**kwargs)
print(res.get())

client_app.background_server.shutdown()


"""