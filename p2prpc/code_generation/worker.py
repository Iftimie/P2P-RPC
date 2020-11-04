worker_dockerfile_string = \
"""
---
version: '3.3'
services:

  mongo-worker:
    container_name: mongo-worker
    image: mvertes/alpine-mongo
    ports:
      - "27017"
    environment:
      - MONGO_PORT=5102
    networks:
      - broker_mynet

  worker-discovery:
    container_name: worker-discovery
    ports:
      - "5004:5004"
    build:
      context: {docker_context}
      dockerfile: {dockerfile_path}
    depends_on:
      - mongo-worker
    volumes:
      - {network_discovery_file}:/app/worker/network_discovery_worker.txt
    environment:
      - MONGO_PORT=27017
      - MONGO_HOST=mongo-worker
      - BOOKKEEPER_PORT=5004
      - APP_ROLES=worker
      - DISCOVERY_FILE=/app/worker/network_discovery_worker.txt
      - PASSWORD="{super_secret_password}"
      - SERVICE_NAME=worker
      - SERVICE_PORT=5003
    command:
      - bash
      - -c
      - |
        cd /app/worker
        export PYTHONPATH=$$PYTHONPATH:./
        python ./p2prpc/bookkeeper_service.py service &
        python ./p2prpc/bookkeeper_service.py update
    networks:
      - broker_mynet

  worker:
    # TODO. if we need to scale the worker, it cannot have a container_name, or ports (because of collision)
    # which means it shouldn't be a Flask service. meh... technical debt
    #ports:
    #  - "5003:5003"
    build:
      context: {docker_context}
      dockerfile: {dockerfile_path}
    depends_on:
      - mongo-worker
      - worker-discovery
      
    environment:
      - MONGO_PORT=27017
      - MONGO_HOST=mongo-worker
    command:
      - bash
      - -c
      - |
        cd /app/
        bash worker/install_deps.sh || true
        export PYTHONPATH=$$PYTHONPATH:./
        python ./worker/workerapp.py
    networks:
      - broker_mynet

networks:
  broker_mynet:
    external: true
# sudo docker-compose -f worker.docker-compose.yml up

"""

workerapp_string = \
"""
from p2prpc.p2p_clientworker import P2PClientworkerApp
from {module} import {function}
import os.path as osp

password = "{super_secret_password}"
path = osp.join(osp.abspath(osp.dirname(__file__)), 'workerdb')

clientworker_app = P2PClientworkerApp("network_discovery_worker.txt", password=password, cache_path=path)

clientworker_app.register_p2p_func(can_do_work_func=lambda: True)({function})
clientworker_app.run(host='0.0.0.0')

"""