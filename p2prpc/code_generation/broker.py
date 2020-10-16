dockercompose_string = \
"""
---
version: '3.3'
services:

  mongo-broker:
    container_name: mongo-broker
    hostname: mongo-broker
    image: mvertes/alpine-mongo
    ports:
      - "27017"
    environment:
      - MONGO_PORT=5101
    networks:
      - mynet

  broker-discovery:
    container_name: broker-discovery
    hostname: broker-discovery
    ports:
      - "5002:5002"
    build:
      context: {docker_context}
      dockerfile: {dockerfile_path}
    depends_on:
      - mongo-broker
    environment:
      - MONGO_PORT=27017
      - MONGO_HOST=mongo-broker
      - BOOKKEEPER_PORT=5002
      - APP_ROLES=broker
      - PASSWORD="{super_secret_password}"
      - SERVICE_NAME=broker
      - SERVICE_PORT=5001
    command:
      - bash
      - -c
      - |
        cd /app/broker
        export PYTHONPATH=$$PYTHONPATH:./
        python ./p2prpc/bookkeeper_service.py service &
        python ./p2prpc/bookkeeper_service.py update
    networks:
      - mynet

  broker:
    container_name: broker
    hostname: broker
    ports:
      - "5001:5001"
    build:
      context: {docker_context}
      dockerfile: {dockerfile_path}
    depends_on:
      - mongo-broker
      - broker-discovery

    environment:
      - MONGO_PORT=27017
      - MONGO_HOST=mongo-broker
    command:
      - bash
      - -c
      - |
        cd /app/
        export PYTHONPATH=$$PYTHONPATH:./
        python ./broker/brokerapp.py
    networks:
      - mynet

networks:
  mynet:
    driver: bridge

# sudo docker-compose -f client.docker-compose.yml up
"""

broker_script = \
"""
from p2prpc.p2p_brokerworker import P2PBrokerworkerApp
from {module} import {function}
import os.path as osp

password = "{super_secret_password}"
path = osp.join(osp.abspath(osp.dirname(__file__)), 'brokerworkerdb')
broker_worker_app = P2PBrokerworkerApp(None, password=password, cache_path=path)

broker_worker_app.register_p2p_func()({function})
broker_worker_app.start_background_threads()
broker_worker_app.run(host='0.0.0.0')
"""
