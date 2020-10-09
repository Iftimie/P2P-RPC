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
      context: {p2prpc_package_path}
      dockerfile: code_generation/Dockerfile
    depends_on:
      - mongo-client
    volumes:
      - {p2prpc_package_path}:/app/client/p2prpc/
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

  client:
    container_name: client
    ports:
      - "4999:4999"
    build:
      context: {p2prpc_package_path}
      dockerfile: code_generation/Dockerfile
    depends_on:
      - mongo-client
      - client-discovery
    volumes:
      - {p2prpc_package_path}:/app/p2prpc/
      - {client_app_path}:/app/client/clientapp.py
      - {current_function_file_path}:/app/function.py
    environment:
      - MONGO_PORT=27017
      - MONGO_HOST=mongo-client
    command:
      - bash
      - -c
      - |
        cd /app/
        export PYTHONPATH=$$PYTHONPATH:./
        python ./client/clientapp.py
    networks:
      - broker_mynet

networks:
  broker_mynet:
    external: true
# sudo docker-compose -f client.docker-compose.yml up
"""