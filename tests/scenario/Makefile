SHELL:=/bin/bash

.PHONY: services delete checkservices test evaluatedbs logbroker
THIS_FILE := $(lastword $(MAKEFILE_LIST))

services:
	p2prpc generate-broker function.py
	sudo docker-compose -f broker/broker.docker-compose.yml build
	sudo docker-compose -f broker/broker.docker-compose.yml up -d

	output=$$(sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' broker-discovery); \
	while [[ "$$output" == "" ]]; do \
		output=$$(sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' broker-discovery); \
		echo $$output; \
		sleep 1; \
	done; \
	echo $$output:5002 > 'discovery.txt'; \
	cat 'discovery.txt';

	p2prpc generate-client function.py discovery.txt

	sudo docker-compose -f client/client.docker-compose.yml build
	sudo docker-compose -f client/client.docker-compose.yml up -d

delete:
	sudo docker-compose -f broker/broker.docker-compose.yml stop
	sudo docker-compose -f broker/broker.docker-compose.yml rm -f
	sudo docker-compose -f client/client.docker-compose.yml stop
	sudo docker-compose -f client/client.docker-compose.yml rm -f
	sudo rm -R broker
	sudo rm -R client/clientdb
	sudo rm -R client/client.docker-compose.yml || true
	sudo rm client_errors.log || true
	sudo rm  client_info.log || true

checkservices:

	@sudo printf 'mongo-client  '; sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mongo-client
	@sudo printf 'mongo-broker  '; sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mongo-broker

test:
	@$(MAKE) -f $(THIS_FILE) evaluatedbs
	@export MONGO_HOST=$(shell sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mongo-client); \
	export MONGO_PORT=27017; \
	export PYTHONPATH=$$PYTHONPATH:./; \
	python client/clientapp.py

evaluatedbs:
	@export MONGO_HOST=$(shell sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mongo-client); \
	export MONGO_PORT=27017; \
	export PYTHONPATH=$$PYTHONPATH:./; \
	python evaluatedbs.py

logbroker:
	sudo docker-compose -f broker/broker.docker-compose.yml logs -f broker


