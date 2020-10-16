SHELL:=/bin/bash

.PHONY: services delete checkservices test evaluatedbs logbroker
THIS_FILE := $(lastword $(MAKEFILE_LIST))

services:
	# TODO group the commands into the p2prpc CLI
	cd ../../; python setup.py install; cd tests/scenario5
	p2prpc generate-broker function_module/function.py
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

	p2prpc generate-client function_module/function.py discovery.txt
	sudo docker-compose -f client/client.docker-compose.yml build
	sudo docker-compose -f client/client.docker-compose.yml up -d
	rm -R client/p2prpc

	p2prpc generate-worker function_module/function.py discovery.txt
	sudo docker-compose -f worker/worker.docker-compose.yml build
	sudo docker-compose -f worker/worker.docker-compose.yml up -d
	@$(MAKE) -f $(THIS_FILE) checkservices

delete:
	sudo docker-compose -f broker/broker.docker-compose.yml kill
	sudo docker-compose -f broker/broker.docker-compose.yml rm -f
	sudo docker-compose -f client/client.docker-compose.yml kill
	sudo docker-compose -f client/client.docker-compose.yml rm -f
	sudo docker-compose -f worker/worker.docker-compose.yml kill
	sudo docker-compose -f worker/worker.docker-compose.yml rm -f

	sudo rm -R worker
	sudo rm -R broker
	sudo rm -R client/client.docker-compose.yml || true
	sudo rm -R client/Dockerfile || true

checkservices:
	@sudo printf 'mongo-client  '; sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mongo-client
	@sudo printf 'mongo-client  ' > 'iplist.txt'; sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mongo-client >> 'iplist.txt'
	@sudo printf 'mongo-broker  '; sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mongo-broker
	@sudo printf 'mongo-broker  ' >> 'iplist.txt'; sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mongo-broker >> 'iplist.txt'
	@sudo printf 'mongo-worker  '; sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mongo-worker
	@sudo printf 'mongo-worker  ' >> 'iplist.txt'; sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mongo-worker >> 'iplist.txt'


test:
	@$(MAKE) -f $(THIS_FILE) cleandbs
	cd ../../; python setup.py install; cd tests/scenario4
	@export MONGO_HOST=$(shell sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mongo-client); \
	export MONGO_PORT=27017; \
	export PYTHONPATH=$$PYTHONPATH:./; \
	python client/clientapp.py

cleandbs:
	@export MONGO_HOST=$(shell sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mongo-client); \
	export MONGO_PORT=27017; \
	export PYTHONPATH=$$PYTHONPATH:./; \
	python test_utils.py clean mongo-client mongo-broker mongo-worker

logbroker:
	sudo docker-compose -f broker/broker.docker-compose.yml logs -f broker

logworker:
	sudo docker-compose -f worker/worker.docker-compose.yml logs -f worker


