SHELL:=/bin/bash

.PHONY: services delete checkservices test evaluatedbs logbroker worker broker client
THIS_FILE := $(lastword $(MAKEFILE_LIST))

broker:
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

checkservices:
	@sudo printf 'mongo-client  '; sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mongo-client || true
	@sudo printf 'mongo-client  ' > 'iplist.txt'; sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mongo-client >> 'iplist.txt' || true
	@sudo printf 'mongo-broker  '; sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mongo-broker || true
	@sudo printf 'mongo-broker  ' >> 'iplist.txt'; sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mongo-broker >> 'iplist.txt' || true
	@sudo printf 'mongo-worker  '; sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mongo-worker || true
	@sudo printf 'mongo-worker  ' >> 'iplist.txt'; sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mongo-worker >> 'iplist.txt' || true
	@sudo printf 'broker        '; sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' broker || true

client:
	p2prpc generate-client function_module/function.py discovery.txt
	sudo docker-compose -f client/client.docker-compose.yml build
	sudo docker-compose -f client/client.docker-compose.yml up -d
	rm -R client/p2prpc

worker:
	p2prpc generate-worker function_module/function.py discovery.txt
	sudo docker network create broker_mynet  || true
	sudo docker-compose -f worker/worker.docker-compose.yml build
	sudo docker-compose -f worker/worker.docker-compose.yml up -d

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


