SHELL:=/bin/bash
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

	p2prpc generate-worker function.py discovery.txt
	p2prpc generate-client function.py discovery.txt

	sudo docker-compose -f worker/worker.docker-compose.yml build
	sudo docker-compose -f worker/worker.docker-compose.yml up -d
	sudo docker-compose -f client/client.docker-compose.yml build
	sudo docker-compose -f client/client.docker-compose.yml up -d

delete:
	sudo docker-compose -f broker/broker.docker-compose.yml stop
	sudo docker-compose -f broker/broker.docker-compose.yml rm -f
	sudo docker-compose -f client/client.docker-compose.yml stop
	sudo docker-compose -f client/client.docker-compose.yml rm -f
	sudo docker-compose -f worker/worker.docker-compose.yml stop
	sudo docker-compose -f worker/worker.docker-compose.yml rm -f
	sudo rm discovery.txt client_errors.log client_info.log
	sudo rm -R broker clientdb client worker

checkservices:

	@sudo printf 'mongo-client  '; sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mongo-client
	@sudo printf 'broker        '; sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' broker

test:
	@export MONGO_HOST=$(shell sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mongo-client); \
	echo $$MONGO_HOST; \
	export PYTHONPATH=$$PYTHONPATH:./; \
	python client/clientapp.py

evaluatedbs:
	@export MONGO_HOST=$(shell sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mongo-client); \
	export MONGO_PORT=27017; \
	export PYTHONPATH=$$PYTHONPATH:./; \
	python evaluatedbs.py

logworker:
	sudo docker-compose -f worker/worker.docker-compose.yml logs -f worker

logbroker:
	sudo docker-compose -f broker/broker.docker-compose.yml logs -f broker


.PHONY: services startservices test
