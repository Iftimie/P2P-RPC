SHELL:=/bin/bash

.PHONY: services delete test cleandbs logbroker logworker
THIS_FILE := $(lastword $(MAKEFILE_LIST))

services:
	cd ../../; python setup.py install; cd tests/scenario7
	p2prpc generate-broker function_module/function.py
	p2prpc start broker

	p2prpc generate-worker function_module/function.py discovery.txt
	p2prpc start worker
	sudo docker-compose -f worker/worker.docker-compose.yml up -d --scale worker=3

	p2prpc generate-client function_module/function.py discovery.txt
	p2prpc start client

delete:
	p2prpc delete client;
	p2prpc delete broker;
	p2prpc delete worker;

test:
	@$(MAKE) -f $(THIS_FILE) cleandbs
	cd ../../; python setup.py install; cd tests/scenario6
	python client/clientapp.py

cleandbs:
	p2prpc delete-data broker
	p2prpc delete-data client
	p2prpc delete-data worker

logbroker:
	p2prpc log broker

logworker:
	p2prpc log worker


