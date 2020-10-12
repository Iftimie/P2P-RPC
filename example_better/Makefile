client:
	p2prpc generate-client function.py clientapp.py discovery.txt
	sudo docker-compose -f client/client.docker-compose.yml up -d
	sudo docker-compose -f client/client.docker-compose.yml logs -f client

broker:
	p2prpc generate-broker function.py
	sudo docker-compose -f broker/broker.docker-compose.yml up -d
	sudo docker-compose -f broker/broker.docker-compose.yml logs -f broker

worker:
	p2prpc generate-worker function.py discovery.txt
	sudo docker-compose -f worker/worker.docker-compose.yml up -d
	sudo docker-compose -f worker/worker.docker-compose.yml logs -f worker

cleanbroker:
	sudo docker-compose -f broker/broker.docker-compose.yml stop
	sudo docker-compose -f broker/broker.docker-compose.yml rm -f

cleanclient:
	sudo docker-compose -f client/client.docker-compose.yml stop
	sudo docker-compose -f client/client.docker-compose.yml rm -f

cleanworker:
	sudo docker-compose -f worker/worker.docker-compose.yml stop
	sudo docker-compose -f worker/worker.docker-compose.yml rm -f

cleanall: cleanclient cleanworker cleanbroker

logclient:
	sudo docker-compose -f client/client.docker-compose.yml logs -f client

logbroker:
	sudo docker-compose -f broker/broker.docker-compose.yml logs -f broker

logworker:
	sudo docker-compose -f worker/worker.docker-compose.yml logs -f worker

.PHONY: client broker worker cleanclient cleanbroker cleanworker logclient logbroker logworker
