both:
	p2prpc generate-broker function.py
	p2prpc generate-client function.py clientapp.py discovery.txt
	sudo docker-compose -f broker/broker.docker-compose.yml up -d
	sudo docker-compose -f client/client.docker-compose.yml up

clean:
	sudo docker-compose -f broker/broker.docker-compose.yml stop
	sudo docker-compose -f broker/broker.docker-compose.yml rm -f
	sudo docker-compose -f client/client.docker-compose.yml stop
	sudo docker-compose -f client/client.docker-compose.yml rm -f


