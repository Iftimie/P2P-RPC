FROM python:3.8-buster

RUN apt-get update
RUN apt-get install python3-dev -y

COPY requirements.txt /app/p2prpc/requirements.txt

RUN pip install --requirement /app/p2prpc/requirements.txt
