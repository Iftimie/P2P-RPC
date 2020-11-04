dockerfile_string = \
"""
FROM python:3.6.9-buster
RUN apt-get update
RUN apt-get install python3-dev -y

RUN pip install flask \
pymongo \
multipledispatch \
dill==0.3.1.1 \
varint \
mmh3 \
passlib \
requests \
deprecated \
celery \
requests_toolbelt \
uwsgi

COPY install_deps.sh /app/install_deps.sh

RUN bash /app/install_deps.sh || true

COPY . /app
"""
