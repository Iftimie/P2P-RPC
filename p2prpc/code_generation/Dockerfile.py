dockerfile_string = \
"""
FROM python:3.6.9-buster
RUN apt-get update
RUN apt-get install python3-dev -y

COPY . /app

RUN pip install --requirement /app/{apptype}/p2prpc/requirements.txt
RUN bash /app/install_deps.sh || true
"""
