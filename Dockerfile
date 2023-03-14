FROM ubuntu:14.04
RUN rm /bin/sh && ln -s /bin/bash /bin/sh
FROM python:3.10

WORKDIR pyready_trader_go

COPY data data 
COPY docker_requirements.txt docker_requirements.txt

RUN pip install -r docker_requirements.txt

ENTRYPOINT [ "python3", "rtg.py", "test" ]