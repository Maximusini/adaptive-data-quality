FROM python:3.10-slim

WORKDIR /opt/app

RUN pip install --no-cache-dir kafka-python faker numpy

COPY generator.py

CMD['python', 'generator.py']