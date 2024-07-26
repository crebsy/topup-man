FROM python:3.11-slim as builder
RUN apt-get update && apt-get install -y gcc git libpq-dev
ADD requirements.txt  ./
RUN pip3 install -r requirements.txt

WORKDIR /app
COPY . .
RUN ape plugins install -y .
