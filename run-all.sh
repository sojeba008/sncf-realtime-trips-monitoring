#!/bin/bash

mkdir -p ./airflow/logs
sudo chmod -R 777 ./airflow/* # don't use 777 x)

docker-compose up -d

# docker-compose down