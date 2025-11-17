#!/bin/bash

mkdir -p ./airflow/logs
sudo chmod -R 777 ./airflow/*

./Dockerfiles/pentaho-server/build_pentaho.sh
docker-compose up

# docker-compose down