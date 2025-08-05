mkdir -p ./airflow/logs
mkdir postgres-db-volume
mkdir -p ./airflow/dags
sudo chmod -R 777 ./airflow/*
# mkdir local-delta
# sudo chown -R 50000:0 ./airflow/dags
#docker build -t custom-airflow Dockerfiles/Airflow/
#docker build -t delta-kafka-consumer Dockerfiles/Spark/
