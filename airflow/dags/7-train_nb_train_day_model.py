from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta, date
import pandas as pd
from pathlib import Path
from utils.minio_ops import nb_trains_model_from_s3

def train_nb_train_day_model():
    load_dotenv()
    MINIO_PARAMS = {
        'host': "http://"+os.getenv("MINIO_HOST"),
        'user': os.getenv("MINIO_ROOT_USER"),
        'password': os.getenv("MINIO_ROOT_PASSWORD"),
        'use_listings_cache': True if os.getenv("S3_USE_LISTINGS_CACHE").lower() == 'true' else False,
        'use_ssl': True if os.getenv("S3_USE_SSL").lower() == 'true' else False
    }
    BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")
    nb_trains_model_from_s3(MINIO_PARAMS, BUCKET_NAME)

default_args = {
    'owner': 'sncf-data',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'train_nb_train_day_model',
    default_args=default_args,
    schedule_interval='0 2 * * *', 
    catchup=False,
    tags=['ML', 'Daily'],
    max_active_runs=1
) as dag:
    
    nb_train_model_generation_task = PythonOperator(
        task_id='train_nb_train_day_model',
        python_callable=train_nb_train_day_model
    )