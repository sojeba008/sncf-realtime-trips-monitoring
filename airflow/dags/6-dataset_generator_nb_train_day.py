from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta, date
import pandas as pd
from pathlib import Path
from minio import Minio
import io

load_dotenv()
DB_PARAMS = {
    'host': os.getenv('DWH_HOST'),
    'dbname': os.getenv('DWH_DB'),
    'user': os.getenv('DWH_USER'),
    'password': os.getenv('DWH_PASSWORD'),
    'port': (os.getenv('DWH_PORT'))
}

def generate_trains_number_dataset():
    yesterday = date.today() - timedelta(days=1)
    suffix_yesterday = yesterday.strftime("%Y%m%d") 
    date_for_sql = yesterday.strftime("%Y-%m-%d") 
    
    MINIO_CLIENT = Minio(
        os.getenv("MINIO_HOST"),
        access_key=os.getenv("MINIO_ROOT_USER"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
        secure=False
    )
    BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")

    dag_folder = os.path.dirname(__file__)
    SQL_FILE_PATH = os.path.join(dag_folder, 'SQL', 'TASKS', 'create_yesterday_trains_number_dataset.sql')
    
    with open(SQL_FILE_PATH, 'r', encoding='utf-8') as file:
        sql_template = file.read()

    sql_query = sql_template.format(yesterday=date_for_sql)

    conn = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        df = pd.read_sql(sql_query, conn)

        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_size = parquet_buffer.tell()
        parquet_buffer.seek(0)
        object_name = f"NB_TRAINS/nb_trains_day_{suffix_yesterday}.parquet"
        print("nom bucket "+str(BUCKET_NAME))
        MINIO_CLIENT.put_object(
            BUCKET_NAME,
            object_name,
            data=parquet_buffer,
            length=parquet_size,
            content_type='application/octet-stream'
        )
        
        print(f"Upload réussi : {BUCKET_NAME}/{object_name}")
        print(f"Nombre d enregistrements : {len(df)}")
        
    except Exception as e:
        print("Erreur :", e)
        if conn: conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

default_args = {
    'owner': 'sncf-data',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'dataset_generator_nb_train_day',
    default_args=default_args,
    schedule_interval='0 1 * * *', 
    catchup=False,
    tags=['dataset', 'jounralier'],
    max_active_runs=1
) as dag:
    
    generate_dataset_task = PythonOperator(
        task_id='generate_yesterday_trains_number_dataset',
        python_callable=generate_trains_number_dataset
    )