from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta, date
import pandas as pd
from pathlib import Path
from utils.minio_ops import upload_parquet_to_minio

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
    
    MINIO_PARAMS = {
        'host': os.getenv("MINIO_HOST"),
        'user': os.getenv("MINIO_ROOT_USER"),
        'password': os.getenv("MINIO_ROOT_PASSWORD")
    }
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
        print(df.head(5))
        suffix = (date.today() - timedelta(days=1)).strftime("%Y%m%d")
        object_name = f"NB_TRAINS/nb_trains_day_{suffix}.parquet"
        upload_parquet_to_minio(
            df=df, 
            object_name=object_name, 
            minio_config=MINIO_PARAMS, 
            bucket_name=BUCKET_NAME
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