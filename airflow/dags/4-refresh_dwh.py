from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv
import os
from datetime import datetime, date, timedelta
import requests
from google.transit import gtfs_realtime_pb2
import json
import xml.etree.ElementTree as ET
from pathlib import Path
import pandas as pd
from psycopg2.extras import execute_values

load_dotenv()
DB_PARAMS = {
    'host': os.getenv('DB_HOST'),
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'port': (os.getenv('DB_PORT'))
}

def refresh_dwh():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        dag_folder = os.path.dirname(__file__)

        DWH_SQL_FOLDER=os.path.join(dag_folder, 'SQL', 'DWH')
        sql_files = [f for f in os.listdir(DWH_SQL_FOLDER) if os.path.isfile(os.path.join(DWH_SQL_FOLDER, f))]
        for sql_file in sql_files:
            if sql_file[0] != '0':
                sql_path = os.path.join(dag_folder, 'SQL', 'DWH', sql_file)
                with open(sql_path, 'r', encoding='utf-8') as file:
                    sql = file.read()
                    cursor.execute(sql)
        
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print("Erreur PostgreSQL :", e)
        if conn:
            conn.rollback()

default_args = {
    'owner': 'sncf-data',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'refresh_dwh',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    tags=[]
) as dag:
    
    collect_task = PythonOperator(
        task_id='4-refresh_dwh',
        python_callable=refresh_dwh
    )
    

    collect_task