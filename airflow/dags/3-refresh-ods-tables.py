from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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

load_dotenv()
DB_PARAMS = {
    'host': os.getenv('DB_HOST'),
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'port': (os.getenv('DB_PORT'))
}

BASE_DIR = Path(__file__).resolve().parent           
SQL_DIR  = BASE_DIR / "SQL" / "ODS"   

def refresh_ods_table():
    print("ODS")

    today     = date.today()          
    tomorrow  = today + timedelta(days=1)
    dayAfterTomorrow  = tomorrow + timedelta(days=1)
    suffix    = today.strftime("%Y%m%d")
    suffix_2    = tomorrow.strftime("%Y%m%d")

    sql_partition = f"""
    CREATE TABLE IF NOT EXISTS ods.trips_{suffix} PARTITION OF ods.trips
        FOR VALUES FROM ('{today}') TO ('{tomorrow}');

    CREATE TABLE IF NOT EXISTS ods.stops_{suffix} PARTITION OF ods.stops
        FOR VALUES FROM ('{today}') TO ('{tomorrow}');

    CREATE TABLE IF NOT EXISTS ods.trips_{suffix_2} PARTITION OF ods.trips
        FOR VALUES FROM ('{tomorrow}') TO ('{dayAfterTomorrow}');

    CREATE TABLE IF NOT EXISTS ods.stops_{suffix_2} PARTITION OF ods.stops
        FOR VALUES FROM ('{tomorrow}') TO ('{dayAfterTomorrow}');
    """
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        dag_folder = os.path.dirname(__file__)

        cursor.execute(sql_partition)

        print(dag_folder)
        # sql_path = os.path.join(dag_folder, 'SQL', 'ODS', 'refresh_trips.sql').replace("***", "airflow")
        sql_path = os.path.join(dag_folder, 'SQL', 'ODS', 'refresh_trips.sql')
        with open(sql_path, 'r', encoding='utf-8') as file:
            sql = file.read()
            cursor.execute(sql)

        # sql_path = os.path.join(dag_folder, 'SQL', 'ODS', 'refresh_stops.sql').replace("***", "airflow")
        sql_path = os.path.join(dag_folder, 'SQL', 'ODS', 'refresh_stops.sql')
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


# fetch_and_push()
# create_database_and_schemas()
default_args = {
    'owner': 'sncf-data',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'refresh_ods_table',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    tags=[]
) as dag:
    
    collect_task = PythonOperator(
        task_id='3-refresh_ods_table',
        python_callable=refresh_ods_table
    )
    
    trigger_next_run_2 = TriggerDagRunOperator(
        task_id='4-refresh_dwh',
        trigger_dag_id='refresh_dwh',
        wait_for_completion=False
    )

    collect_task >> trigger_next_run_2