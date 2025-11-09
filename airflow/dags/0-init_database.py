from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta

load_dotenv()
DB_PARAMS = {
    'host': os.getenv('DB_HOST'),
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'port': (os.getenv('DB_PORT'))
}
TARGET_DB = 'sncf_trips'
# SCHEMAS = ['dsa', 'ods']

def create_database_and_schemas():
    print(DB_PARAMS)
    DB_PARAMS_1 = DB_PARAMS.copy()
    DB_PARAMS_1['dbname'] = 'postgres'
    print(DB_PARAMS_1)
    conn = psycopg2.connect(**DB_PARAMS_1)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{TARGET_DB}'")
    exists = cur.fetchone()
    if not exists:
        cur.execute(f'CREATE DATABASE {TARGET_DB}')
    else:
        print(f"Database '{TARGET_DB}' already exists.")

    cur.close()
    conn.close()

    db_params_target = DB_PARAMS.copy()
    db_params_target['dbname'] = TARGET_DB
    
    for schema_init_script in ['init_dsa_tables.sql', 'init_ods_tables.sql', 'init_dwh_tables.sql', 'init_ods_statics_refs.sql']:
        conn = psycopg2.connect(**db_params_target)
        cur = conn.cursor()

        # for schema in SCHEMAS:
        #      cur.execute(f'CREATE SCHEMA IF NOT EXISTS {schema}')
        dag_folder = os.path.dirname(__file__)
        print(dag_folder)
        
        sql_path = os.path.join(dag_folder, 'SQL', 'INIT', schema_init_script)
        with open(sql_path, 'r', encoding='utf-8') as file:
            sql = file.read()
            cur.execute(sql)

        conn.commit()
        cur.close()
        conn.close()

# create_database_and_schemas()
default_args = {
    'owner': 'sncf-data',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='create_database_and_schemas',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    is_paused_upon_creation=False,
    tags=[]
) as dag:
    
    collect_task = PythonOperator(
        task_id='0-create_database_and_schemas',
        python_callable=create_database_and_schemas
    )

    collect_task