from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta, date
import pandas as pd
from pathlib import Path

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
    
    dag_folder = os.path.dirname(__file__)
    SQL_FILE_PATH = os.path.join(dag_folder, 'SQL', 'TASKS', 'create_yesterday_trains_number_dataset.sql')
    
    try:
        with open(SQL_FILE_PATH, 'r', encoding='utf-8') as file:
            sql_template = file.read()
    except FileNotFoundError:
        print(f"Erreur de lecture de {SQL_FILE_PATH}")
        raise

    sql_query = sql_template.format(yesterday=date_for_sql)
    print("Requête SQL exécutée :")
    #print(sql_query)

    conn = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)

        df = pd.read_sql(sql_query, conn)

        OUTPUT_DIR = Path(dag_folder) / "DATASETS/NB_TRAINS"
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        
        parquet_filename = f"nb_trains_day_{suffix_yesterday}.parquet"
        OUTPUT_FILE_PATH = OUTPUT_DIR / parquet_filename
        
        df.to_parquet(OUTPUT_FILE_PATH, index=False)
        
        print(f"Creation fichier ok : {OUTPUT_FILE_PATH}")
        print(f"Nombre d enregistrements : {len(df)}")
        
    except Exception as e:
        print("Erreur lors de la génération du dataset PostgreSQL/Parquet :", e)
        if conn:
            conn.rollback()
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